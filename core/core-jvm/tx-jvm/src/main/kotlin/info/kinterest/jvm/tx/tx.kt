package info.kinterest.jvm.tx

import info.kinterest.*
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.datastores.IEntityTrace
import info.kinterest.functional.Either
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.addIncomingRelation
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.annotations.GeneratedByStore
import info.kinterest.jvm.annotations.GuarantueedUnique
import info.kinterest.jvm.datastores.DataStoreConfig
import info.kinterest.jvm.datastores.IDataStoreFactoryProvider
import info.kinterest.jvm.filter.filter
import info.kinterest.jvm.removeIncomingRelation
import info.kinterest.jvm.tx.jvm.*
import info.kinterest.meta.KIRelationProperty
import info.kinterest.meta.Relation
import info.kinterest.paging.Paging
import info.kinterest.query.*
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import mu.KLogger
import mu.KLogging
import org.kodein.di.Kodein
import org.kodein.di.KodeinAware
import org.kodein.di.erased.instance

typealias TxCallback<R> = suspend (Try<R>) -> Unit

interface TransactionManager {
    val log: KLogger
    val qm: QueryManager
    val txStore: DataStoreFacade
    val metas: MetaProvider
    val txDispatcher: CoroutineDispatcher
    val txProcessors: CoroutineDispatcher
    var deferreds: Map<Long, CompletableDeferred<Try<*>>>
    var callbacks: Map<Long, TxCallback<*>>
    val lock: Mutex

    suspend fun addDeferred(id: Long, d: CompletableDeferred<Try<*>>, cb: TxCallback<*>?) = lock.withLock {
        deferreds += id to d
        if (cb != null)
            callbacks += id to cb
    }

    suspend fun removeDeferreds(ids: Iterable<Long>) = lock.withLock {
        deferreds -= ids
        callbacks -= ids
    }

    suspend fun setState(tx: Transaction<*>, state: TxState) {
        txStore.setValues(tx._meta, tx.id, tx._version, mapOf(tx._meta.props["state"]!! to state)).await().fold({
            if (it is DataStoreError.OptimisticLockException) runBlocking { setState(tx, state) }
        }, {})
    }

    suspend fun <R> create(tx: Transaction<R>): Transaction<R> = run {
        val txc = txStore.create(tx._meta, tx).getOrElse { throw it }.await().getOrElse { throw it }
        setState(txc, TxState.NEW)
        txc
    }

    fun <R> add(tx: Transaction<R>, cb: TxCallback<R>? = null): Try<Pair<Long, Deferred<Try<R>>>> = Try {
        runBlocking(txDispatcher) {
            val txc = create(tx)
            val deferred = CompletableDeferred<Try<R>>()
            addDeferred(txc.id, deferred.cast(), cb?.cast())

            added(txc)
            txc.id to deferred
        }
    }

    suspend fun added(tx: Transaction<*>)

    suspend fun <R> fail(tx: Transaction<R>, f: Try.Failure<R>) {
        logger.debug { "fail $tx" }
        if (tx.state in setOf(TxState.FAILING, TxState.FAILED)) return
        setState(tx, TxState.FAILING)
        tx.rollback(this@TransactionManager)
        children(tx, TxState.values().toSet()).map { fail<Any>(it.cast(), f.cast()) }
        callback(tx, f)
        setState(tx, TxState.FAILED)
        complete(tx, f)
    }

    suspend fun <R> complete(tx: Transaction<R>, r: Try<R>) {
        if (tx.parent == null) {
            @Suppress("UNCHECKED_CAST")
            (deferreds[tx.id] as? CompletableDeferred<Try<R>>)?.complete(r)
        }
    }

    suspend fun <R> callback(tx: Transaction<R>, r: Try<R>) {
        if (tx.parent == null) {
            callbacks[tx.id]?.let { cbx ->
                val cb = cbx as TxCallback<R>
                try {
                    cb(r)
                } catch (ex: Exception) {
                    //TODO: check how bad this actually is
                    log.warn(ex) { "exception in callback ignored" }
                }
            }
        }
    }

    suspend fun <R> commit(tx: Transaction<R>, r: Try.Success<R>): R = run {
        logger.debug { "commit $tx" }
        callback(tx, r)
        setState(tx, TxState.DONE)
        complete(tx, r)
        r.res
    }

    suspend fun cleaner() {
        val f = filter<Transaction<*>, Long>(TransactionJvm.meta) {
            "parent".isNull<Long>() and (("state" `in` TxState.INACTIVE) and ("validTill" lt OffsetDateTime.now()))
        }
        val ep = EntityProjection<Transaction<*>, Long>(Ordering.natural(), Paging(0, 1000))
        val q = Query(f, listOf(ep))
        val epr = qm.query(q).getOrElse { throw it }.await().getOrElse { throw it }.retrieve(ep.path, qm).getOrElse { throw it }.await().getOrElse { throw it }
        if (epr is EntityProjectionResult) {
            if (epr.page.size > 0) {
                val meta = epr.page.entities.first()._meta
                epr.page.entities.map { it.id }.apply {
                    removeDeferreds(this)

                }
                txStore.delete(meta, epr.page.entities)
            }
        }
    }

    suspend fun cleanup(tx: Transaction<*>) {
        children(tx, TxState.values().toSet()).forEach { cleanup(it) }
        txStore.delete(tx._meta, listOf(tx)).getOrElse { throw it }.await().getOrElse { throw it }
    }

    suspend fun <R> execute(tx: Transaction<R>, cb: TxCallback<R>? = null): Deferred<Try<R>> = async(txProcessors) {
        log.debug { "execute $tx" }
        Try {
            runBlocking(coroutineContext) {
                log.debug { "entering while" }
                var res: R? = null
                while (isActive) {
                    try {
                        setState(tx, TxState.PROCESSING)
                        val ct = children(tx, TxState.ACTIVE)
                        logger.debug { "children ${ct.map { "$it ${it.state}" }.joinToString(", ")}" }
                        setState(tx, TxState.WAITING)
                        ct.map { execute(it) }.map { it.await() }
                        val ctd = children(tx, TxState.INACTIVE)
                        logger.debug { "children done ${ctd.map { "$it ${it.state}" }.joinToString(", ")}" }
                        require(ct.count() == ctd.count()) {
                            "expected ${ct.count()} but was ${ctd.count()}"
                        }

                        setState(tx, TxState.PROCESSING)
                        val either: Either<Iterable<Transaction<*>>, R> = tx.process(ctd, this@TransactionManager)
                        when (either) {
                            is Either.Left -> {
                                ctd.forEach { cleanup(it) }
                                either.left.forEach { this@TransactionManager.add(it) }
                            }
                            is Either.Right -> res = commit(tx, Try.succeed(either.right))
                        }
                        if (res != null) break
                    } catch (e: java.lang.Exception) {
                        logger.debug(e) { "tx $tx failed" }
                        fail(tx.cast(), Try.raise<Any>(e.cast()))
                        throw e
                    }
                }
                res!!
            }
        }
    }

    suspend fun children(tx: Transaction<*>, state: Set<TxState>): Iterable<Transaction<*>> = run {
        val f = filter<Transaction<*>, Long>(TransactionJvm.meta) {
            "state" `in` state and ("parent" eq tx.id)
        }
        logger.debug { f }
        val projection = EntityProjection<Transaction<*>, Long>(Ordering.natural(), Paging.ALL)
        val query = txStore.query(Query(f, listOf(projection), setOf(txStore)))
        val await = query.getOrElse { throw it }.await()
        val qr = await.getOrElse { throw it }
        val txs = qr.retrieve(projection.path, qm).getOrElse { throw it }.await().getOrElse { throw it } as EntityProjectionResult<Transaction<*>, Long>
        txs.page.entities
    }

    companion object : KLogging()
}

class TransactionManagerJvm(override val kodein: Kodein) : TransactionManager, KodeinAware {
    override val qm: QueryManager by instance()
    override val txStore: DataStoreFacade by lazy {
        val dsf: IDataStoreFactoryProvider by instance()
        val cfg: DataStoreConfig by instance("tx-store")
        dsf.create(cfg).getOrElse { throw it }
    }
    override val metas: MetaProvider by instance()
    override val txDispatcher: CoroutineDispatcher = newSingleThreadContext("tx.add")
    override val txProcessors: CoroutineDispatcher = newFixedThreadPoolContext(Runtime.getRuntime().availableProcessors(), "tx")
    override var deferreds: Map<Long, CompletableDeferred<Try<*>>> = emptyMap()
    override var callbacks: Map<Long, TxCallback<*>> = emptyMap()
    override val lock: Mutex = Mutex()
    private val created: OffsetDateTime = OffsetDateTime.now()

    private val ch: Channel<Transaction<*>> = Channel()

    override suspend fun added(tx: Transaction<*>) {
        if (tx.parent == null)
            ch.send(tx)
    }

    init {
        launch(txProcessors) {
            for (tx in ch) {
                execute(tx)
            }
        }

    }

    override val log: KLogger
        get() = logger

    companion object : KLogging()
}

enum class TxState {
    NEW, WAITING, PROCESSING, FAILING, FAILED, DONE;

    companion object {
        val ACTIVE: Set<TxState> = setOf(NEW, WAITING, PROCESSING, FAILING)
        val INACTIVE: Set<TxState> = setOf(FAILED, DONE)
    }
}


@Entity
interface Transaction<R> : KIVersionedEntity<Long> {
    override val id: Long
        @GuarantueedUnique(true)
        @GeneratedByStore
        get() = TODO()
    val parent: Long?
    val createdAt: OffsetDateTime
    val validTill: OffsetDateTime

    val state: TxState

    suspend fun rollback(tm: TransactionManager)

    suspend fun process(childrenDone: Iterable<Transaction<*>>, tm: TransactionManager): Either<Iterable<Transaction<*>>, R>
}

@Entity
interface CreateTransaction : Transaction<Any> {
    override val id: Long
    val metaName: String
    val create: Any
    val ds: String

    override suspend fun process(childrenDone: Iterable<Transaction<*>>, tm: TransactionManager): Either<Iterable<Transaction<*>>, Any> = run {
        val f = filter<CreateTransaction, Long>(tm.metas.meta(_meta.me)!!) {
            @Suppress("UNCHECKED_CAST")
            ("state" `in` TxState.ACTIVE).and(
                    "metaName" eq metaName,
                    "create" eq create as Comparable<Any>,
                    "createdAt" lte createdAt,
                    ids(id).inverse())

        }
        val cp = CountProjection<CreateTransaction, Long>(_meta.idProperty)
        val elements = EntityProjection<CreateTransaction, Long>(Ordering.natural(), Paging(0, 10))
        logger.debug { "query $f" }
        val q = Query(f, listOf(cp, elements), setOf(tm.txStore))
        val qr = tm.txStore.query(q).getOrElse { throw it }.await().getOrElse { throw it }
        val retrieve = qr.retrieve(cp.path, tm.qm).getOrElse { throw it }.await().getOrElse { throw it } as CountProjectionResult<CreateTransaction, Long>
        if (retrieve.count > 0) {
            val entities = qr.retrieve(elements.path, tm.qm)
            val er = entities.getOrElse { throw it }.await().getOrElse { throw it } as EntityProjectionResult
            logger.debug { "count: $retrieve entities: ${er.page.entities}" }
            er.page.entities.forEach {
                logger.debug {
                    "$it: ${it.state} ${it.metaName} ${it.create}"
                }
            }
            throw QueryError(q, tm.qm, "tx already exists")
        }
        val meta = tm.metas.meta(metaName)!!
        val res = tm.qm.retrieve(meta, listOf(create)).getOrElse { throw it }.await().getOrElse { throw it }
        if (res.count() > 0) throw DataStoreError.EntityError.EntityExists(meta, create, DataStore(ds))
        Either.Right(create)
    }

    override suspend fun rollback(tm: TransactionManager) {

    }

    companion object : KLogging()
}

@Entity
interface AddRelationTransaction : Transaction<Boolean> {
    override val id: Long
    val relation: String
    val soutce: IEntityTrace
    val target: IEntityTrace

    override suspend fun process(childrenDone: Iterable<Transaction<*>>, tm: TransactionManager): Either<Iterable<Transaction<*>>, Boolean> {
        if (childrenDone.count() == 0) {
            return Either.Left(listOf(
                    BookRelationTransactionJvm.Transient(tm.txStore, System.nanoTime(), id, java.time.OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, relation, soutce, target, false),
                    AddOutgoingRelationTransactionJvm.Transient(tm.txStore, System.nanoTime(), id, OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, relation, soutce, target, false)
            ))
        }
        if (childrenDone.count() == 2 && childrenDone.all { it.state == TxState.DONE }) {
            if (childrenDone.all { it is BookRelationTransaction && it.booked || it is AddOutgoingRelationTransaction && it.booked })
                return Either.right(true)
            else if (childrenDone.all { it is BookRelationTransaction && !it.booked || it is AddOutgoingRelationTransaction && !it.booked })
                return Either.right(false)
            else
                throw IllegalStateException("neither all booked or unbooked ${childrenDone.map {
                    "$it ${it.state} ${
                    if (it is BookRelationTransaction) "${it.booked}" else if (it is AddOutgoingRelationTransaction) "${it.booked}" else ""
                    }"
                }.joinToString(",", "[", "]")}")
        } else throw IllegalStateException("not all done ${childrenDone.map { "$it ${it.state}" }}")
    }

    override suspend fun rollback(tm: TransactionManager) {

    }
}

@Entity
interface RemoveRelationTransaction : Transaction<Boolean> {
    override val id: Long
    val relation: String
    val soutce: IEntityTrace
    val target: IEntityTrace

    override suspend fun process(childrenDone: Iterable<Transaction<*>>, tm: TransactionManager): Either<Iterable<Transaction<*>>, Boolean> {
        if (childrenDone.count() == 0) {
            return Either.Left(listOf(
                    UnBookRelationTransactionJvm.Transient(tm.txStore, System.nanoTime(), id, java.time.OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, relation, soutce, target, false),
                    RemoveOutgoingRelationTransactionJvm.Transient(tm.txStore, System.nanoTime(), id, OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, relation, soutce, target, false)
            ))
        }
        if (childrenDone.count() == 2 && childrenDone.all { it.state == TxState.DONE }) {
            if (childrenDone.all { it is UnBookRelationTransaction && it.unbooked || it is RemoveOutgoingRelationTransaction && it.unbooked })
                return Either.right(true)
            else if (childrenDone.all { it is UnBookRelationTransaction && !it.unbooked || it is RemoveOutgoingRelationTransaction && !it.unbooked })
                return Either.right(false)
            else
                throw IllegalStateException("neither all booked or unbooked ${childrenDone.map {
                    "$it ${it.state} ${
                    if (it is UnBookRelationTransaction) "${it.unbooked}" else if (it is RemoveOutgoingRelationTransaction) "${it.unbooked}" else ""
                    }"
                }.joinToString(",", "[", "]")}")
        } else throw IllegalStateException("not all done ${childrenDone.map { "$it ${it.state}" }}")
    }

    override suspend fun rollback(tm: TransactionManager) {

    }
}


@Entity
interface BookRelationTransaction : Transaction<Boolean> {
    override val id: Long
    val relation: String
    val soutce: IEntityTrace
    val target: IEntityTrace
    var booked: Boolean

    override suspend fun rollback(tm: TransactionManager) {
        if (booked) {
            val rp = tm.metas.meta(soutce.type)!!.props[relation]!! as KIRelationProperty
            val sel = tm.qm.retrieve(tm.metas.meta(soutce.type)!!, listOf(soutce.id), setOf(DataStore(soutce.ds))).getOrElse { throw it }
                    .await().getOrElse { throw it }
            val retrieve = tm.qm.retrieve(tm.metas.meta(target.type)!!, listOf(target.id),
                    setOf(DataStore(target.ds))).getOrElse { throw it }.await().getOrElse { throw it }
            val es = sel.first()
            val et = retrieve.first() as KIJvmEntity<KIEntity<Any>, Any>
            et.removeIncomingRelation(Relation(rp, es, et)).getOrElse { throw it }
        }
    }

    override suspend fun process(childrenDone: Iterable<Transaction<*>>, tm: TransactionManager): Either<Iterable<Transaction<*>>, Boolean> {
        val rp = tm.metas.meta(soutce.type)!!.props[relation]!! as KIRelationProperty
        val sel = tm.qm.retrieve(tm.metas.meta(soutce.type)!!, listOf(soutce.id), setOf(DataStore(soutce.ds))).getOrElse { throw it }
                .await().getOrElse { throw it }
        val retrieve = tm.qm.retrieve(tm.metas.meta(target.type)!!, listOf(target.id),
                setOf(DataStore(target.ds))).getOrElse { throw it }.await().getOrElse { throw it }
        val es = sel.first()
        val et = retrieve.first() as KIJvmEntity<KIEntity<Any>, Any>
        booked = et.addIncomingRelation(Relation(rp, es, et)).getOrElse { throw it }
        logger.debug { "result of addIncoming $booked for $soutce -> $target" }
        return Either.right(booked)
    }

    companion object : KLogging()
}

@Entity
interface UnBookRelationTransaction : Transaction<Boolean> {
    override val id: Long
    val relation: String
    val source: IEntityTrace
    val target: IEntityTrace
    var unbooked: Boolean

    override suspend fun rollback(tm: TransactionManager) {
        logger.debug { "rollback $this $source -> $target" }
        if (unbooked) {
            val rp = tm.metas.meta(source.type)!!.props[relation]!! as KIRelationProperty
            val sel = tm.qm.retrieve(tm.metas.meta(source.type)!!, listOf(source.id), setOf(DataStore(source.ds))).getOrElse { throw it }
                    .await().getOrElse { throw it }
            val retrieve = tm.qm.retrieve(tm.metas.meta(target.type)!!, listOf(target.id),
                    setOf(DataStore(target.ds))).getOrElse { throw it }.await().getOrElse { throw it }
            val es = sel.first()
            val et = retrieve.first() as KIJvmEntity<KIEntity<Any>, Any>
            et.addIncomingRelation(Relation(rp, es, et)).getOrElse { throw it }
        }
    }

    override suspend fun process(childrenDone: Iterable<Transaction<*>>, tm: TransactionManager): Either<Iterable<Transaction<*>>, Boolean> {
        val rp = tm.metas.meta(source.type)!!.props[relation]!! as KIRelationProperty
        val sel = tm.qm.retrieve(tm.metas.meta(source.type)!!, listOf(source.id), setOf(DataStore(source.ds))).getOrElse { throw it }
                .await().getOrElse { throw it }
        val retrieve = tm.qm.retrieve(tm.metas.meta(target.type)!!, listOf(target.id),
                setOf(DataStore(target.ds))).getOrElse { throw it }.await().getOrElse { throw it }
        val es = sel.first()
        val et = retrieve.first() as KIJvmEntity<KIEntity<Any>, Any>
        unbooked = et.removeIncomingRelation(Relation(rp, es, et)).getOrElse { throw it }
        logger.debug { "result of removeIncoming $unbooked for $source -> $target" }
        return Either.right(unbooked)
    }

    companion object : KLogging()
}


@Entity
interface AddOutgoingRelationTransaction : Transaction<Boolean> {
    override val id: Long
    val relation: String
    val source: IEntityTrace
    val target: IEntityTrace
    var booked: Boolean

    override suspend fun process(childrenDone: Iterable<Transaction<*>>, tm: TransactionManager): Either<Iterable<Transaction<*>>, Boolean> {
        val rp = tm.metas.meta(source.type)!!.props[relation]!! as KIRelationProperty
        val sel = tm.qm.retrieve(tm.metas.meta(source.type)!!, listOf(source.id), setOf(DataStore(source.ds))).getOrElse { throw it }
                .await().getOrElse { throw it }
        val retrieve = tm.qm.retrieve(tm.metas.meta(target.type)!!, listOf(target.id),
                setOf(DataStore(target.ds))).getOrElse { throw it }.await().getOrElse { throw it }
        val es = sel.first() as KIJvmEntity<KIEntity<Any>, Any>
        val et = retrieve.first()
        booked = es._store.setRelationSync(Relation(rp, es, et)).getOrElse { throw it }
        logger.debug { "result of setRelation $booked for $source -> $target" }
        return Either.right(booked)
    }

    override suspend fun rollback(tm: TransactionManager) {
        val rp = tm.metas.meta(source.type)!!.props[relation]!! as KIRelationProperty
        val sel = tm.qm.retrieve(tm.metas.meta(source.type)!!, listOf(source.id), setOf(DataStore(source.ds))).getOrElse { throw it }
                .await().getOrElse { throw it }
        val retrieve = tm.qm.retrieve(tm.metas.meta(target.type)!!, listOf(target.id),
                setOf(DataStore(target.ds))).getOrElse { throw it }.await().getOrElse { throw it }
        val es = sel.first() as KIJvmEntity<KIEntity<Any>, Any>
        val et = retrieve.first()
        es._store.unsetRelation(Relation(rp, es, et)).getOrElse { throw it }.await().getOrElse { throw it }
    }

    companion object : KLogging()
}

@Entity
interface RemoveOutgoingRelationTransaction : Transaction<Boolean> {
    override val id: Long
    val relation: String
    val soutce: IEntityTrace
    val target: IEntityTrace
    var unbooked: Boolean

    override suspend fun process(childrenDone: Iterable<Transaction<*>>, tm: TransactionManager): Either<Iterable<Transaction<*>>, Boolean> {
        val rp = tm.metas.meta(soutce.type)!!.props[relation]!! as KIRelationProperty
        val sel = tm.qm.retrieve(tm.metas.meta(soutce.type)!!, listOf(soutce.id), setOf(DataStore(soutce.ds))).getOrElse { throw it }
                .await().getOrElse { throw it }
        val retrieve = tm.qm.retrieve(tm.metas.meta(target.type)!!, listOf(target.id),
                setOf(DataStore(target.ds))).getOrElse { throw it }.await().getOrElse { throw it }
        val es = sel.first() as KIJvmEntity<KIEntity<Any>, Any>
        val et = retrieve.first()
        unbooked = es._store.unsetRelationSync(Relation(rp, es, et)).getOrElse { throw it }
        logger.debug { "result of unsetRelation $unbooked for $soutce -> $target" }
        return Either.right(unbooked)
    }

    override suspend fun rollback(tm: TransactionManager) {
        logger.debug { "rollback $this $soutce -> $target" }
        if (unbooked) {
            val rp = tm.metas.meta(soutce.type)!!.props[relation]!! as KIRelationProperty
            val sel = tm.qm.retrieve(tm.metas.meta(soutce.type)!!, listOf(soutce.id), setOf(DataStore(soutce.ds))).getOrElse { throw it }
                    .await().getOrElse { throw it }
            val retrieve = tm.qm.retrieve(tm.metas.meta(target.type)!!, listOf(target.id),
                    setOf(DataStore(target.ds))).getOrElse { throw it }.await().getOrElse { throw it }
            val es = sel.first() as KIJvmEntity<KIEntity<Any>, Any>
            val et = retrieve.first()
            es._store.setRelation(Relation(rp, es, et)).getOrElse { throw it }.await().getOrElse { throw it }
        }
    }

    companion object : KLogging()
}