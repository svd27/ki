package info.kinterest.jvm.tx

import com.github.salomonbrys.kodein.KodeinInjected
import com.github.salomonbrys.kodein.KodeinInjector
import com.github.salomonbrys.kodein.instance
import info.kinterest.*
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Either
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.annotations.TypeArgs
import info.kinterest.jvm.filter.filter
import info.kinterest.jvm.tx.jvm.TransactionJvm
import info.kinterest.paging.Paging
import info.kinterest.query.*
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import mu.KLogger
import mu.KLogging

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
        txStore.setValues(tx._meta, tx.id, tx._version, mapOf(tx._meta.props["state"]!! to state)).await().getOrElse { throw it }
    }

    suspend fun <R> create(tx: Transaction<R>): Transaction<R> = run {
        val txc = txStore.create(tx._meta, listOf(tx)).getOrElse { throw it }.await().getOrElse { throw it }.first()
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
                    setState(tx, TxState.PROCESSING)
                    val ct = children(tx, TxState.ACTIVE)
                    setState(tx, TxState.WAITING)
                    ct.map { execute(it).await() }
                    val ctd = children(tx, TxState.INACTIVE)
                    require(ct.count() == ctd.count())
                    try {
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
                    } catch (e: Exception) {
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
        log.debug { f }
        val projection = EntityProjection<Transaction<*>, Long>(Ordering.natural(), Paging.ALL)
        val qr = txStore.query(Query(f, listOf(projection), setOf(txStore))).getOrElse { throw it }.await().getOrElse { throw it }
        val txs = qr.retrieve(projection.path, qm).getOrElse { throw it }.await().getOrElse { throw it } as EntityProjectionResult<Transaction<*>, Long>
        txs.page.entities
    }
}

class TransactionManagerJvm : TransactionManager, KodeinInjected {
    override val injector: KodeinInjector = KodeinInjector()

    override val qm: QueryManager by injector.instance()
    override val txStore: DataStoreFacade by injector.instance("tx-store")
    override val metas: MetaProvider by injector.instance()
    override val txDispatcher: CoroutineDispatcher = newSingleThreadContext("tx.add")
    override val txProcessors: CoroutineDispatcher = newFixedThreadPoolContext(Runtime.getRuntime().availableProcessors(), "tx")
    override var deferreds: Map<Long, CompletableDeferred<Try<*>>> = emptyMap()
    override var callbacks: Map<Long, TxCallback<*>> = emptyMap()
    override val lock: Mutex = Mutex()
    private val created: OffsetDateTime = OffsetDateTime.now()

    private val ch: Channel<Transaction<*>> = Channel()

    override suspend fun added(tx: Transaction<*>) {
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
    val parent: Long?
    val createdAt: OffsetDateTime
    val validTill: OffsetDateTime

    val state: TxState

    fun rollback(tm: TransactionManager)

    suspend fun process(childrenDone: Iterable<Transaction<*>>, tm: TransactionManager): Either<Iterable<Transaction<*>>, R>
}

@Entity
interface CreateTransaction : Transaction<Any> {
    override val id: Long
    val metaName: String
    @TypeArgs(args = [Any::class])
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
        val q = Query(f, listOf(cp, elements))
        val qr = tm.txStore.query(q).getOrElse { throw it }.await().getOrElse { throw it }
        val retrieve = qr.retrieve(cp.path, tm.qm).getOrElse { throw it }.await().getOrElse { throw it } as CountProjectionResult<CreateTransaction, Long>
        if (retrieve.count > 0) {
            val entities = qr.retrieve(elements.path, tm.qm)
            val er = entities.getOrElse { throw it }.await().getOrElse { throw it } as EntityProjectionResult
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

    override fun rollback(tm: TransactionManager) {

    }

    companion object : KLogging()
}