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
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Paging
import info.kinterest.query.*
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import mu.KLogger
import mu.KLogging

interface TransactionManager {
    val log: KLogger
    val qm: QueryManager
    val txStore: DataStoreFacade
    val metas: MetaProvider
    val txDispatcher: CoroutineDispatcher
    val txProcessors: CoroutineDispatcher
    var deferreds: Map<Long, CompletableDeferred<*>>

    suspend fun setState(tx: Transaction<*>, state: TxState) {
        txStore.setValues(tx._meta, tx.id, tx._version, mapOf(tx._meta.props["state"]!! to state)).await().getOrElse { throw it }
    }

    suspend fun <R> create(tx: Transaction<R>): Transaction<R> = run {
        val txc = txStore.create(tx._meta, listOf(tx)).getOrElse { throw it }.await().getOrElse { throw it }.first()
        setState(txc, TxState.NEW)
        txc
    }

    operator fun <R> plus(tx: Transaction<R>): Try<Deferred<Try<R>>> = Try {
        runBlocking(txDispatcher) {
            val txc = create(tx)
            val deferred = CompletableDeferred<Try<R>>()
            deferreds += txc.id to deferred
            added(txc)
            deferred
        }
    }

    suspend fun added(tx: Transaction<*>)

    suspend fun <R> addAndExecute(tx: Transaction<R>, cb: Try<R>.() -> Unit) = runBlocking(txDispatcher) {
        val txc = create(tx)
        launch(txProcessors) { execute(txc).await().apply(cb) }
    }

    suspend fun <R> fail(tx: Transaction<R>, ex: Exception) {
        setState(tx, TxState.FAILING)
        tx.rollback(this@TransactionManager)
        children(tx, TxState.values().toSet()).map { fail(it, ex) }
        setState(tx, TxState.FAILED)
        if (tx.parent == null) {
            @Suppress("UNCHECKED_CAST")
            (deferreds[tx.id] as? CompletableDeferred<Try<R>>)?.complete(Try.raise(ex.cast()))
            launch(txProcessors) { cleanup(tx) }
        }
    }

    suspend fun <R> commit(tx: Transaction<R>, r: R): R = run {
        setState(tx, TxState.DONE)
        if (tx.parent == null) {
            @Suppress("UNCHECKED_CAST")
            (deferreds[tx.id] as? CompletableDeferred<Try<R>>)?.complete(Try { r })
            launch(txProcessors) { cleanup(tx) }
        }
        r
    }

    suspend fun cleanup(tx: Transaction<*>) {
        children(tx, TxState.values().toSet()).forEach { cleanup(it) }
        txStore.delete(tx._meta, listOf(tx)).getOrElse { throw it }.await().getOrElse { throw it }
    }

    suspend fun <R> execute(tx: Transaction<R>): Deferred<Try<R>> = async(txProcessors) {
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
                                either.left.forEach { this@TransactionManager + it }
                            }
                            is Either.Right -> res = commit(tx, either.right)
                        }
                        if (res != null) break
                    } catch (e: Exception) {
                        fail(tx, e)
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
    override var deferreds: Map<Long, CompletableDeferred<*>> = emptyMap()

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

            ("state" notin TxState.INACTIVE).and(
                    "metaName" eq metaName,
                    "create" eq create as Comparable<Any>,
                    "createdAt" lte createdAt,
                    ids(id).inverse())

        }
        @Suppress("UNCHECKED_CAST")
        val cp = CountProjection<CreateTransaction, Long>(_meta.idProperty as KIProperty<Any>)
        val q = Query(f, listOf(cp))
        val qr = tm.txStore.query(q).getOrElse { throw it }.await().getOrElse { throw it }
        val retrieve = qr.retrieve(cp.path, tm.qm).getOrElse { throw it }.await().getOrElse { throw it } as CountProjectionResult<CreateTransaction, Long>
        if (retrieve.count > 0) throw QueryError(q, tm.qm, "tx already exists")
        val meta = tm.metas.meta(metaName)!!
        val res = tm.qm.retrieve(meta, listOf(create)).getOrElse { throw it }.await().getOrElse { throw it }
        if (res.count() > 0) throw DataStoreError.EntityError.EntityExists(meta, create, DataStore(ds))
        Either.Right(create)
    }

    override fun rollback(tm: TransactionManager) {

    }
}