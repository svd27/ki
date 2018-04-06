package info.kinterest.jvm.datastores

import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.QueryError
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page
import info.kinterest.query.Query
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KLogging


interface DataStoreFacade : DataStore {
    override val name: String
    fun <K : Any> version(type: KIEntityMeta, id: K): Any
    fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<Page<E, K>>>>
    fun <E : KIEntity<K>, K : Any> retrieve(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>>
    fun <E : KIEntity<K>, K : Any> create(type: KIEntityMeta, entities: Iterable<E>): Try<Deferred<Try<Iterable<E>>>>
    fun <E : KIEntity<K>, K : Any> delete(type: KIEntityMeta, entities: Iterable<E>): Try<Deferred<Try<Iterable<K>>>>
    fun getValues(type: KIEntityMeta, id: Any): Deferred<Try<Map<String, Any?>?>>
    fun getValues(type: KIEntityMeta, id: Any, vararg props: KIProperty<*>): Deferred<Try<Map<String, Any?>?>>
    fun getValues(type: KIEntityMeta, id: Any, props: Iterable<KIProperty<*>>): Deferred<Try<Map<String, Any?>?>>
    fun setValues(type: KIEntityMeta, id: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>>
    fun setValues(type: KIEntityMeta, id: Any, version: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>>
}

sealed class QueryMessage
data class QueryMsg(val q: Query<*, *>, val id: CompletableDeferred<Pair<Boolean, Long>>) : QueryMessage()
sealed class QueryResult : QueryMessage()
data class QueryTrySuccess(val id: Long, val page: Page<*, *>) : QueryResult()
data class QueryTryFailure(val id: Long) : QueryResult()


interface RemoteDataStoreFacade : DataStoreFacade {
    val ch: SendChannel<QueryMsg>
    val chResp: ReceiveChannel<QueryResult>
    val pool: CoroutineDispatcher
    var pendingQueries: Map<Long, Pair<Query<*, *>, CompletableDeferred<Try<Page<*, *>>>>>

    override fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<Page<E, K>>>> = Try {
        runBlocking {
            val resp = CompletableDeferred<Pair<Boolean, Long>>(parent = coroutineContext[Job])
            ch.send(QueryMsg(query, resp))
            val (result, id) = resp.await()
            if (!result) throw QueryError(query, "query failed", null)
            val res = CompletableDeferred<Try<Page<*, *>>>(coroutineContext[Job])
            pendingQueries += (id to (query to res))
            @Suppress("UNCHECKED_CAST")
            res as CompletableDeferred<Try<Page<E, K>>>
        }
    }

    fun receiverInit() {
        launch(pool) {
            for (msg in chResp) {
                when (msg) {
                    is QueryTryFailure -> pendingQueries[msg.id]?.let {
                        it.second.complete(Try { throw QueryError(it.first, "failed") })
                    }
                    is QueryTrySuccess -> pendingQueries[msg.id]?.apply {
                        logger.debug { "received success: $msg" }
                        second.complete(Try { msg.page })
                    }
                }
            }
        }
    }

    companion object : KLogging()
}

interface RemoteOutgoingDataStoreFacade : DataStoreFacade {
    val ds: DataStoreFacade
    val chIn: ReceiveChannel<QueryMsg>
    val chOut: SendChannel<QueryResult>
    val pool: CoroutineDispatcher

    val nextId: Long

    override fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<Page<E, K>>>> = ds.query(query)

    fun initReceiver() {
        launch(pool) {
            for (msg in chIn) {
                @Suppress("UNCHECKED_CAST")
                val tq = query(msg.q as Query<KIEntity<Any>, Any>)
                if (tq.isSuccess) {
                    val id = nextId
                    msg.id.complete(true to id)
                    val deferred = tq.getOrElse { null }
                    launch(coroutineContext) {
                        val res = deferred?.await()!!
                        chOut.send(res.map { QueryTrySuccess(id, it) }.getOrElse { QueryTryFailure(id) })
                    }
                } else {
                    msg.id.complete(false to -1)
                }
            }
        }
    }


}