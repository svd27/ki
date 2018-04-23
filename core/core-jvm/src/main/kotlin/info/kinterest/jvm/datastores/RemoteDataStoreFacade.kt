package info.kinterest.jvm.datastores

import info.kinterest.DataStoreError
import info.kinterest.KIEntity
import info.kinterest.datastores.*
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.query.Query
import info.kinterest.query.QueryResult
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KLogging

abstract class RemoteDataStoreFacade(name: String) : DataStoreFacade(name) {
    abstract val ch: SendChannel<QueryMsg>
    abstract val chResp: ReceiveChannel<QueryResultMsg>
    abstract val pool: CoroutineDispatcher
    var pendingQueries: Map<Long, Pair<Query<*, *>, CompletableDeferred<Try<QueryResult<*, *>>>>> = mapOf()

    override fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<QueryResult<E, K>>>> = Try {
        runBlocking {
            val resp = CompletableDeferred<Pair<Boolean, Long>>(parent = coroutineContext[Job])
            ch.send(QueryMsg(query, resp))
            val (result, id) = resp.await()
            if (!result) throw DataStoreError.QueryFailed(query, this@RemoteDataStoreFacade, "query failed")
            val res = CompletableDeferred<Try<QueryResult<*, *>>>(coroutineContext[Job])
            pendingQueries += (id to (query to res))
            @Suppress("UNCHECKED_CAST")
            res as CompletableDeferred<Try<QueryResult<E, K>>>
        }
    }

    override fun <E : KIEntity<K>, K : Any> querySync(query: Query<E, K>): Try<QueryResult<E, K>> = query(query).map { runBlocking { it.await().getOrElse { throw it } } }

    fun receiverInit() {
        launch(pool) {
            for (msg in chResp) {
                when (msg) {
                    is QueryTryFailure -> pendingQueries[msg.id]?.let {
                        it.second.complete(Try { throw DataStoreError.QueryFailed(it.first, this@RemoteDataStoreFacade, "query failed") })
                    }
                    is QueryTrySuccess -> pendingQueries[msg.id]?.apply {
                        logger.trace { "received success: $msg" }
                        second.complete(Try { msg.result })
                    }
                }
            }
        }
    }

    companion object : KLogging()
}
