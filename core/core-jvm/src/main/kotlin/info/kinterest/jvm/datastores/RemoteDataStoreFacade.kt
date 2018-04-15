package info.kinterest.jvm.datastores

import info.kinterest.DataStoreError
import info.kinterest.KIEntity
import info.kinterest.datastores.*
import info.kinterest.functional.Try
import info.kinterest.query.Query
import info.kinterest.query.QueryResult
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KLogging

interface RemoteDataStoreFacade : DataStoreFacade {
    val ch: SendChannel<QueryMsg>
    val chResp: ReceiveChannel<QueryResultMsg>
    val pool: CoroutineDispatcher
    var pendingQueries: Map<Long, Pair<Query<*, *>, CompletableDeferred<Try<QueryResult<*, *>>>>>

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
