package info.kinterest.jvm.datastores

import info.kinterest.KIEntity
import info.kinterest.QueryError
import info.kinterest.datastores.*
import info.kinterest.functional.Try
import info.kinterest.paging.Page
import info.kinterest.query.Query
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KLogging

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
                        logger.trace { "received success: $msg" }
                        second.complete(Try { msg.page })
                    }
                }
            }
        }
    }

    companion object : KLogging()
}
