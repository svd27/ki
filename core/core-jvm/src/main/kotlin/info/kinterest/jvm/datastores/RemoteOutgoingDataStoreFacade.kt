package info.kinterest.jvm.datastores

import info.kinterest.KIEntity
import info.kinterest.datastores.*
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.query.Query
import info.kinterest.query.QueryResult
import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.launch

interface RemoteOutgoingDataStoreFacade : DataStoreFacade {
    val ds: DataStoreFacade
    val chIn: ReceiveChannel<QueryMsg>
    val chOut: SendChannel<QueryResultMsg>
    val pool: CoroutineDispatcher

    val nextId: Long

    override fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<QueryResult<E, K>>>> = ds.query(query)

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