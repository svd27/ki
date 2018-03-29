package info.kinterest.jvm.events

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.selects.select
import mu.KLogging

class Dispatcher<T> {
    val incoming: Channel<T> = Channel()
    private var outgoing: List<Channel<T>> = listOf()
    private val _subscribing: Channel<Channel<T>> = Channel()
    val subscribing: SendChannel<Channel<T>> = _subscribing
    private val _unsubscribe: Channel<Channel<T>> = Channel()
    val unsubscribe: SendChannel<Channel<T>> = _unsubscribe

    init {
        launch(CommonPool) {
            select<Unit> {
                _subscribing.onReceive {
                    outgoing += it
                }
                _unsubscribe.onReceive {
                    outgoing -= it
                }
            }
        }
        launch(CommonPool) {
            for (t in incoming)
                for (s in outgoing) {
                    logger.debug { "received $t" }
                    s.send(t)
                }
        }
    }

    fun close() {
        incoming.close()
        subscribing.close()
        for (o in outgoing) o.close()
    }

    companion object : KLogging()
}
