package info.kinterest.jvm.events

import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.selects.select
import mu.KLogging

class Dispatcher<T>(pool: CoroutineDispatcher? = null) {
    val incoming: Channel<T> = Channel()
    private var outgoing: List<Channel<T>> = listOf()
    private val _subscribing: Channel<Channel<T>> = Channel()
    val subscribing: SendChannel<Channel<T>> = _subscribing
    private val _unsubscribe: Channel<Channel<T>> = Channel()
    val unsubscribe: SendChannel<Channel<T>> = _unsubscribe

    private val dispatcher = pool ?: newFixedThreadPoolContext(8, "Dispatcher")

    init {
        launch(dispatcher) {
            select<Unit> {
                _subscribing.onReceive {
                    outgoing += it
                }
                _unsubscribe.onReceive {
                    outgoing -= it
                }
            }
        }
        launch(dispatcher) {
            for (t in incoming) {
                logger.debug { "received $t" }
                for (s in outgoing) {
                    if (s.isClosedForReceive) {
                        outgoing -= s
                        continue
                    }

                    s.send(t)
                }
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
