package info.kinterest.jvm.util

import info.kinterest.KIEvent
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import mu.KLogging


class EventWaiter<E : KIEvent>(channel: Channel<E>) {
    var evts: List<E> = listOf()
    val pool: CoroutineDispatcher = newFixedThreadPoolContext(2, "event-waiter")
    val out: Channel<E> = Channel()

    init {
        launch(pool) {
            for (e in channel) {
                logger.debug { "received $e" }
                evts += e
                launch(pool) {
                    out.send(e)
                }
            }
        }
    }

    fun waitFor(check: (E) -> Boolean): E = runBlocking(pool) {
        val evt = evts.firstOrNull(check)
        if (evt != null) evt else withTimeout(500) {
            var res: E? = null
            for (e in out) {
                if (check(e)) {
                    res = e; break
                }
                res = evts.firstOrNull(check)
                if (res != null) break
            }
            res!!
        }
    }


    companion object : KLogging()
}