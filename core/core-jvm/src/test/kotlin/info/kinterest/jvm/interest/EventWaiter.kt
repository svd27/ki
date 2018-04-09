package info.kinterest.jvm.interest

import info.kinterest.KIEvent
import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.withTimeout
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

    fun waitFor(check: (E) -> Boolean) {
        launch(pool) {
            if (evts.any { check(it) }) return@launch
            withTimeout(500) {
                for (e in out) {
                    logger.debug { "received $e" }
                    if (check(e)) break
                    if (evts.any { check(it) }) break
                }
                logger.debug { "waitFor done." }
            }
        }
    }

    companion object : KLogging()
}