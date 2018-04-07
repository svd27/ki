package info.kinterest.jvm.events

import info.kinterest.jvm.filter.log
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should be true`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

class DispatcherTest : Spek({
    val dispatcher = Dispatcher<Int>()
    val pool = newFixedThreadPoolContext(4, "test")

    given("having a dispatcher") {
        logger.debug { "start" }
        on("sending to it ") {
            runBlocking { dispatcher.incoming.send(1) }
            it("should work") {}

        }
        val receiver = object {
            var sum = 0
            val received = Channel<Int>()
            val launched : Job
            val expect = 2

            init {
                launched = launch(pool) {
                    for (r in received) {
                        sum += r
                        log.debug { "received $r sum $sum" }
                        if(sum==expect) received.close()
                    }

                }

            }

            fun close() = {
                runBlocking { dispatcher.unsubscribe.send(received) }
                received.close()
            }
        }

        on("adding a subscriber") {
            runBlocking(pool) { dispatcher.subscribing.send(receiver.received) }
            runBlocking(pool) {
                dispatcher.incoming.send(1)
                dispatcher.incoming.send(1)
            }

            runBlocking(pool) {
                receiver.launched.join()
            }


            it("it should receive all events") {
                receiver.sum `should be equal to` 2
            }
        }

        on("closing the dispatcher") {
            dispatcher.close()
            it("should really be closed") {
                dispatcher.incoming.isClosedForReceive.`should be true`()
                dispatcher.incoming.isClosedForSend.`should be true`()
                dispatcher.subscribing.isClosedForSend.`should be true`()
                logger.debug { "Done." }
            }
        }
    }
}) {
    companion object : KLogging()
}