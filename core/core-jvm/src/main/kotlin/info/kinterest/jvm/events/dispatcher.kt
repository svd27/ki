package info.kinterest.jvm.events

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.selects.select
import mu.KLogging

class Dispatcher<T> {
    val incoming : Channel<T> = Channel()
    private var outgoing : List<Channel<T>> = listOf()
    val subscribing : Channel<Channel<T>> = Channel()
     init {
         launch(CommonPool) {
             for(s in subscribing) {
                 launch { s.receiveOrNull(); logger.debug { "removing $s" }; outgoing -= s }
                 outgoing += s
             }
         }
         launch(CommonPool) {
             for(t in incoming)
                 for (s in outgoing)
                     s.send(t)
         }
     }

    fun close() {
        incoming.close()
        subscribing.close()
        for(o in outgoing) o.close()
    }

    companion object : KLogging() {

    }
}
