package info.kinterest.jvm.events

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.launch

class Dispatcher<out T> {
    val incoming : ReceiveChannel<T> = Channel()
    private var outgoing : List<SendChannel<T>> = listOf()
    val subscribing : ReceiveChannel<SendChannel<T>> = Channel()
     init {
         launch(CommonPool) {
             for(s in subscribing) {
                 outgoing += s
             }
         }
         launch(CommonPool) {
             for(t in incoming) for (s in outgoing) s.send(t)
         }
     }

    fun close() {
        incoming.close()
        subscribing.close()
        for(o in outgoing) o.close()
    }
}
