package info.kinterest.jvm.events

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.launch

class Dispatcher<out T> {
    private val _incoming = Channel<T>()
    val incoming : ReceiveChannel<T> get() =  _incoming
    private var outgoing : List<SendChannel<T>> = listOf()
    val _subscribing = Channel<T>()
    val subscribing : ReceiveChannel<SendChannel<T>> get() = _subscribing
    
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
        _incoming.close()
        _subscribing.close()
        for(o in outgoing) o.close()
    }
}
