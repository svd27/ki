package info.kinterest

import info.kinterest.meta.KIProperty

sealed class KIEvent
sealed class EntityEvent<out E : KIEntity<K>, out K : Any>(val entity: E) : KIEvent()
data class EntityCreateEvent<out E : KIEntity<K>, out K : Any>(private val e: E) : EntityEvent<E, K>(e)
data class EntityDeleteEvent<out E : KIEntity<K>, out K : Any>(private val e: E) : EntityEvent<E, K>(e)
data class EntityUpdatedEvent<out E : KIEntity<K>, out K : Any>(private val e: E, val updates: Iterable<EntityUpdated<*>>) : EntityEvent<E, K>(e) {
    fun <V : Any> history(prop: KIProperty<V>): Iterable<V?> = updates.filter { it.prop == prop }.let {
        if (it.isEmpty()) listOf()
        else {
            val first = it.elementAt(0)
            val last = it.takeLast(1).elementAt(0)
            val rest = it.drop(1)
            @Suppress("UNCHECKED_CAST")
            (listOf(first.old) + rest.map { it.old } + listOf(last.new)) as Iterable<V?>
        }
    }
}

data class EntityUpdated<out V : Any>(val prop: KIProperty<V>, val old: V?, val new: V?)
sealed class KIErrorEvent<T:KIError> : KIEvent()
class KIRecoverableErrorEvent(val msg:String, val ex:Exception?=null) : KIErrorEvent<KIRecoverableError>()
class KIFatalErrorEvent(val msg:String, val ex:Throwable) : KIErrorEvent<KIFatalError>()