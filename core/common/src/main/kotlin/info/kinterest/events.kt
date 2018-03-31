package info.kinterest

import info.kinterest.meta.KIProperty

sealed class KIEvent
sealed class EntityEvent<out E : KIEntity<K>, out K : Any>(val entity: E) : KIEvent()
class EntityCreateEvent<out E : KIEntity<K>, out K : Any>(e: E) : EntityEvent<E, K>(e)
class EntityDeleteEvent<out E : KIEntity<K>, out K : Any>(e: E) : EntityEvent<E, K>(e)
class EntityUpdatedEvent<out E : KIEntity<K>, out K : Any>(e: E, val updates: Iterable<EntityUpdated<*>>) : EntityEvent<E, K>(e)
class EntityUpdated<V : Any>(val prop: KIProperty<V>, val old: V?, val new: V?)
sealed class KIErrorEvent<T:KIError> : KIEvent()
class KIRecoverableErrorEvent(val msg:String, val ex:Exception?=null) : KIErrorEvent<KIRecoverableError>()
class KIFatalErrorEvent(val msg:String, val ex:Throwable) : KIErrorEvent<KIFatalError>()