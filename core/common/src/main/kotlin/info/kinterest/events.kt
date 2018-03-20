package info.kinterest

sealed class KIEvent
sealed class EntityEvent<E:KIEntity<T>,T:Comparable<T>>(val entity: E) : KIEvent()
class EntityCreateEvent<E:KIEntity<K>, K:Comparable<K>>(e:E) : EntityEvent<E,K>(e)
class EntityDeleteEvent<E:KIEntity<K>, K:Comparable<K>>(entity : E,val k:K) : EntityEvent<E,K>(entity)
class EntityUpdateEvent<E:KIEntity<K>, K:Comparable<K>,V:Any>(e:E, val prop:String, val old:V?) : EntityEvent<E,K>(e)
sealed class KIErrorEvent : KIEvent()
sealed class KIRecoverableErrorEvent(val msg:String)
sealed class KIFatalError(val ex:Throwable)