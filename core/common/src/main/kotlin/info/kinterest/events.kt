package info.kinterest

import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty

sealed class KIEvent
sealed class EntityEvent<E:KIEntity<K>,K:Any>(val id: K, val meta: KIEntityMeta) : KIEvent()
class EntityCreateEvent<E:KIEntity<K>, K:Any>(id:K, meta: KIEntityMeta) : EntityEvent<E,K>(id, meta)
class EntityDeleteEvent<E:KIEntity<K>, K:Any>(id:K, meta: KIEntityMeta) : EntityEvent<E,K>(id, meta)
class EntityUpdatedEvent<E:KIEntity<K>, K:Any>(id:K, meta: KIEntityMeta, val updates:Iterable<EntityUpdateEvent<*>>) : EntityEvent<E,K>(id, meta)
class EntityUpdateEvent<V:Any>(val prop:KIProperty<V>, val old:V?, val new:V?)
sealed class KIErrorEvent<T:KIError> : KIEvent()
class KIRecoverableErrorEvent(val msg:String, val ex:Exception?=null) : KIErrorEvent<KIRecoverableError>()
class KIFatalErrorEvent(val msg:String, val ex:Throwable) : KIErrorEvent<KIFatalError>()