package info.kinterest

import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty

sealed class KIEvent
sealed class EntityEvent<E:KIEntity<K>,K:Comparable<K>>(val id: K, val meta: KIEntityMeta<K>) : KIEvent()
class EntityCreateEvent<E:KIEntity<K>, K:Comparable<K>>(id:K, meta: KIEntityMeta<K>) : EntityEvent<E,K>(id, meta)
class EntityDeleteEvent<E:KIEntity<K>, K:Comparable<K>>(id:K, meta: KIEntityMeta<K>) : EntityEvent<E,K>(id, meta)
class EntityUpdatedEvent<E:KIEntity<K>, K:Comparable<K>>(id:K, meta: KIEntityMeta<K>, val updates:Iterable<EntityUpdateEvent<*>>) : EntityEvent<E,K>(id, meta)
class EntityUpdateEvent<V:Any>(val prop:KIProperty<V>, val old:V?, val new:V?)
sealed class KIErrorEvent : KIEvent()
sealed class KIRecoverableErrorEvent(val msg:String, val ex:Exception?=null)
sealed class KIFatalErrorEvent(val ex:Throwable)