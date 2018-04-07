package info.kinterest

import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page

sealed class KIEvent
sealed class EntityEvent<out E : KIEntity<K>, out K : Any> : KIEvent()
data class EntityCreateEvent<out E : KIEntity<K>, out K : Any>(val entities: Iterable<E>) : EntityEvent<E, K>()
data class EntityDeleteEvent<out E : KIEntity<K>, out K : Any>(val entities: Iterable<E>) : EntityEvent<E, K>()
data class EntityUpdatedEvent<out E : KIEntity<K>, out K : Any>(val entity: E, val updates: Iterable<EntityUpdated<*>>) : EntityEvent<E, K>() {
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

sealed class InterestEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any>
sealed class InterestContainedEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(open val interest: I)
data class InterestCreated<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I) : InterestContainedEvent<I, E, K>(interest)
data class InterestDeleted<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(val id: Any) : InterestEvent<I, E, K>()
sealed class InterestEntityEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(interest: I) : InterestContainedEvent<I, E, K>(interest)
/**
 * will be sent once after created event to indicate that all entities have been lodaed
 */
data class InterestLoaded<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I, val entities: Iterable<E>) : InterestEntityEvent<I, E, K>(interest)

/**
 * sent after paging changed and new page is established
 */
data class InterestPaged<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(
        override val interest: I, val paging: Page<E, K>) : InterestEntityEvent<I, E, K>(interest)

/**
 * sent when no page change happened but current page changed due to ordering
 */
data class InterestPageChanged<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I, val removals: Iterable<E>, val inserts: Iterable<Pair<Int, E>>) : InterestEntityEvent<I, E, K>(interest)

/**
 * sent when an entity becomes into the purvey of this interest due to creation or update
 */
data class InterestEntitiesAdded<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I, val added: Iterable<E>) : InterestEntityEvent<I, E, K>(interest)

/**
 * sent when an entity falls out of an interest, maybe due to deletion or update
 */
data class InterestEntitiesRemoved<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I, val removed: Iterable<E>) : InterestEntityEvent<I, E, K>(interest)


sealed class DataStoreEvent(val ds: DataStore) : KIEvent()
class StoreReady(ds: DataStore) : DataStoreEvent(ds)
class StoreDown(ds: DataStore) : DataStoreEvent(ds)
