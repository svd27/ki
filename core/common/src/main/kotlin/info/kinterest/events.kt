package info.kinterest

import info.kinterest.meta.KIProperty
import info.kinterest.query.EntityProjection
import info.kinterest.query.Projection
import info.kinterest.query.ProjectionResult
import info.kinterest.query.QueryResult

sealed class KIEvent
@Suppress("unused")
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

@Suppress("unused")
sealed class InterestEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any> : KIEvent()
sealed class InterestContainedEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(open val interest: I) : InterestEvent<I, E, K>()
data class InterestCreated<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I) : InterestContainedEvent<I, E, K>(interest)
data class InterestDeleted<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(val id: Any) : InterestEvent<I, E, K>()
data class InterestLoadedEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I, val result: QueryResult<E, K>) : InterestContainedEvent<I, E, K>(interest)
data class InterestProjectionEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I, val evts: Iterable<ProjectionEvent<E, K>>) : InterestContainedEvent<I, E, K>(interest)

sealed class ProjectionEvent<E : KIEntity<K>, K : Any>(open val projection: Projection<E, K>)
data class ProjectionChanged<E : KIEntity<K>, K : Any>(override val projection: Projection<E, K>) : ProjectionEvent<E, K>(projection)
data class ProjectionLoaded<E : KIEntity<K>, K : Any>(val result: ProjectionResult<E, K>) : ProjectionEvent<E, K>(result.projection)
data class ProjectionPageChanged<E : KIEntity<K>, K : Any>(override val projection: EntityProjection<E, K>, val removals: Iterable<E>, val inserts: Iterable<Pair<Int, E>>) : ProjectionEvent<E, K>(projection)
data class ProjectionEntitiesAdded<E : KIEntity<K>, K : Any>(override val projection: EntityProjection<E, K>, val added: Iterable<E>) : ProjectionEvent<E, K>(projection)
data class ProjectionEntitiesRemoved<E : KIEntity<K>, K : Any>(override val projection: EntityProjection<E, K>, val removals: Iterable<E>) : ProjectionEvent<E, K>(projection)


sealed class DataStoreEvent(val ds: DataStore) : KIEvent()
class StoreReady(ds: DataStore) : DataStoreEvent(ds)
class StoreDown(ds: DataStore) : DataStoreEvent(ds)
