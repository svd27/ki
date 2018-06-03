package info.kinterest

import info.kinterest.filter.Filter
import info.kinterest.filter.FilterWant
import info.kinterest.meta.KIProperty
import info.kinterest.meta.KIRelationProperty
import info.kinterest.meta.Relation
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
sealed class EntityRelationEvent<out S : KIEntity<K>, out K : Any, T : KIEntity<L>, L : Any>(open val relation: Relation<S, T, K, L>) : EntityEvent<S, K>()
data class EntityRelationsAdded<out S : KIEntity<K>, out K : Any, T : KIEntity<L>, L : Any>(override val relation: Relation<S, T, K, L>) : EntityRelationEvent<S, K, T, L>(relation)
data class EntityRelationsRemoved<out S : KIEntity<K>, out K : Any, T : KIEntity<L>, L : Any>(override val relation: Relation<S, T, K, L>) : EntityRelationEvent<S, K, T, L>(relation)


@Suppress("unused")
sealed class InterestEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any> : KIEvent()
sealed class InterestContainedEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(open val interest: I) : InterestEvent<I, E, K>()
data class InterestCreated<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I) : InterestContainedEvent<I, E, K>(interest)
data class InterestDeleted<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(val id: Any) : InterestEvent<I, E, K>()
data class InterestLoadedEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I, val result: QueryResult<E, K>) : InterestContainedEvent<I, E, K>(interest)
data class InterestProjectionEvent<out I : Interest<E, K>, E : KIEntity<K>, K : Any>(override val interest: I, val evts: Iterable<ProjectionEvent<E, K>>) : InterestContainedEvent<I, E, K>(interest)


sealed class FilterEvent<E : KIEntity<K>, K : Any>(val want: FilterWant, val filter: Filter<E, K>) : KIEvent()
class FilterCreateEvent<E : KIEntity<K>, K : Any>(val entities: Iterable<E>, want: FilterWant, f: Filter<E, K>) : FilterEvent<E, K>(want, f)
class FilterDeleteEvent<E : KIEntity<K>, K : Any>(val entities: Iterable<E>, want: FilterWant, f: Filter<E, K>) : FilterEvent<E, K>(want, f)
class FilterUpdateEvent<E : KIEntity<K>, K : Any>(val upds: EntityUpdatedEvent<E, K>, want: FilterWant, f: Filter<E, K>) : FilterEvent<E, K>(want, f)
class FilterRelationEvent<E : KIEntity<K>, K : Any>(val relationEvent: EntityRelationEvent<E, K, *, *>, want: FilterWant, f: Filter<E, K>) : FilterEvent<E, K>(want, f)
sealed class FilterRelationScopeEvent<E : KIEntity<K>, K : Any>(val entity: E, val rel: KIRelationProperty, want: FilterWant, relationFilter: Filter<E, K>) : FilterEvent<E, K>(want, relationFilter)
class FilterRelationInScopeEvent<E : KIEntity<K>, K : Any>(entity: E, rel: KIRelationProperty, want: FilterWant, relationFilter: Filter<E, K>) : FilterRelationScopeEvent<E, K>(entity, rel, want, relationFilter)
class FilterRelationOutOfScopeEvent<E : KIEntity<K>, K : Any>(entity: E, rel: KIRelationProperty, want: FilterWant, relationFilter: Filter<E, K>) : FilterRelationScopeEvent<E, K>(entity, rel, want, relationFilter)

class FilterRelationChangeEvent<E : KIEntity<K>, K : Any>(val entity: E, val want: FilterWant, val relationFilter: Filter<E, K>, val target: KIEntity<Any>)


sealed class ProjectionEvent<E : KIEntity<K>, K : Any>(open val projection: Projection<E, K>)
data class ProjectionChanged<E : KIEntity<K>, K : Any>(override val projection: Projection<E, K>) : ProjectionEvent<E, K>(projection)
data class ProjectionLoaded<E : KIEntity<K>, K : Any>(val result: ProjectionResult<E, K>) : ProjectionEvent<E, K>(result.projection)
data class ProjectionPageChanged<E : KIEntity<K>, K : Any>(override val projection: EntityProjection<E, K>, val removals: Iterable<E>, val inserts: Iterable<Pair<Int, E>>) : ProjectionEvent<E, K>(projection)
data class ProjectionEntitiesAdded<E : KIEntity<K>, K : Any>(override val projection: EntityProjection<E, K>, val added: Iterable<E>) : ProjectionEvent<E, K>(projection)
data class ProjectionEntitiesRemoved<E : KIEntity<K>, K : Any>(override val projection: EntityProjection<E, K>, val removals: Iterable<E>) : ProjectionEvent<E, K>(projection)


sealed class DataStoreEvent(val ds: DataStore) : KIEvent()
class StoreReady(ds: DataStore) : DataStoreEvent(ds)
class StoreDown(ds: DataStore) : DataStoreEvent(ds)
