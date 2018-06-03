package info.kinterest.filter

import info.kinterest.EntityEvent
import info.kinterest.EntityRelationEvent
import info.kinterest.EntityUpdatedEvent
import info.kinterest.KIEntity
import info.kinterest.meta.KIEntityMeta

enum class FilterWant {
    ININ, OUTIN, INOUT, OUTOUT, NONE;

    fun and(w: FilterWant): FilterWant = when (this) {
        w -> this
        NONE -> w
        ININ -> w.and(this)
        OUTIN -> w.and(this)
        INOUT -> when (w) {
            NONE -> INOUT
            ININ -> INOUT
            OUTIN -> INOUT
            INOUT -> INOUT
            OUTOUT -> OUTOUT
        }
        OUTOUT -> OUTOUT
    }

    fun or(w: FilterWant): FilterWant = when (this) {
        w -> this
        NONE -> w
        ININ -> ININ
        INOUT -> w.or(this)
        OUTIN -> when (w) {
            NONE -> this
            ININ -> ININ
            OUTIN -> OUTIN
            INOUT -> ININ
            OUTOUT -> OUTIN
        }
        OUTOUT -> w.or(this)
    }
}


@Suppress("AddVarianceModifier")
interface Filter<E : KIEntity<K>, K : Any> {
    val meta: KIEntityMeta
    fun matches(e: E): Boolean
    fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant
    fun wants(rel: EntityRelationEvent<E, K, *, *>): FilterWant
    fun inverse(): Filter<E, K>
    fun and(f: Filter<E, K>): Filter<E, K>
    fun relationFilters(): Iterable<Filter<E, K>>

    companion object {
        fun <E : KIEntity<K>, K : Any> nofilter(meta: KIEntityMeta): Filter<E, K> = object : Filter<E, K> {
            override val meta: KIEntityMeta
                get() = meta

            override fun relationFilters(): Iterable<Filter<E, K>> = emptyList()

            override fun matches(e: E): Boolean = false
            override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant = FilterWant.OUTOUT
            override fun wants(rel: EntityRelationEvent<E, K, *, *>): FilterWant = FilterWant.OUTOUT
            override fun inverse(): Filter<E, K> = this
            override fun and(f: Filter<E, K>): Filter<E, K> = this
        }
    }
}

abstract class AbstractFilterWrapper<E : KIEntity<K>, K : Any>(filter: Filter<E, K>) : Filter<E, K> {
    val f: Filter<E, K> = if (filter is AbstractFilterWrapper<E, K>) {
        filter.f
    } else filter
    override val meta: KIEntityMeta
        get() = f.meta

    override fun matches(e: E): Boolean = f.matches(e)

    override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant = f.wants(upd)
    override fun wants(rel: EntityRelationEvent<E, K, *, *>): FilterWant = f.wants(rel)
    override abstract fun and(f: Filter<E, K>): AbstractFilterWrapper<E, K>

    abstract fun digest(ev: EntityEvent<E, K>)

    override fun toString(): String = "FilterWrapper { $f }"
}

open class FilterWrapper<E : KIEntity<K>, K : Any>(f: Filter<E, K>) : AbstractFilterWrapper<E, K>(f) {
    override fun inverse(): Filter<E, K> = FilterWrapper(f.inverse())

    override fun and(f: Filter<E, K>): FilterWrapper<E, K> = if (f is AbstractFilterWrapper) FilterWrapper(this.f.and(f.f)) else FilterWrapper(this.f.and(f))

    override fun relationFilters(): Iterable<Filter<E, K>> = f.relationFilters()

    override fun digest(ev: EntityEvent<E, K>) {}

    companion object {
        fun <E : KIEntity<K>, K : Any> nofilter(meta: KIEntityMeta): FilterWrapper<E, K> = FilterWrapper(Filter.nofilter(meta))
    }
}

abstract class RelationFilterWrapper<T : KIEntity<L>, L : Any, S : KIEntity<K>, K : Any>(f: Filter<T, L>, val parent: FilterWrapper<S, K>, val relationFilter: Filter<S, K>) : FilterWrapper<T, L>(f)

expect class IdFilter<E : KIEntity<K>, K : Any>(ids: Set<K>, meta: KIEntityMeta) : Filter<E, K>
