package info.kinterest.filter

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
            NONE -> this
            ININ -> this
            OUTIN -> this
            INOUT -> this
            OUTOUT -> w
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

sealed class FilterEvent<E : KIEntity<K>, K : Any>(val want: FilterWant, val filter: Filter<E, K>)
class FilterCreateEvent<E : KIEntity<K>, K : Any>(val entities: Iterable<E>, want: FilterWant, f: Filter<E, K>) : FilterEvent<E, K>(want, f)
class FilterDeleteEvent<E : KIEntity<K>, K : Any>(val entities: Iterable<E>, want: FilterWant, f: Filter<E, K>) : FilterEvent<E, K>(want, f)
class FilterUpdateEvent<E : KIEntity<K>, K : Any>(val upds: EntityUpdatedEvent<E, K>, want: FilterWant, f: Filter<E, K>) : FilterEvent<E, K>(want, f)
class FilterRelationEvent<E : KIEntity<K>, K : Any>(val upds: EntityRelationEvent<E, K, *, *>, want: FilterWant, f: Filter<E, K>) : FilterEvent<E, K>(want, f)

@Suppress("AddVarianceModifier")
interface Filter<E : KIEntity<K>, K : Any> {
    val meta: KIEntityMeta
    fun matches(e: E): Boolean
    fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant
    fun wants(rel: EntityRelationEvent<E, K, *, *>): FilterWant
    fun inverse(): Filter<E, K>
    fun and(f: Filter<E, K>): Filter<E, K>

    companion object {
        fun <E : KIEntity<K>, K : Any> nofilter(meta: KIEntityMeta): Filter<E, K> = object : Filter<E, K> {
            override val meta: KIEntityMeta
                get() = meta

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

}

open class FilterWrapper<E : KIEntity<K>, K : Any>(f: Filter<E, K>) : AbstractFilterWrapper<E, K>(f) {
    override fun inverse(): Filter<E, K> = FilterWrapper(f.inverse())

    override fun and(f: Filter<E, K>): AbstractFilterWrapper<E, K> = if (f is AbstractFilterWrapper) FilterWrapper(this.f.and(f.f)) else FilterWrapper(this.f.and(f))

    companion object {
        fun <E : KIEntity<K>, K : Any> nofilter(meta: KIEntityMeta): AbstractFilterWrapper<E, K> = FilterWrapper(Filter.nofilter(meta))
    }
}

expect class IdFilter<E : KIEntity<K>, K : Any>(ids: Set<K>, meta: KIEntityMeta) : Filter<E, K>
