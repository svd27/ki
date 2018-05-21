package info.kinterest.jvm.filter

import info.kinterest.*
import info.kinterest.filter.*
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.meta.KIRelationProperty
import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import mu.KLogging
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

sealed class KIFilter<T> {
    abstract fun matches(e: T): Boolean
}

inline fun <E : KIEntity<K>, K : Any> filter(meta: KIEntityMeta, crossinline cb: EntityFilter<E, K>.() -> Filter<E, K>): LiveFilterWrapper<E, K> = EntityFilter.Empty<E, K>(meta).run {
    EntityFilter.LiveFilterWrapper(this.cb())
}

typealias LiveFilterWrapper<E, K> = EntityFilter.LiveFilterWrapper<E, K>


@Suppress("EqualsOrHashCode")
sealed class EntityFilter<E : KIEntity<K>, K : Any>(override val meta: KIEntityMeta) : KIFilter<E>(), Filter<E, K> {
    /**
     * returns which properties affect this filter on one level depth
     *
     * a < 12 || b < 11 will return (a,b)
     *
     * whereas a < 12 || (b < 11 && a > 12) will return (a)
     */
    abstract val affectedBy: Set<KIProperty<*>>
    /**
     * returns all properties which affect this filter
     */
    abstract val affectedByAll: Set<KIProperty<*>>

    abstract override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant

    class Empty<E : KIEntity<K>, K : Any>(meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
        override val affectedBy: Set<KIProperty<*>>
            get() = setOf()
        override val affectedByAll: Set<KIProperty<*>>
            get() = affectedBy

        override fun matches(e: E): Boolean = DONTDOTHIS()

        override fun inverse(): EntityFilter<E, K> = DONTDOTHIS()
        override fun contentEquals(f: EntityFilter<*, *>): Boolean = DONTDOTHIS()
        override fun wants(upd: EntityUpdatedEvent<E, K>) = DONTDOTHIS()
        override fun wants(rel: EntityRelationEvent<E, K, *, *>): FilterWant = DONTDOTHIS()
    }

    class LiveFilterWrapper<E : KIEntity<K>, K : Any>(f: Filter<E, K>) : FilterWrapper<E, K>(f) {
        var listener: SendChannel<FilterEvent<E, K>>? = null

        fun digest(ev: EntityEvent<E, K>) {

            listener?.let {
                launch(context) {
                    val send = when (ev) {
                        is EntityCreateEvent -> {
                            val entities = ev.entities.filter { matches(it) }
                            if (entities.isNotEmpty()) FilterCreateEvent(entities, FilterWant.OUTIN, this@LiveFilterWrapper) else null
                        }
                        is EntityDeleteEvent -> {
                            val entities = ev.entities.filter { matches(it) }
                            if (entities.isNotEmpty()) FilterDeleteEvent(entities, FilterWant.INOUT, this@LiveFilterWrapper) else null
                        }
                        is EntityUpdatedEvent -> {
                            val wants = wants(ev)
                            when (wants) {
                                FilterWant.NONE, FilterWant.OUTOUT -> null
                                FilterWant.ININ, FilterWant.OUTIN, FilterWant.INOUT -> FilterUpdateEvent(ev, wants, this@LiveFilterWrapper)
                            }
                        }
                        is EntityRelationEvent<E, K, *, *> -> {
                            val wants = wants(ev)
                            when (wants) {
                                FilterWant.NONE, FilterWant.OUTOUT -> null
                                FilterWant.ININ, FilterWant.OUTIN, FilterWant.INOUT -> FilterRelationEvent(ev, wants, this@LiveFilterWrapper)
                            }
                        }
                    }
                    logger.debug { "digest \n$ev \nsending $send\nfilter: ${this@LiveFilterWrapper}" }
                    if (send != null) it.send(send)
                }
            }
        }

        override fun matches(e: E): Boolean = f.matches(e)

        override fun wants(upd: EntityUpdatedEvent<E, K>) = f.wants(upd)

        override fun equals(other: Any?): Boolean = this === other

        override fun hashCode(): Int = System.identityHashCode(this)

        override fun toString(): String = "${meta.name}{$f}"

        override fun inverse(): Filter<E, K> = LiveFilterWrapper(f.inverse())

        override fun and(f: Filter<E, K>): LiveFilterWrapper<E, K> = if (f is LiveFilterWrapper) LiveFilterWrapper(this.f.and(f.f)) else
            LiveFilterWrapper(this.f.and(f))
    }

    class NoneFilter<E : KIEntity<K>, K : Any>(meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
        override val affectedBy: Set<KIProperty<*>>
            get() = emptySet()
        override val affectedByAll: Set<KIProperty<*>>
            get() = affectedBy

        override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant = FilterWant.ININ

        override fun wants(rel: EntityRelationEvent<E, K, *, *>): FilterWant = FilterWant.ININ

        override fun matches(e: E): Boolean = true

        override fun inverse(): EntityFilter<E, K> = AllFilter(meta)

        override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is NoneFilter && f.meta == meta
    }

    class AllFilter<E : KIEntity<K>, K : Any>(meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
        override val affectedBy: Set<KIProperty<*>>
            get() = meta.props.values.toSet()
        override val affectedByAll: Set<KIProperty<*>>
            get() = affectedBy

        override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant = FilterWant.ININ

        override fun wants(rel: EntityRelationEvent<E, K, *, *>): FilterWant = FilterWant.ININ

        override fun matches(e: E): Boolean = true

        override fun inverse(): EntityFilter<E, K> = NoneFilter(meta)

        override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is AllFilter && f.meta == meta
    }

    fun ids(vararg ids: K): StaticEntityFilter<E, K> = StaticEntityFilter(ids.toSet(), meta)
    infix fun <P : Comparable<P>> String.eq(p: P): EQFilter<E, K, P> = meta.props[this]?.let {
        @Suppress("UNCHECKED_CAST")
        EQFilter<E, K, P>(it as KIProperty<P>, meta, p)
    } ?: throw FilterError("property $this not found in ${meta.me}")

    fun <P : Any> String.isNull(): PropertyNullFilter<E, K, P> = PropertyNullFilter(meta.props[this]!!.cast(), meta)
    infix fun <P : Comparable<P>> String.neq(value: P): NEQFilter<E, K, P> = (this eq value).inverse()
    infix fun <P : Comparable<P>> String.gt(value: P): GTFilter<E, K, P> = withProp(this, value) { GTFilter(it, meta, value) }
    infix fun <P : Comparable<P>> String.lt(value: P): LTFilter<E, K, P> = withProp(this, value) { LTFilter(it, meta, value) }
    infix fun <P : Comparable<P>> String.gte(value: P): GTEFilter<E, K, P> = withProp(this, value) { GTEFilter(it, meta, value) }
    infix fun <P : Comparable<P>> String.lte(value: P): LTEFilter<E, K, P> = withProp(this, value) { LTEFilter(it, meta, value) }
    infix fun <P : Comparable<P>> String.`in`(values: Set<P>): PropertyInFilter<E, K, P> = withProp(this, values.first()) { PropertyInFilter(values, it, meta) }
    infix fun <P : Comparable<P>> String.notin(values: Set<P>): PropertyNotInFilter<E, K, P> = withProp(this, values.first()) { PropertyNotInFilter(values, it, meta) }

    fun or(f: Iterable<EntityFilter<E, K>>): EntityFilter<E, K> {
        if (f.count() == 0) return this
        val first = f.first()
        return when (this) {
            is AllFilter -> if (f.count() == 1) first else first.or(f.drop(1))
            is NoneFilter -> this
            is CombinationFilter -> when (this) {
                is OrFilter -> {
                    @Suppress("UNCHECKED_CAST")
                    when (first) {
                        is OrFilter<*, *> ->
                            OrFilter(this.operands + first.operands as Iterable<EntityFilter<E, K>>,
                                    meta).or(f.drop(1))
                        else -> OrFilter(this.operands + f, meta)
                    }
                }
                else -> OrFilter(listOf(this) + f, meta)
            }
            else -> when (first) {
                is OrFilter<*, *> -> (first as OrFilter<E, K>).or(f.drop(1) + this)
                else -> OrFilter(listOf(this) + f, meta)
            }
        }
    }

    override infix fun and(f: Filter<E, K>): Filter<E, K> = if (f is EntityFilter) this.and(listOf(f)) else DONTDOTHIS()

    fun and(vararg fs: EntityFilter<E, K>): EntityFilter<E, K> = and(fs.toList())
    fun and(f: Iterable<EntityFilter<E, K>>): EntityFilter<E, K> {
        if (f.count() == 0) return this
        val first = f.first()
        return when (this) {
            is AllFilter -> if (f.count() == 1) first else first.and(f.drop(1))
            is NoneFilter -> this
            is CombinationFilter -> when (this) {
                is AndFilter -> {
                    @Suppress("UNCHECKED_CAST")
                    when (first) {
                        is AndFilter<*, *> ->
                            AndFilter(this.operands + first.operands as Iterable<EntityFilter<E, K>>,
                                    meta).and(f.drop(1))
                        else -> AndFilter(this.operands + f, meta)
                    }
                }
                else -> AndFilter(listOf(this) + f, meta)
            }
            else -> when (first) {
                is AndFilter<*, *> -> (first as AndFilter<E, K>).and(f.drop(1) + this)
                else -> AndFilter(listOf(this) + f, meta)
            }
        }
    }

    @Suppress("UNUSED_PARAMETER")
    private fun <R, P : Any> withProp(name: String, value: P, cb: (KIProperty<P>) -> R): R = meta.props[name]?.let { cb(it.cast()) }
            ?: throw FilterError("property $this not found in ${meta.me}")

    abstract override fun inverse(): EntityFilter<E, K>

    override fun equals(other: Any?): Boolean = if (other === this) true else {
        if (other is EntityFilter<*, *>) {
            if (other.meta == meta) contentEquals(other)
            else false
        } else false
    }

    protected abstract fun contentEquals(f: EntityFilter<*, *>): Boolean

    companion object : KLogging() {
        val context: CoroutineDispatcher = newFixedThreadPoolContext(2, "filter")
    }
}

sealed class IdFilter<E : KIEntity<K>, K : Any>(meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
    override val affectedBy: Set<KIProperty<*>>
        get() = setOf()
    override val affectedByAll: Set<KIProperty<*>>
        get() = affectedBy

    override fun wants(upd: EntityUpdatedEvent<E, K>) = FilterWant.NONE
    override fun wants(rel: EntityRelationEvent<E, K, *, *>): FilterWant = FilterWant.NONE
}

abstract class AnIdFilter<E : KIEntity<K>, K : Any>(meta: KIEntityMeta) : IdFilter<E, K>(meta)
@Suppress("EqualsOrHashCode")
class StaticEntityFilter<E : KIEntity<K>, K : Any>(val ids: Set<K>, meta: KIEntityMeta) : IdFilter<E, K>(meta), Filter<E, K> {
    override fun matches(e: E): Boolean = e.id in ids

    inner class Inverse : AnIdFilter<E, K>(meta) {
        val ids get() = this@StaticEntityFilter.ids
        override fun matches(e: E): Boolean = !this@StaticEntityFilter.matches(e)

        override fun inverse(): IdFilter<E, K> = this@StaticEntityFilter
        override fun contentEquals(f: EntityFilter<*, *>): Boolean = if (f is Inverse) ids == f.ids else false
        override fun toString(): String = "not(${this@StaticEntityFilter})"
    }

    override fun inverse(): IdFilter<E, K> = Inverse()

    override fun contentEquals(f: EntityFilter<*, *>): Boolean = if (f is StaticEntityFilter<*, *>) ids == f.ids else false
    override fun hashCode(): Int = ids.hashCode()
}

sealed class IdValueFilter<E : KIEntity<K>, K : Any>(meta: KIEntityMeta) : IdFilter<E, K>(meta)
class IdComparisonFilter<E : KIEntity<K>, K : Any>(meta: KIEntityMeta, val valueFilter
: PropertyValueFilter<E, K, K>) : IdValueFilter<E, K>(meta) {
    override fun matches(e: E): Boolean = valueFilter.matches(e)

    @Suppress("UNCHECKED_CAST")
    override fun inverse(): EntityFilter<E, K> = IdComparisonFilter(meta, valueFilter.inverse() as PropertyValueFilter<E, K, K>)

    override fun contentEquals(f: EntityFilter<*, *>): Boolean = DONTDOTHIS("equals overridden")
    override fun equals(other: Any?): Boolean = if (other is IdComparisonFilter<*, *>) valueFilter == other.valueFilter else false
    override fun hashCode(): Int = valueFilter.hashCode()
    override fun toString(): String = valueFilter.toString()
}

sealed class PropertyFilter<E : KIEntity<K>, K : Any, P : Any>(val prop: KIProperty<P>, meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
    override val affectedBy: Set<KIProperty<*>> = setOf(prop)
    override val affectedByAll: Set<KIProperty<*>>
        get() = affectedBy

    override fun wants(rel: EntityRelationEvent<E, K, *, *>): FilterWant = FilterWant.NONE

    companion object {
        fun valueToString(value: Any?): String = when (value) {
            is LocalDate -> """date("${value.format(DateTimeFormatter.ofPattern("yyyyMMdd"))}", "yyyyMMdd")"""
            is LocalDateTime -> """datetime("${value.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"))}", "yyyyMMddHHmmssSSS")"""
            is OffsetDateTime -> """offsetDatetime("${value.format(DateTimeFormatter.ofPattern("yyyyMMdwdHHmmssSSSX"))}", "yyyyMMdwdHHmmssSSSX")"""
            else -> "$value"
        }
    }
}

class PropertyNullFilter<E : KIEntity<K>, K : Any, P : Any>(prop: KIProperty<P>, meta: KIEntityMeta) : PropertyFilter<E, K, P>(prop, meta) {
    override fun matches(e: E): Boolean = (e.getValue(prop)) == null

    override fun inverse(): EntityFilter<E, K> = PropertyNotNullFilter(prop, meta)
    override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is PropertyNullFilter<*, *, *> && f.prop == prop

    @Suppress("UNCHECKED_CAST")
    override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant = upd.history(prop as KIProperty<Any>).run {
        when {
            firstOrNull() == null && lastOrNull() != null -> FilterWant.INOUT
            firstOrNull() != null && lastOrNull() == null -> FilterWant.OUTIN
            firstOrNull() == null && lastOrNull() == null -> FilterWant.ININ
            firstOrNull() != null && lastOrNull() != null -> FilterWant.OUTOUT
            else -> FilterWant.NONE
        }
    }

    override fun toString(): String = "${prop.name} is null"
}

class PropertyNotNullFilter<E : KIEntity<K>, K : Any, P : Any>(prop: KIProperty<P>, meta: KIEntityMeta) : PropertyFilter<E, K, P>(prop, meta) {
    override fun matches(e: E): Boolean = e.getValue(prop) != null

    override fun inverse(): EntityFilter<E, K> = PropertyNullFilter(prop, meta)
    override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is PropertyNullFilter<*, *, *> && f.prop == prop
    @Suppress("UNCHECKED_CAST")
    override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant = upd.history(prop as KIProperty<Any>).run {
        when {
            firstOrNull() == null && lastOrNull() != null -> FilterWant.OUTIN
            firstOrNull() != null && lastOrNull() == null -> FilterWant.INOUT
            firstOrNull() == null && lastOrNull() == null -> FilterWant.OUTOUT
            firstOrNull() != null && lastOrNull() != null -> FilterWant.ININ
            else -> FilterWant.NONE
        }
    }

    override fun toString(): String = "${prop.name} !is null"
}

class PropertyInFilter<E : KIEntity<K>, K : Any, P : Any>(val values: Set<P>, prop: KIProperty<P>, meta: KIEntityMeta) : PropertyFilter<E, K, P>(prop, meta) {
    override fun matches(e: E): Boolean = relate(e.getValue(prop))
    fun relate(p: P?): Boolean = p in values

    override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant = upd.history(prop).run {
        when {
            relate(firstOrNull()) && !relate(lastOrNull()) -> FilterWant.INOUT
            !relate(firstOrNull()) && relate(lastOrNull()) -> FilterWant.OUTIN
            !relate(firstOrNull()) && !relate(lastOrNull()) -> FilterWant.OUTOUT
            relate(firstOrNull()) && relate(lastOrNull()) -> FilterWant.ININ
            else -> throw IllegalStateException("should never happen")
        }
    }

    override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is PropertyInFilter<*, *, *> && f.prop == prop && f.values == values
    override fun inverse(): EntityFilter<E, K> = PropertyNotInFilter(values, prop, meta)

    override fun toString(): String = "${prop.name} in (${values.map { PropertyFilter.valueToString(it) }.joinToString(",")})"
}

class PropertyNotInFilter<E : KIEntity<K>, K : Any, P : Any>(val values: Set<P>, prop: KIProperty<P>, meta: KIEntityMeta) : PropertyFilter<E, K, P>(prop, meta) {
    override fun matches(e: E): Boolean = relate(e.getValue(prop))
    fun relate(p: P?): Boolean = p !in values

    override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant = upd.history(prop).run {
        when {
            relate(firstOrNull()) && !relate(lastOrNull()) -> FilterWant.INOUT
            !relate(firstOrNull()) && relate(lastOrNull()) -> FilterWant.OUTIN
            !relate(firstOrNull()) && !relate(lastOrNull()) -> FilterWant.OUTOUT
            relate(firstOrNull()) && relate(lastOrNull()) -> FilterWant.ININ
            else -> throw IllegalStateException("should never happen")
        }
    }

    override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is PropertyInFilter<*, *, *> && f.prop == prop && f.values == values
    override fun inverse(): EntityFilter<E, K> = PropertyInFilter(values, prop, meta)

    override fun toString(): String = "${prop.name} !in (${values.map { PropertyFilter.valueToString(it) }.joinToString(",")})"
}

sealed class PropertyValueFilter<E : KIEntity<K>, K : Any, P : Any>(prop: KIProperty<P>, meta: KIEntityMeta, val value: P) : PropertyFilter<E, K, P>(prop, meta) {
    fun match(p: P?): Boolean = relate(p, value)
    abstract fun relate(value: P?, test: P): Boolean
    override fun matches(e: E): Boolean = match(e.getValue(prop))

    override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant = upd.history(prop).run {
        when {
            relate(firstOrNull(), value) && !relate(lastOrNull(), value) -> FilterWant.INOUT
            !relate(firstOrNull(), value) && relate(lastOrNull(), value) -> FilterWant.OUTIN
            !relate(firstOrNull(), value) && !relate(lastOrNull(), value) -> FilterWant.OUTOUT
            relate(firstOrNull(), value) && relate(lastOrNull(), value) -> FilterWant.ININ
            else -> throw IllegalStateException("should never happen")
        }
    }

    /**
     * defines the min/max extend of values for this filter. a null means infinity
     */
    abstract val minmax: Pair<P?, P?>

    override fun contentEquals(f: EntityFilter<*, *>): Boolean = if (this::class == f::class && f is PropertyValueFilter<*, *, *>) {
        prop == f.prop && value == f.value
    } else false


    override fun toString(): String = "${prop.name} $op ${valueToString(value)}"

    abstract val op: String
}

class EQFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, value: P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value == test
    override fun inverse(): NEQFilter<E, K, P> = NEQFilter(prop, meta, value)
    override val minmax: Pair<P?, P?> = value to value
    override val op: String
        get() = "="

}

class NEQFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, value: P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value != test
    override fun inverse(): EQFilter<E, K, P> = EQFilter(prop, meta, value)
    override val minmax: Pair<P?, P?> = null to null
    override val op: String
        get() = "!="
}

class GTFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, value: P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v -> v > test } ?: false
    override fun inverse(): LTEFilter<E, K, P> = LTEFilter(prop, meta, value)
    override val minmax: Pair<P?, P?> = value to null
    override val op: String
        get() = ">"
}

class GTEFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, value: P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v -> v >= test } ?: false
    override fun inverse(): LTFilter<E, K, P> = LTFilter(prop, meta, value)
    override val minmax: Pair<P?, P?> = value to null
    override val op: String get() = ">="
}

class LTFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, value: P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->
        v < test
    } ?: true

    override fun inverse(): GTEFilter<E, K, P> = GTEFilter(prop, meta, value)
    override val minmax: Pair<P?, P?> = null to value
    override val op: String get() = "<"
}

class LTEFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, value: P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->
        v <= test
    } ?: true

    override fun inverse(): GTFilter<E, K, P> = GTFilter(prop, meta, value)
    override val minmax: Pair<P?, P?> = null to value
    override val op: String get() = "<="
}

sealed class CombinationFilter<E : KIEntity<K>, K : Any>(val operands: Iterable<EntityFilter<E, K>>, meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
    abstract fun wantCombiner(w1: FilterWant, w2: FilterWant): FilterWant
    override fun wants(upd: EntityUpdatedEvent<E, K>): FilterWant = operands.map { it.wants(upd) }.reduce { w1, w2 -> wantCombiner(w1, w2) }
    override fun wants(rel: EntityRelationEvent<E, K, *, *>): FilterWant = operands.map { it.wants(rel) }.reduce { w1, w2 -> if (this is AndFilter) w1.and(w2) else w1.or(w2) }

    override val affectedByAll: Set<KIProperty<*>>
        get() = operands.fold(setOf()) { acc, op ->
            acc + op.affectedByAll
        }

    override val affectedBy: Set<KIProperty<*>>
        get() = operands.filterIsInstance<PropertyFilter<*, *, *>>().flatMap { it.affectedBy }.toSet()

    override fun contentEquals(f: EntityFilter<*, *>): Boolean = if (this::class == f::class && f is CombinationFilter) {
        operands.all { source -> f.operands.any { source == it } }
                && f.operands.all { source -> operands.any { source == it } }
    } else false

    abstract val op: String
    override fun toString(): String = operands.map { "($it)" }.joinToString(op)
}

class AndFilter<E : KIEntity<K>, K : Any>(operands: Iterable<EntityFilter<E, K>>, meta: KIEntityMeta) : CombinationFilter<E, K>(operands, meta) {
    override fun matches(e: E): Boolean = operands.all { it.matches(e) }

    override fun inverse(): EntityFilter<E, K> = OrFilter(operands.map(EntityFilter<E, K>::inverse), meta)

    override val op: String get() = "&&"

    override fun wantCombiner(w1: FilterWant, w2: FilterWant): FilterWant = w1.and(w2)
}

class OrFilter<E : KIEntity<K>, K : Any>(operands: Iterable<EntityFilter<E, K>>, meta: KIEntityMeta) : CombinationFilter<E, K>(operands, meta) {
    override fun matches(e: E): Boolean = operands.any { it.matches(e) }

    override fun inverse(): EntityFilter<E, K> = AndFilter(operands.map { it.inverse() }, meta)

    override val op: String get() = "||"

    override fun wantCombiner(w1: FilterWant, w2: FilterWant): FilterWant = w1.or(w2)
}

sealed class RelationFilter<S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(meta: KIEntityMeta, val rel: KIRelationProperty, val rf: EntityFilter<T, L>) : EntityFilter<S, K>(meta) {
    override val affectedBy: Set<KIProperty<*>>
        get() = setOf(rel)
    override val affectedByAll: Set<KIProperty<*>>
        get() = affectedBy

    override fun wants(upd: EntityUpdatedEvent<S, K>): FilterWant = FilterWant.NONE

}

/*
class AnyRelationFilter<S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(meta: KIEntityMeta, rel: KIRelationProperty, rf: EntityFilter<T, L>) : RelationFilter<S, K, T, L>(meta, rel, rf) {
    override fun wants(rel: EntityRelationEvent<S, K, *, *>): FilterWant = if (rel.relation.rel == this.rel)
        when (rel) {
            is EntityRelationsAdded -> {
                val relation = rel.relation as Relation<S, T, K, L>
                val target = relation.target
                if (rf.matches(target)) {
                    val value = relation.source.getValue(rel.relation.rel) as? Collection<T>
                    if (value?.filter { it != relation.target }?.none { rf.matches(it) } ?: false) {
                        FilterWant.OUTIN
                    } else FilterWant.NONE
                } else FilterWant.NONE
            }
            is EntityRelationsRemoved -> {
                val relation = rel.relation as Relation<S, T, K, L>
                val target = relation.target
                if (rf.matches(target)) {
                    val value = relation.source.getValue(rel.relation.rel) as? Collection<T>
                    if (value?.filter { it != relation.target }?.none { rf.matches(it) } ?: true) {
                        FilterWant.INOUT
                    } else FilterWant.NONE
                } else FilterWant.NONE
            }

        } else FilterWant.NONE

    override fun matches(e: S): Boolean = (e.getValue(rel) as Collection<T>).any { rf.matches(it) }
    override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is AnyRelationFilter<*,*,*,*> && rel == f.rel && rf == f.rf
}
*/

