package info.kinterest.jvm.filter

import info.kinterest.*
import info.kinterest.filter.Filter
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
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

inline fun <E : KIEntity<K>, K : Any> filter(meta: KIEntityMeta, crossinline cb: EntityFilter<E, K>.() -> EntityFilter<E, K>): FilterWrapper<E, K> = EntityFilter.Empty<E, K>(meta).run {
    EntityFilter.FilterWrapper<E, K>(meta).apply {
        val filter = this.cb()
        f = (filter as? FilterWrapper)?.f ?: filter
    }
}

typealias FilterWrapper<E, K> = EntityFilter.FilterWrapper<E, K>


interface IFilterWrapper<E : KIEntity<K>, K : Any> {
    val f: Filter<E, K>
    val meta: KIEntityMeta
    fun matches(e: E): Boolean
    fun wants(upd: EntityUpdatedEvent<E, K>): Boolean
    fun inverse(): IFilterWrapper<E, K>
}

@Suppress("EqualsOrHashCode")
sealed class EntityFilter<E : KIEntity<K>, K : Any>(override val meta: KIEntityMeta) : KIFilter<E>(), IFilterWrapper<E, K> {
    override val f: Filter<E, K>
        get() = this
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

    abstract override fun wants(upd: EntityUpdatedEvent<E, K>): Boolean

    class Empty<E : KIEntity<K>, K : Any>(meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
        override val affectedBy: Set<KIProperty<*>>
            get() = setOf()
        override val affectedByAll: Set<KIProperty<*>>
            get() = affectedBy

        override fun matches(e: E): Boolean = DONTDOTHIS()

        override fun inverse(): EntityFilter<E, K> = DONTDOTHIS()
        override fun contentEquals(f: EntityFilter<*, *>): Boolean = DONTDOTHIS()
        override fun wants(upd: EntityUpdatedEvent<E, K>) = DONTDOTHIS()
    }

    class FilterWrapper<E : KIEntity<K>, K : Any>(meta: KIEntityMeta) : EntityFilter<E, K>(meta), IFilterWrapper<E, K> {
        override lateinit var f: EntityFilter<E, K>
        var listener: SendChannel<EntityEvent<E, K>>? = null

        fun digest(ev: EntityEvent<E, K>) {

            listener?.let {
                launch(context) {
                    val send = when (ev) {
                        is EntityCreateEvent -> {
                            val entities = ev.entities.filter { matches(it) }
                            if (entities.isNotEmpty()) EntityCreateEvent(entities) else null
                        }
                        is EntityDeleteEvent -> {
                            val entities = ev.entities.filter { matches(it) }
                            if (entities.isNotEmpty()) EntityDeleteEvent(entities) else null
                        }
                        is EntityUpdatedEvent -> if (wants(ev)) ev else null
                    }
                    logger.debug { "digest \n$ev \nsending $send\nfilter: ${this@FilterWrapper}" }
                    if (send != null) it.send(send)
                }
            }
        }

        override val affectedBy: Set<KIProperty<*>>
            get() = f.affectedBy
        override val affectedByAll: Set<KIProperty<*>>
            get() = f.affectedByAll

        override fun matches(e: E): Boolean = f.matches(e)

        override fun wants(upd: EntityUpdatedEvent<E, K>) = f.wants(upd)

        override fun inverse(): EntityFilter<E, K> = FilterWrapper<E, K>(meta).also { it.f = f.inverse() }

        override fun equals(other: Any?): Boolean = this === other

        override fun hashCode(): Int = System.identityHashCode(this)

        override fun contentEquals(f: EntityFilter<*, *>): Boolean = equals(f)

        override fun toString(): String = "${meta.name}{$f}"
    }

    fun ids(vararg ids: K): StaticEntityFilter<E, K> = StaticEntityFilter(ids.toSet(), meta)
    infix fun <P : Comparable<P>> String.eq(p: P): EQFilter<E, K, P> = meta.props[this]?.let {
        @Suppress("UNCHECKED_CAST")
        EQFilter<E, K, P>(it as KIProperty<P>, meta, p)
    } ?: throw FilterError("property $this not found in ${meta.me}")

    infix fun <P : Comparable<P>> String.neq(value: P): NEQFilter<E, K, P> = (this eq value).inverse()
    infix fun <P : Comparable<P>> String.gt(value: P): GTFilter<E, K, P> = withProp(this, value) { GTFilter(it, meta, value) }
    infix fun <P : Comparable<P>> String.gte(value: P): GTEFilter<E, K, P> = withProp(this, value) { GTEFilter(it, meta, value) }

    fun or(f: Iterable<EntityFilter<E, K>>): OrFilter<E, K> {
        val first = f.first()
        return when (this) {
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


    fun and(f: Iterable<EntityFilter<E, K>>): EntityFilter<E, K> {
        val first = f.first()
        return when (this) {
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
    private fun <R, P> withProp(name: String, value: P, cb: (KIProperty<P>) -> R): R = meta.props[name]?.let { cb(it.cast()) }
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

abstract class IdFilter<E : KIEntity<K>, K : Any>(meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
    override val affectedBy: Set<KIProperty<*>>
        get() = setOf()
    override val affectedByAll: Set<KIProperty<*>>
        get() = affectedBy

    override fun wants(upd: EntityUpdatedEvent<E, K>) = false
}

@Suppress("EqualsOrHashCode")
class StaticEntityFilter<E : KIEntity<K>, K : Any>(private val ids: Set<K>, meta: KIEntityMeta) : IdFilter<E, K>(meta) {
    override fun matches(e: E): Boolean = ids.any { e.id == it }

    inner class Inverse : IdFilter<E, K>(meta) {
        private val origin = this@StaticEntityFilter
        override fun matches(e: E): Boolean = !origin.matches(e)

        override fun inverse(): IdFilter<E, K> = origin
        override fun contentEquals(f: EntityFilter<*, *>): Boolean = if (f is Inverse) ids == f.origin.ids else false
        override fun toString(): String = "not($origin)"
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

sealed class PropertyFilter<E : KIEntity<K>, K : Any, P>(val prop: KIProperty<P>, meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
    override val affectedBy: Set<KIProperty<*>> = setOf(prop)
    override val affectedByAll: Set<KIProperty<*>>
        get() = affectedBy

    override fun wants(upd: EntityUpdatedEvent<E, K>): Boolean = upd.updates.any { it.prop == prop }
}

class PropertyNullFilter<E : KIEntity<K>, K : Any, P>(prop: KIProperty<P>, meta: KIEntityMeta) : PropertyFilter<E, K, P>(prop, meta) {
    override fun matches(e: E): Boolean = (e.getValue(prop)) == null

    override fun inverse(): EntityFilter<E, K> = PropertyNotNullFilter(prop, meta)
    override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is PropertyNullFilter<*, *, *> && f.prop == prop

    @Suppress("UNCHECKED_CAST")
    override fun wants(upd: EntityUpdatedEvent<E, K>): Boolean = upd.history(prop as KIProperty<Any>).any { it == null }
}

class PropertyNotNullFilter<E : KIEntity<K>, K : Any, P>(prop: KIProperty<P>, meta: KIEntityMeta) : PropertyFilter<E, K, P>(prop, meta) {
    override fun matches(e: E): Boolean = e.getValue(prop) != null

    override fun inverse(): EntityFilter<E, K> = PropertyNullFilter(prop, meta)
    override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is PropertyNullFilter<*, *, *> && f.prop == prop
    @Suppress("UNCHECKED_CAST")
    override fun wants(upd: EntityUpdatedEvent<E, K>): Boolean = upd.history(prop as KIProperty<Any>).let {
        it.any { it == null } && it.any { it != null }
    }
}

sealed class PropertyValueFilter<E : KIEntity<K>, K : Any, P : Any>(prop: KIProperty<P>, meta: KIEntityMeta, val value: P) : PropertyFilter<E, K, P>(prop, meta) {
    fun match(p: P?): Boolean = relate(p, value)
    abstract fun relate(value: P?, test: P): Boolean
    override fun matches(e: E): Boolean = match(e.getValue(prop))

    override fun wants(upd: EntityUpdatedEvent<E, K>): Boolean = upd.history(prop).let {
        it.any { relate(it, value) } && it.any { !relate(it, value) }
    }

    /**
     * defines the min/max extend of values for this filter. a null means infinity
     */
    abstract val minmax: Pair<P?, P?>

    override fun contentEquals(f: EntityFilter<*, *>): Boolean = if (this::class == f::class && f is PropertyValueFilter<*, *, *>) {
        prop == f.prop && value == f.value
    } else false

    fun valueToString(): String = when (value) {
        is LocalDate -> """date("${value.format(DateTimeFormatter.ofPattern("yyyyMMdd"))}", "yyyyMMdd")"""
        is LocalDateTime -> """datetime("${value.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"))}", "yyyyMMddHHmmssSSS")"""
        is OffsetDateTime -> """offsetDatetime("${value.format(DateTimeFormatter.ofPattern("yyyyMMdwdHHmmssSSSX"))}", "yyyyMMdwdHHmmssSSSX")"""
        else -> "$value"
    }

    override fun toString(): String = "${prop.name} $op ${valueToString()}"

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
    override fun wants(upd: EntityUpdatedEvent<E, K>): Boolean = operands.any { it.wants(upd) }

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
}

class OrFilter<E : KIEntity<K>, K : Any>(operands: Iterable<EntityFilter<E, K>>, meta: KIEntityMeta) : CombinationFilter<E, K>(operands, meta) {
    override fun matches(e: E): Boolean = operands.any { it.matches(e) }

    override fun inverse(): EntityFilter<E, K> = AndFilter(operands.map { it.inverse() }, meta)

    override val op: String get() = "||"
}
