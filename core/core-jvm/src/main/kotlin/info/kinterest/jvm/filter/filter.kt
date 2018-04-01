package info.kinterest.jvm.filter

import info.kinterest.*
import info.kinterest.jvm.MetaProvider
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.launch
import mu.KLogging
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

sealed class KIFilter<T> {
    abstract fun matches(e: T): Boolean
}

inline fun <reified E : KIEntity<K>, K : Any> filter(ds: DataStore, provider: MetaProvider, crossinline cb: EntityFilter<E, K>.() -> EntityFilter<E, K>): FilterWrapper<E, K> = run {
    val meta = provider.meta(E::class) as KIEntityMeta
    filter(ds, meta, cb)
}

inline fun <E : KIEntity<K>, K : Any> filter(ds: DataStore, meta: KIEntityMeta, crossinline cb: EntityFilter<E, K>.() -> EntityFilter<E, K>): FilterWrapper<E, K> = EntityFilter.Empty<E, K>(meta).run {
    EntityFilter.FilterWrapper<E, K>(ds, meta).apply {
        val filter = this.cb()
        f = (filter as? FilterWrapper)?.f ?: filter
    }
}

typealias FilterWrapper<E, K> = EntityFilter.FilterWrapper<E, K>

@Suppress("EqualsOrHashCode")
sealed class EntityFilter<E : KIEntity<K>, K : Any>(val meta: KIEntityMeta) : KIFilter<E>() {
    abstract val parent: EntityFilter<E, K>
    open val ds: DataStore
        get() = parent.ds
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

    abstract fun wants(upd: EntityUpdatedEvent<E, K>): Boolean

    class Empty<E : KIEntity<K>, K : Any>(meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
        override lateinit var parent: EntityFilter<E, K>
        override val affectedBy: Set<KIProperty<*>>
            get() = setOf()
        override val affectedByAll: Set<KIProperty<*>>
            get() = affectedBy

        override fun matches(e: E): Boolean = DONTDOTHIS()

        override fun inverse(): EntityFilter<E, K> = DONTDOTHIS()
        override fun contentEquals(f: EntityFilter<*, *>): Boolean = DONTDOTHIS()
        override fun wants(upd: EntityUpdatedEvent<E, K>) = DONTDOTHIS()
    }

    class FilterWrapper<E : KIEntity<K>, K : Any>(override val ds: DataStore, meta: KIEntityMeta) : EntityFilter<E, K>(meta) {
        lateinit var f: EntityFilter<E, K>
        var listener: SendChannel<EntityEvent<E, K>>? = null

        fun digest(ev: EntityEvent<E, K>) {
            listener?.let {
                launch {
                    val send = when (ev) {
                        is EntityCreateEvent, is EntityDeleteEvent -> matches(ev.entity)
                        is EntityUpdatedEvent -> wants(ev)
                    }
                    if (send) it.send(ev)
                    logger.debug { "digest \n$ev \nsending $send\nfilter: ${this@FilterWrapper}" }
                }
            }
        }

        override val parent: EntityFilter<E, K>
            get() = TODO("not implemented")

        override val affectedBy: Set<KIProperty<*>>
            get() = f.affectedBy
        override val affectedByAll: Set<KIProperty<*>>
            get() = f.affectedByAll

        override fun matches(e: E): Boolean = f.matches(e)

        override fun wants(upd: EntityUpdatedEvent<E, K>) = f.wants(upd)

        override fun inverse(): EntityFilter<E, K> = FilterWrapper<E, K>(ds, meta).also { it.f = f.inverse() }

        override fun equals(other: Any?): Boolean = this === other

        override fun hashCode(): Int = System.identityHashCode(this)

        override fun contentEquals(f: EntityFilter<*, *>): Boolean = equals(f)

        override fun toString(): String = "${meta.name}{$f}"
    }

    fun ids(vararg ids: K): StaticEntityFilter<E, K> = StaticEntityFilter(ids.toSet(), meta, this)
    infix fun <P : Comparable<P>> String.eq(p: P): EQFilter<E, K, P> = meta.props[this]?.let {
        EQFilter(it.cast(), meta, this@EntityFilter, p)
    } ?: throw FilterError("property $this not found in ${meta.me}")

    infix fun <P : Comparable<P>> String.neq(value: P): NEQFilter<E, K, P> = (this eq value).inverse()
    infix fun <P : Comparable<P>> String.gt(value: P): GTFilter<E, K, P> = withProp(this, value) { GTFilter(it, meta, this@EntityFilter, value) }
    infix fun <P : Comparable<P>> String.gte(value: P): GTEFilter<E, K, P> = withProp(this, value) { GTEFilter(it, meta, this@EntityFilter, value) }

    fun or(f: Iterable<EntityFilter<E, K>>): OrFilter<E, K> {
        val first = f.first()
        return when (this) {
            is CombinationFilter -> when (this) {
                is OrFilter -> {
                    @Suppress("UNCHECKED_CAST")
                    when (first) {
                        is OrFilter<*, *> ->
                            OrFilter(this.operands + first.operands as Iterable<EntityFilter<E, K>>,
                                    meta, parent).or(f.drop(1))
                        else -> OrFilter(this.operands + f, meta, parent)
                    }
                }
                else -> OrFilter(listOf(this) + f, meta, parent)
            }
            else -> when (first) {
                is OrFilter<*, *> -> (first as OrFilter<E, K>).or(f.drop(1) + this)
                else -> OrFilter(listOf(this) + f, meta, parent)
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
                                    meta, parent).and(f.drop(1))
                        else -> AndFilter(this.operands + f, meta, parent)
                    }
                }
                else -> AndFilter(listOf(this) + f, meta, parent)
            }
            else -> when (first) {
                is AndFilter<*, *> -> (first as AndFilter<E, K>).and(f.drop(1) + this)
                else -> AndFilter(listOf(this) + f, meta, parent)
            }
        }
    }

    @Suppress("UNUSED_PARAMETER")
    private fun <R, P> withProp(name: String, value: P, cb: (KIProperty<P>) -> R) = meta.props[name]?.let { cb(it.cast()) }
            ?: throw FilterError("property $this not found in ${meta.me}")

    abstract fun inverse(): EntityFilter<E, K>

    override fun equals(other: Any?): Boolean = if (other === this) true else {
        if (other is EntityFilter<*, *>) {
            if (other.meta == meta && other.ds == ds) contentEquals(other)
            else false
        } else false
    }

    protected abstract fun contentEquals(f: EntityFilter<*, *>): Boolean

    companion object : KLogging()
}

abstract class IdFilter<E : KIEntity<K>, K : Any>(meta: KIEntityMeta, override val parent: EntityFilter<E, K>) : EntityFilter<E, K>(meta) {
    override val affectedBy: Set<KIProperty<*>>
        get() = setOf()
    override val affectedByAll: Set<KIProperty<*>>
        get() = affectedBy

    override fun wants(upd: EntityUpdatedEvent<E, K>) = false
}

@Suppress("EqualsOrHashCode")
class StaticEntityFilter<E : KIEntity<K>, K : Any>(private val ids: Set<K>, meta: KIEntityMeta, parent: EntityFilter<E, K>) : IdFilter<E, K>(meta, parent) {
    override fun matches(e: E): Boolean = ids.any { e.id == it }

    inner class Inverse : IdFilter<E, K>(meta, parent) {
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

sealed class IdValueFilter<E : KIEntity<K>, K : Any>(meta: KIEntityMeta, parent: EntityFilter<E, K>) : IdFilter<E, K>(meta, parent)
class IdComparisonFilter<E : KIEntity<K>, K : Any>(meta: KIEntityMeta, parent: EntityFilter<E, K>, val f: PropertyValueFilter<E, K, K>) : IdValueFilter<E, K>(meta, parent) {
    override fun matches(e: E): Boolean = f.matches(e)

    @Suppress("UNCHECKED_CAST")
    override fun inverse(): EntityFilter<E, K> = IdComparisonFilter(meta, parent, f.inverse() as PropertyValueFilter<E, K, K>)

    override fun contentEquals(f: EntityFilter<*, *>): Boolean = DONTDOTHIS("equals overridden")
    override fun equals(other: Any?): Boolean = if (other is IdComparisonFilter<*, *>) f == other else false
    override fun hashCode(): Int = f.hashCode()
    override fun toString(): String = f.toString()
}

sealed class PropertyFilter<E : KIEntity<K>, K : Any, P>(val prop: KIProperty<P>, meta: KIEntityMeta, override val parent: EntityFilter<E, K>) : EntityFilter<E, K>(meta) {
    override val affectedBy: Set<KIProperty<*>> = setOf(prop)
    override val affectedByAll: Set<KIProperty<*>>
        get() = affectedBy

    override fun wants(upd: EntityUpdatedEvent<E, K>): Boolean = upd.updates.any { it.prop == prop }
}

class PropertyNullFilter<E : KIEntity<K>, K : Any, P>(prop: KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>) : PropertyFilter<E, K, P>(prop, meta, parent) {
    override fun matches(e: E): Boolean = (e.getValue(prop)) == null

    override fun inverse(): EntityFilter<E, K> = PropertyNotNullFilter(prop, meta, parent)
    override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is PropertyNullFilter<*, *, *> && f.prop == prop

    @Suppress("UNCHECKED_CAST")
    override fun wants(upd: EntityUpdatedEvent<E, K>): Boolean = upd.history(prop as KIProperty<Any>).any { it == null }
}

class PropertyNotNullFilter<E : KIEntity<K>, K : Any, P>(prop: KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>) : PropertyFilter<E, K, P>(prop, meta, parent) {
    override fun matches(e: E): Boolean = e.getValue(prop) != null

    override fun inverse(): EntityFilter<E, K> = PropertyNullFilter(prop, meta, parent)
    override fun contentEquals(f: EntityFilter<*, *>): Boolean = f is PropertyNullFilter<*, *, *> && f.prop == prop
    @Suppress("UNCHECKED_CAST")
    override fun wants(upd: EntityUpdatedEvent<E, K>): Boolean = upd.history(prop as KIProperty<Any>).let {
        it.any { it == null } && it.any { it != null }
    }
}

sealed class PropertyValueFilter<E : KIEntity<K>, K : Any, P : Any>(prop: KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, val value: P) : PropertyFilter<E, K, P>(prop, meta, parent) {
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

class EQFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value: P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value == test
    override fun inverse(): NEQFilter<E, K, P> = NEQFilter(prop, meta, parent, value)
    override val minmax: Pair<P?, P?> = value to value
    override val op: String
        get() = "="

}

class NEQFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value: P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value != test
    override fun inverse(): EQFilter<E, K, P> = EQFilter(prop, meta, parent, value)
    override val minmax: Pair<P?, P?> = null to null
    override val op: String
        get() = "!="
}

class GTFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value: P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v -> v > test } ?: false
    override fun inverse(): LTEFilter<E, K, P> = LTEFilter(prop, meta, parent, value)
    override val minmax: Pair<P?, P?> = value to null
    override val op: String
        get() = ">"
}

class GTEFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value: P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v -> v >= test } ?: false
    override fun inverse(): LTFilter<E, K, P> = LTFilter(prop, meta, parent, value)
    override val minmax: Pair<P?, P?> = value to null
    override val op: String get() = ">="
}

class LTFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value: P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->
        v < test
    } ?: true

    override fun inverse(): GTEFilter<E, K, P> = GTEFilter(prop, meta, parent, value)
    override val minmax: Pair<P?, P?> = null to value
    override val op: String get() = "<"
}

class LTEFilter<E : KIEntity<K>, K : Any, P : Comparable<P>>(prop: KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value: P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->
        v <= test
    } ?: true

    override fun inverse(): GTFilter<E, K, P> = GTFilter(prop, meta, parent, value)
    override val minmax: Pair<P?, P?> = null to value
    override val op: String get() = "<="
}

sealed class CombinationFilter<E : KIEntity<K>, K : Any>(val operands: Iterable<EntityFilter<E, K>>, meta: KIEntityMeta, override val parent: EntityFilter<E, K>) : EntityFilter<E, K>(meta) {
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

class AndFilter<E : KIEntity<K>, K : Any>(operands: Iterable<EntityFilter<E, K>>, meta: KIEntityMeta, parent: EntityFilter<E, K>) : CombinationFilter<E, K>(operands, meta, parent) {
    override fun matches(e: E): Boolean = operands.all { it.matches(e) }

    override fun inverse(): EntityFilter<E, K> = OrFilter(operands.map(EntityFilter<E, K>::inverse), meta, parent)

    override val op: String get() = "&&"
}

class OrFilter<E : KIEntity<K>, K : Any>(operands: Iterable<EntityFilter<E, K>>, meta: KIEntityMeta, parent: EntityFilter<E, K>) : CombinationFilter<E, K>(operands, meta, parent) {
    override fun matches(e: E): Boolean = operands.any { it.matches(e) }

    override fun inverse(): EntityFilter<E, K> = AndFilter(operands.map { it.inverse() }, meta, parent)

    override val op: String get() = "||"
}
