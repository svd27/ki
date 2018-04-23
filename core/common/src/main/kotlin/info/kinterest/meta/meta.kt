package info.kinterest.meta

import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.LocalDate
import info.kinterest.UUID
import kotlin.reflect.KClass

abstract class KIEntityMeta {
    abstract val root: KClass<*>
    abstract val impl: KClass<*>
    abstract val me: KClass<*>
    abstract val parent: KClass<*>?
    abstract val name: String
    abstract val versioned: Boolean

    abstract val idProperty: KIProperty<*>
    abstract val props : Map<String, KIProperty<*>>
    abstract val hierarchy: List<KIEntityMeta>
    abstract val types: List<KIEntityMeta>

    abstract fun <K : Any> new(ds: DataStore, id: K): KIEntity<K>

    override fun equals(other: Any?): Boolean = if (other === this) true else {
        if (other is KIEntityMeta) other.name == name else false
    }

    override fun hashCode(): Int = name.hashCode()
}

sealed class KIProperty<out V>(private val support: KIPropertySupport<V>, val order: Int) {
    val name: String get() = support.name
    val type: KClass<*> get() = support.type
    val readOnly: Boolean get() = support.readOnly
    val nullable: Boolean get() = support.nullable
    val transient: Boolean get() = support.transient
    val comparable: Boolean get() = support.comparable

    val minmax: Pair<V, V> get() = support.minmax

    override fun equals(other: Any?): Boolean = if (other === this) true else {
        if (other is KIProperty<*>) {
            other.name == name && other.type == type && other.readOnly == readOnly && other.nullable == nullable && other.transient == transient
        } else false
    }

    override fun hashCode(): Int = name.hashCode()
}


interface KIPropertySupport<V> {
    val name: String
    val type: KClass<*>
    val readOnly: Boolean
    val nullable: Boolean
    val transient: Boolean
    val comparable: Boolean
    val minmax: Pair<V, V>
    fun minMax(v1: V, v2: V): Pair<V, V>
}

interface KIPropertyRelationSupport<V> : KIPropertySupport<V> {
    val target: KClass<*>
    val targetId: KClass<*>
    val container: KClass<*>?
}

data class Relation<out S : KIEntity<K>, out T : KIEntity<L>, out K : Any, out L : Any>(val rel: KIRelationProperty, val source: S, val target: T)


class KIBooleanProperty(support: KIPropertySupport<Boolean>) : KIProperty<Boolean>(support, 0)
class KIEnumProperty<E : Enum<*>>(support: KIPropertySupport<E>) : KIProperty<E>(support, 1)
sealed class KINumberProperty<N : Number>(support: KIPropertySupport<N>, order: Int) : KIProperty<N>(support, order)
class KIByteProperty(support: KIPropertySupport<Byte>) : KINumberProperty<Byte>(support, 2)
//short
//char
class KIIntProperty(support: KIPropertySupport<Int>) : KINumberProperty<Int>(support, 5)

class KILongProperty(support: KIPropertySupport<Long>) : KINumberProperty<Long>(support, 6)
//float
class KIDoubleProperty(support: KIPropertySupport<Double>) : KINumberProperty<Double>(support, 8)

class KIStringProperty(support: KIPropertySupport<String>) : KIProperty<String>(support, 9)
sealed class KITypeProperty<T>(support: KIPropertySupport<T>, order: Int) : KIProperty<T>(support, order)
class KIEmbedProperty<T>(support: KIPropertySupport<T>) : KITypeProperty<T>(support, Int.MAX_VALUE)
class KIRelationProperty(support: KIPropertyRelationSupport<Any>) : KITypeProperty<Any>(support, 20) {
    val target: KClass<*> = support.target
    val targetId: KClass<*> = support.targetId
    val container: KClass<*>? = support.container

    override fun toString(): String = "${this::class.simpleName}($name, $type, $target, $container)"
}
class KIReferenceProperty<T>(support: KIPropertySupport<T>) : KITypeProperty<T>(support, Int.MAX_VALUE - 1)
sealed class KISimpleTypeProperty<T>(support: KIPropertySupport<T>, order: Int) : KITypeProperty<T>(support, order)
class KIUUIDProperty(support: KIPropertySupport<UUID>) : KISimpleTypeProperty<UUID>(support, 12)
sealed class KIDateOrTimePropertyclass<T>(support: KIPropertySupport<T>, order: Int) : KISimpleTypeProperty<T>(support, order)
class KILocalDateProperty(support: KIPropertySupport<LocalDate>) : KISimpleTypeProperty<LocalDate>(support, 10)
class KIUnknownTypeProperty<T>(support: KIPropertySupport<T>) : KISimpleTypeProperty<T>(support, Int.MAX_VALUE)