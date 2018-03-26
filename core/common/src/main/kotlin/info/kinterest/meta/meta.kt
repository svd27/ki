package info.kinterest.meta

import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.Klass
import info.kinterest.UUID

sealed class KIProperty<V>(support:KIPropertySupport<V>) {
    val name: String = support.name
    val type: Klass<*> = support.type
    val readOnly: Boolean = support.readOnly
    val nullable: Boolean = support.nullable
    val transient: Boolean = support.transient
}
abstract class KIEntityMeta<K : Comparable<K>> {
    abstract val impl: Klass<*>
    abstract val me: Klass<*>
    abstract val name: String
    abstract val root : Klass<*>
    abstract val parent: Klass<*>?
    abstract val props : Map<String, KIProperty<*>>
    abstract fun new(ds: DataStore, id: Any) : KIEntity<K>
    abstract fun<V> get(e:KIEntity<K>,property: KIProperty<V>) : V?
}

interface KIPropertySupport<V> {
    val name: String
    val type: Klass<*>
    val readOnly: Boolean
    val nullable: Boolean
    val transient: Boolean
}

class KIEnumProperty<E>(support: KIPropertySupport<E>) : KIProperty<E>(support)
class KIBooleanProperty(support: KIPropertySupport<Boolean>) : KIProperty<Boolean>(support)
class KIStringProperty(support: KIPropertySupport<String>) : KIProperty<String>(support)
sealed class KINumberProperty<N:Number>(support: KIPropertySupport<N>) : KIProperty<N>(support)
class KIDoubleProperty(support: KIPropertySupport<Double>) : KINumberProperty<Double>(support)
class KIIntProperty(support: KIPropertySupport<Int>) : KINumberProperty<Int>(support)
class KILongProperty(support: KIPropertySupport<Long>) : KINumberProperty<Long>(support)
sealed class KITypeProperty<T>(support: KIPropertySupport<T>) : KIProperty<T>(support)
class KISimpleTypeProperty<T>(support: KIPropertySupport<T>) : KITypeProperty<T>(support)
class KIUUIDProperty(support: KIPropertySupport<UUID>) : KITypeProperty<UUID>(support)
class KIEmbedProperty<T>(support: KIPropertySupport<T>) : KITypeProperty<T>(support)
class KIReferenceProperty<T>(support: KIPropertySupport<T>) : KITypeProperty<T>(support)
