package info.kinterest.jvm

import info.kinterest.meta.*
import info.kinterest.DataStore
import info.kinterest.EntitySupport
import info.kinterest.KIEntity
import info.kinterest.cast
import mu.KLogging
import org.jetbrains.annotations.Nullable
import kotlin.reflect.*
import kotlin.reflect.full.memberProperties
import info.kinterest.Klass
import info.kinterest.jvm.filter.KIFilter


sealed class KIError(msg: String, cause: Throwable?, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
        Exception(msg, cause, enableSuppression, writeableStackTrace)

class FilterError(msg: String, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
        KIError(msg, cause, enableSuppression, writeableStackTrace)

sealed class DataStoreError(val ds: DataStore, msg: String, cause: Throwable?, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
        KIError(msg, cause, enableSuppression, writeableStackTrace) {
    class EntityNotFound(val kc: KClass<*>, val key: Comparable<*>, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
            DataStoreError(ds, "Entity ${kc.simpleName} with Key $key not found in DataStore ${ds.name}", cause, enableSuppression, writeableStackTrace)

    class EntityExists(val kc: KClass<*>, val key: Comparable<*>, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
            DataStoreError(ds, "Entity ${kc.simpleName} with Key $key already exists in DataStore ${ds.name}", cause, enableSuppression, writeableStackTrace)

    class MetaDataNotFound(val kc: KClass<*>, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
            DataStoreError(ds, "Metadata for Entity ${kc.qualifiedName} not found", cause, enableSuppression, writeableStackTrace)

    class VersionNotFound(val kc: KClass<*>, val key: Comparable<*>, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
            DataStoreError(ds, "version for Entity ${kc.simpleName} with id $key not found", cause, enableSuppression, writeableStackTrace)

    class VersionAlreadyExists(val kc: KClass<*>, val key: Comparable<*>, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
            DataStoreError(ds, "version for Entity ${kc.simpleName} with id $key not found", cause, enableSuppression, writeableStackTrace)

    class OptimisticLockException(val kc: KClass<*>, val key: Comparable<*>, val expectedVersion: Any, val actualVersion: Any, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
            DataStoreError(ds, "wrong version for ${kc.simpleName} with id $key, expected: $expectedVersion, actual: $actualVersion", cause, enableSuppression, writeableStackTrace)
    class BatchError(msg:String, val kc:KClass<*>,ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) : DataStoreError(ds, msg, cause, enableSuppression, writeableStackTrace)
}

@Suppress("UNCHECKED_CAST")
abstract class KIJvmEntity<E : KIEntity<K>, K : Comparable<K>> : KIEntity<K> {
    abstract override val _meta: KIJvmEntityMeta<E, K>
    abstract val _me: KClass<*>

    override fun equals(other: Any?): Boolean = if(other is KIJvmEntity<*,*>) {
        if(other._me==_me) {
            other.id == id
        } else false
    } else false

    override fun hashCode(): Int = id.hashCode()

    override fun toString(): String = "${_meta.name}($id)"
}

interface KIJvmEntitySupport<E : KIEntity<K>, K : Comparable<K>> : EntitySupport<E, K> {
    val meta: KIJvmEntityMeta<E, K>
}


abstract class KIJvmEntityMeta<E : KIEntity<K>, K : Comparable<K>>(override val impl: Klass<*>, override val me: Klass<*>) : KIEntityMeta<K>() {
    override val name = me.simpleName!!

    private val propertySupport: MutableMap<String, PropertySupport<*>> = mutableMapOf()
    override val props: Map<String, KIProperty<*>> = me.memberProperties.filter { !it.name.startsWith("_") }.associate { it.name to create(it.cast()) }


    operator fun get(n: String): KIProperty<*>? = props[n]
    inner class PropertySupport<V : Any>(val kProperty: KProperty1<*, *>) : KIPropertySupport<V> {
        val getter = kProperty.getter
        @Suppress("UNCHECKED_CAST")
        fun get(e: KIEntity<K>): V? = getter.call(e) as V?

        override val name: String = kProperty.name
        override val type: Klass<*> = kProperty.returnType.classifier!! as Klass<*>
        override val readOnly: Boolean = kProperty !is KMutableProperty1
        override val nullable: Boolean = kProperty.annotations.any { it is Nullable }
        override val transient: Boolean = kProperty.annotations.any { it is Transient }

        init {
            propertySupport[name] = this
        }
    }

    fun create(p: KProperty1<*, *>): KIProperty<*> = when (p.returnType.classifier) {
        String::class -> KIStringProperty(PropertySupport<String>(p))
        Boolean::class -> KIBooleanProperty(PropertySupport<Boolean>(p))
        Int::class -> KIIntProperty(PropertySupport<Int>(p))
        Long::class -> KILongProperty(PropertySupport<Long>(p))
        else -> TODO("${p.name} ${p.returnType.classifier}")
    }

    override fun <V> get(e: KIEntity<K>, property: KIProperty<V>): V? = propertySupport[property.name]?.get(e) as V?

    private val ctor = findCtor()
    private fun findCtor() = run {
        val ctor = impl.constructors.first()
        logger.trace { "ctor $ctor ${ctor.parameters.size}" }
        assert(ctor.parameters.size == 2)
        ctor
    }

    override fun new(ds: DataStore, id: Any): KIEntity<K> = ctor.call(ds, id).cast()

    companion object : KLogging() {

    }
}

class MetaProvider() {
    private val metas : MutableMap<String,KIEntityMeta<*>> = mutableMapOf()
    fun meta(entity:String) : KIEntityMeta<*>? = metas[entity]
    fun register(meta:KIEntityMeta<*>) { metas[meta.name] = meta }
}

