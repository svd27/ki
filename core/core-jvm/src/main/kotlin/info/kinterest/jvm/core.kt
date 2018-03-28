package info.kinterest.jvm

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.bind
import com.github.salomonbrys.kodein.instance
import com.github.salomonbrys.kodein.singleton
import info.kinterest.*
import info.kinterest.meta.*
import mu.KLogging
import org.jetbrains.annotations.Nullable
import kotlin.reflect.*
import kotlin.reflect.full.memberProperties
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.jvm.filter.KIFilter



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
        else -> KISimpleTypeProperty<Any>(PropertySupport<Any>(p))
    }

    fun <V> get(e: KIEntity<K>, property: KIProperty<V>): V? = propertySupport[property.name]?.get(e) as V?

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
    private val metaByClass : MutableMap<KClass<*>,KIEntityMeta<*>> = mutableMapOf()
    fun meta(entity:String) : KIEntityMeta<*>? = metas[entity]
    fun meta(klass:Klass<*>) = metaByClass[klass]
    fun register(meta:KIEntityMeta<*>) {
        metas[meta.name] = meta
        metaByClass[meta.me] = meta
    }
}

val coreKodein = Kodein.Module {
    bind<MetaProvider>() with singleton { MetaProvider() }
    bind<Dispatcher<EntityEvent<*,*>>>("entities") with singleton { Dispatcher<EntityEvent<*,*>>() }
    bind<Dispatcher<KIErrorEvent<*>>>("errors") with singleton { Dispatcher<KIErrorEvent<*>>() }
}
