package info.kinterest.jvm

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.bind
import com.github.salomonbrys.kodein.instance
import info.kinterest.*
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.datastores.DataStoreFacade
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.meta.*
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging
import org.jetbrains.annotations.Nullable
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.superclasses


@Suppress("UNCHECKED_CAST", "unused", "PropertyName")
abstract class KIJvmEntity<out E : KIEntity<K>, out K : Any>(override val _store: DataStoreFacade, override val id: K) : KIEntity<K> {
    abstract override val _meta: KIJvmEntityMeta
    abstract val _me: KClass<*>

    override fun equals(other: Any?): Boolean = if(other is KIJvmEntity<*,*>) {
        if(other==this) true else
        if(other._me==_me) {
            other.id == id
        } else false
    } else false

    override fun hashCode(): Int = id.hashCode()

    override fun toString(): String = "${_meta.name}($id)"
    @Suppress("UNCHECKED_CAST")
    override fun <V, P : KIProperty<V>> getValue(prop: P): V? = if (prop == _meta.idProperty) id as V? else
        runBlocking { _store.getValues(_meta, id, prop).await().getOrElse { throw it }?.get(prop.name) } as V?

    override fun <V, P : KIProperty<V>> setValue(prop: P, v: V?) {
        _store.setValues(_meta, id, mapOf(prop to v))
    }

    override fun <V, P : KIProperty<V>> setValue(prop: P, version: Any, v: V?) {
        _store.setValues(_meta, id, version, mapOf(prop to v))
    }
}

abstract class KIJvmEntityMeta(override val impl: Klass<*>, final override val me: Klass<*>) : KIEntityMeta() {
    override val name = me.simpleName!!
    private val propertySupport: MutableMap<String, PropertySupport<*>> = mutableMapOf()

    override val idProperty: KIProperty<*> = me.memberProperties.filter { it.name == "id" }.map { create(it) }.first()

    override val props: Map<String, KIProperty<*>> = me.memberProperties.filter { !it.name.startsWith("_") }.associate { it.name to create(it.cast()) }

    operator fun get(n: String): KIProperty<*>? = props[n]
    inner class PropertySupport<V : Any>(kProperty: KProperty1<*, *>) : KIPropertySupport<V> {
        override val name: String = kProperty.name
        override val type: KClass<*> = kProperty.returnType.classifier as KClass<*>
        override val readOnly: Boolean = kProperty !is KMutableProperty1
        override val nullable: Boolean = kProperty.annotations.any { it is Nullable }
        override val transient: Boolean = kProperty.annotations.any { it is Transient }
        override val comparable: Boolean = Comparable::class in type.superclasses
        override val minmax: Pair<V, V>
            get() = MinMaxUtil.minmax(type).cast()


        @Suppress("UNCHECKED_CAST")
        override fun minMax(v1: V, v2: V): Pair<V, V> = if (comparable) {
            {
                val c1 = v1 as Comparable<Any>
                val c2 = v2 as Comparable<Any>
                if (c1 <= c2) c1 to c2 else c2 to c1
            }.cast()
        } else DONTDOTHIS()

        init {
            propertySupport[name] = this
        }
    }

    fun create(p: KProperty1<*, *>): KIProperty<*> = when (p.returnType.classifier) {
        String::class -> KIStringProperty(PropertySupport<String>(p))
        Boolean::class -> KIBooleanProperty(PropertySupport<Boolean>(p))
        Int::class -> KIIntProperty(PropertySupport<Int>(p))
        Long::class -> KILongProperty(PropertySupport<Long>(p))
        else -> KIUnknownTypeProperty<Any>(PropertySupport<Any>(p))
    }

    private val ctor = findCtor()
    private fun findCtor() = run {
        val ctor = impl.constructors.first()
        logger.trace { "ctor $ctor ${ctor.parameters.size}" }
        assert(ctor.parameters.size == 2)
        ctor
    }

    @Suppress("UNCHECKED_CAST")
    override fun<K:Any> new(ds: DataStore, id: K): KIEntity<K> = ctor.call(ds, id) as KIEntity<K>

    companion object : KLogging() {

    }
}

class MetaProvider() {
    private val metas : MutableMap<String,KIEntityMeta> = mutableMapOf()
    private val metaByClass : MutableMap<KClass<*>,KIEntityMeta> = mutableMapOf()
    fun meta(entity:String) : KIEntityMeta? = metas[entity]
    fun meta(klass:Klass<*>) = metaByClass[klass]
    fun register(meta:KIEntityMeta) {
        metas[meta.name] = meta
        metaByClass[meta.me.cast()] = meta
    }
}

val coreKodein = Kodein.Module {
    bind<MetaProvider>() with instance(MetaProvider())
    bind<Dispatcher<EntityEvent<*,*>>>("entities") with instance(Dispatcher())
    bind<Dispatcher<KIErrorEvent<*>>>("errors") with instance(Dispatcher())
}


internal object MinMaxUtil {
    fun minmax(type: KClass<*>): Pair<Any, Any> = when (type) {
        Byte::class -> Byte.MIN_VALUE to Byte.MAX_VALUE
        Char::class -> java.lang.Character.MIN_VALUE to java.lang.Character.MAX_VALUE
        Char::class -> Char.MIN_SURROGATE to Char.MAX_SURROGATE
        Short::class -> Short.MIN_VALUE to Short.MAX_VALUE
        Int::class -> Int.MIN_VALUE to Int.MAX_VALUE
        Long::class -> Long.MIN_VALUE to Long.MAX_VALUE
        Double::class -> Double.MIN_VALUE to Double.MAX_VALUE
        Float::class -> Float.MIN_VALUE to Float.MAX_VALUE
        String::class -> minmax(Char::class).let { "${it.first}" to "${it.second}" }
        Enum::class -> DONTDOTHIS("have to figure that one out")
        LocalDate::class -> LocalDate.MIN to LocalDate.MAX
        LocalDateTime::class -> LocalDateTime.MIN to LocalDateTime.MAX
        OffsetDateTime::class -> OffsetDateTime.MIN to OffsetDateTime.MAX
        else -> DONTDOTHIS("$type not supported")
    }
}