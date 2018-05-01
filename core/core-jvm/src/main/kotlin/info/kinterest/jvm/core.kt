package info.kinterest.jvm

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.bind
import com.github.salomonbrys.kodein.instance
import com.github.salomonbrys.kodein.singleton
import info.kinterest.*
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.datastores.IRelationTrace
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.annotations.Relation
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.jvm.filter.tree.FilterTree
import info.kinterest.jvm.query.QueryManagerJvm
import info.kinterest.meta.*
import info.kinterest.query.QueryManager
import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
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

    override fun equals(other: Any?): Boolean = if (other is KIJvmEntity<*, *>) {
        if (other === this) true else
            if (other._me == _me) {
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

fun <E : KIEntity<K>, K : Any, S : KIEntity<L>, L : Any> KIJvmEntity<E, K>.addIncomingRelation(rel: info.kinterest.meta.Relation<S, E, L, K>): Try<Boolean> = _store.bookRelationSync(rel)
fun <E : KIEntity<K>, K : Any, S : KIEntity<L>, L : Any> KIJvmEntity<E, K>.removeIncomingRelation(rel: info.kinterest.meta.Relation<S, E, L, K>): Try<Boolean> = _store.unbookRelationSync(rel)
fun KIJvmEntity<KIEntity<Any>, Any>.getIncomingRelations(rel: KIRelationProperty, sourceMeta: KIEntityMeta): Try<Iterable<IRelationTrace>> = _store.getBookedRelationsSync(rel, this, sourceMeta)

abstract class KIJvmEntityMeta(override val impl: KClass<*>, final override val me: KClass<*>) : KIEntityMeta() {
    override val name = me.simpleName!!
    private val propertySupport: MutableMap<String, PropertySupport<*>> = mutableMapOf()

    override val idProperty: KIProperty<Any> = me.memberProperties.filter { it.name == "id" }.map { create(it) }.first()

    override val props: Map<String, KIProperty<Any>> = me.memberProperties.filter { !it.name.startsWith("_") }.associate { it.name to create(it.cast()) }
    override val types: List<KIEntityMeta> by lazy { listOf(this) + hierarchy }

    operator fun get(n: String): KIProperty<*>? = props[n]
    inner open class PropertySupport<V : Any>(kProperty: KProperty1<*, *>) : KIPropertySupport<V> {
        override final val name: String = kProperty.name
        override final val type: KClass<*> = kProperty.returnType.classifier as KClass<*>
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

    inner class RelationPropertySupport(property: KProperty1<*, *>) : PropertySupport<Any>(property), KIPropertyRelationSupport<Any> {
        val rel = property.getter.annotations.filterIsInstance<Relation>().first()
        override val target: KClass<*>
            get() = rel.target
        override val targetId: KClass<*>
            get() = rel.targetId
        override val container: KClass<*>? = property.returnType.classifier?.let {
            if (it is KClass<*>) {
                if (it == Set::class || it == MutableSet::class || it == List::class || it == MutableList::class) it else null
            } else null
        }
    }

    fun create(p: KProperty1<*, *>): KIProperty<*> = if (p.getter.annotations.filterIsInstance<Relation>().isNotEmpty()) {
        KIRelationProperty(RelationPropertySupport(p))
    } else
        when (p.returnType.classifier) {
            String::class -> KIStringProperty(PropertySupport<String>(p))
            Boolean::class -> KIBooleanProperty(PropertySupport<Boolean>(p))
            Int::class -> KIIntProperty(PropertySupport<Int>(p))
            Long::class -> KILongProperty(PropertySupport<Long>(p))
            LocalDate::class -> KILocalDateProperty(PropertySupport(p))
            else -> KIUnknownTypeProperty<Any>(PropertySupport<Any>(p))
        }

    private val ctor = findCtor()
    private fun findCtor() = run {
        val ctor = impl.constructors.first { it.parameters.size == 2 }
        logger.trace { "ctor $ctor ${ctor.parameters.size}" }
        assert(ctor.parameters.size == 2)
        ctor
    }

    @Suppress("UNCHECKED_CAST")
    override fun <K : Any> new(ds: DataStore, id: K): KIEntity<K> = ctor.call(ds, id) as KIEntity<K>

    companion object : KLogging() {

    }
}


val coreKodein = Kodein.Module {
    bind<MetaProvider>() with instance(MetaProvider())
    bind<Dispatcher<EntityEvent<*, *>>>("entities") with instance(Dispatcher())
    bind<FilterTree>() with singleton { FilterTree(instance("entities"), 100) }
    bind<QueryManagerJvm>() with singleton { QueryManagerJvm(instance(), instance()) }
    bind<QueryManager>() with singleton { instance<QueryManagerJvm>() }
    bind<Channel<DataStoreEvent>>() with singleton { instance<QueryManagerJvm>().dataStores }
    bind<CoroutineDispatcher>("datastores") with singleton { newFixedThreadPoolContext(8, "ds") }
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