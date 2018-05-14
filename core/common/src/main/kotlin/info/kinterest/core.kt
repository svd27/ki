package info.kinterest

import info.kinterest.datastores.DataStoreFacade
import info.kinterest.datastores.IEntityTrace
import info.kinterest.filter.FilterWrapper
import info.kinterest.filter.IdFilter
import info.kinterest.functional.getOrDefault
import info.kinterest.functional.getOrElse
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.meta.KIRelationProperty
import info.kinterest.meta.Relation
import info.kinterest.query.CountProjection
import info.kinterest.query.CountProjectionResult
import info.kinterest.query.Query
import kotlin.properties.ReadOnlyProperty
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KClass
import kotlin.reflect.KProperty


expect class UUID

interface Keyed<out T : Any> {
    val id: T
}

open class DataStore(open val name: String) {
    override fun equals(other: Any?): Boolean = if (other === this) true else {
        if (other is DataStore) name == other.name else false
    }

    override fun hashCode(): Int = name.hashCode()
}

interface KITransientEntity<out T : Any> : Keyed<T> {
    @Suppress("PropertyName")
    val _meta: KIEntityMeta

    fun <V : Any?, P : KIProperty<V>> getValue(prop: P): V?
    fun asTransient(): KITransientEntity<T>

}

interface KIEntity<out K : Any> : KITransientEntity<K> {
    @Suppress("PropertyName")
    val _store: DataStore

    fun <V : Any?, P : KIProperty<V>> setValue(prop: P, v: V?)
    fun <V : Any?, P : KIProperty<V>> setValue(prop: P, version: Any, v: V?)

    fun asTrace(): IEntityTrace = object : IEntityTrace {
        override val type: String get() = _meta.name
        override val id: Any get() = this@KIEntity.id
        override val ds: String get() = _store.name
        override fun equals(other: Any?): Boolean = _equals(other)
        override fun hashCode(): Int = type.hashCode() xor id.hashCode() xor ds.hashCode()
    }
}

interface KIVersionedEntity<out K : Any> : KIEntity<K> {
    @Suppress("PropertyName")
    val _version: Any
}

open class RelationSet<S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(val source: S, val store: DataStoreFacade, val rel: KIRelationProperty) : Set<T> {
    override val size: Int
        get() = run {
            val projection = CountProjection<S, K>(rel)
            store.querySync(Query<S, K>(FilterWrapper(IdFilter<S, K>(setOf(source.id), source._meta)), listOf(projection))).getOrElse { throw it }.projections[projection]?.let {
                (it as CountProjectionResult<S, K>).count.toInt()
            } ?: throw Exception("Bad Result")
        }

    override fun contains(element: T): Boolean = iterator().asSequence().any { it == element }

    override fun containsAll(elements: Collection<T>): Boolean = run {
        val content = iterator().asSequence().toSet()
        elements.all { it in content }
    }

    override fun isEmpty(): Boolean = size == 0

    override fun iterator(): Iterator<T> = store.getRelationsSync<S, K, T, L>(rel, source).getOrElse { throw it }.iterator()
}

class RelationMutableSet<S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(source: S, store: DataStoreFacade, rel: KIRelationProperty) : RelationSet<S, K, T, L>(source, store, rel), MutableSet<T> {
    override fun add(element: T): Boolean = store.addRelation(Relation(rel, source, element)).isSuccess

    override fun addAll(elements: Collection<T>): Boolean = elements.all { add(it) }


    override fun clear() {
        iterator().forEach { remove(it) }
    }

    override fun iterator(): MutableIterator<T> = store.getRelationsSync<S, K, T, L>(rel, source).getOrElse { throw it }.toMutableSet().iterator()

    override fun remove(element: T): Boolean = store.removeRelation(Relation(rel, source, element)).isSuccess

    override fun removeAll(elements: Collection<T>): Boolean = elements.all { remove(it) }

    override fun retainAll(elements: Collection<T>): Boolean {
        TODO("not implemented")
    }
}


open class RelationRODelegate<E : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(val store: DataStoreFacade, val rel: KIRelationProperty) : ReadOnlyProperty<E, T> {
    override fun getValue(thisRef: E, property: KProperty<*>): T = store.getRelationsSync<E, K, T, L>(rel, thisRef).getOrDefault { throw it }.first()
}

open class RelationNullableRODelegate<E : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(val store: DataStoreFacade, val rel: KIRelationProperty) : ReadOnlyProperty<E, T?> {
    override fun getValue(thisRef: E, property: KProperty<*>): T? = store.getRelationsSync<E, K, T, L>(rel, thisRef).getOrDefault { throw it }.firstOrNull()
}


open class RelationDelegate<E : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(val store: DataStoreFacade, val rel: KIRelationProperty) : ReadWriteProperty<E, T> {
    override fun getValue(thisRef: E, property: KProperty<*>): T = store.getRelationsSync<E, K, T, L>(rel, thisRef).getOrDefault { throw it }.first()
    override fun setValue(thisRef: E, property: KProperty<*>, value: T) {
        store.replaceRelation(rel, thisRef, value)
    }
}

open class RelationNullableDelegate<E : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(val store: DataStoreFacade, val rel: KIRelationProperty) : ReadWriteProperty<E, T?> {
    override fun getValue(thisRef: E, property: KProperty<*>): T? = store.getRelationsSync<E, K, T, L>(rel, thisRef).getOrDefault { throw it }.firstOrNull()
    override fun setValue(thisRef: E, property: KProperty<*>, value: T?) {
        if (value != null)
            store.replaceRelation(rel, thisRef, value)
        else {
            val old = getValue(thisRef, property)
            if (old != null) store.removeRelation(Relation<E, T, K, L>(rel, thisRef, old))
        }
    }
}

class MetaProvider() {
    private val metas: MutableMap<String, KIEntityMeta> = mutableMapOf()
    private val metaByClass: MutableMap<KClass<*>, KIEntityMeta> = mutableMapOf()
    fun meta(entity: String): KIEntityMeta? = metas[entity]
    fun meta(klass: KClass<*>) = metaByClass[klass]
    fun register(meta: KIEntityMeta) {
        metas[meta.name] = meta
        metaByClass[meta.me.cast()] = meta
    }
}

interface EntitySupport {
    val meta: KIEntityMeta
}
