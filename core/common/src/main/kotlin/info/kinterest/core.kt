package info.kinterest

import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.getOrDefault
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.meta.KIRelationProperty
import kotlin.properties.ReadOnlyProperty
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty


expect class UUID

interface Keyed<out T:Any> {
    val id: T
}

expect interface DataStore {
    val name: String
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
}

interface KIVersionedEntity<out K : Any> : KIEntity<K> {
    @Suppress("PropertyName")
    val _version: Any
}

open class RelationSet<S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(val source: S, val store: DataStoreFacade, val rel: KIRelationProperty<T, L>) : Set<T> {
    override val size: Int
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override fun contains(element: T): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun containsAll(elements: Collection<T>): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun isEmpty(): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun iterator(): Iterator<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class RelationMutableSet<S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(source: S, store: DataStoreFacade, rel: KIRelationProperty<T, L>) : RelationSet<S, K, T, L>(source, store, rel), MutableSet<T> {
    override fun add(element: T): Boolean = store.addRelation(rel, source, element).isSuccess

    override fun addAll(elements: Collection<T>): Boolean = elements.all { add(it) }


    override fun clear() {
        iterator().forEach { remove(it) }
    }

    override fun iterator(): MutableIterator<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun remove(element: T): Boolean = store.removeRelation(rel, source, element).isSuccess

    override fun removeAll(elements: Collection<T>): Boolean = elements.all { remove(it) }

    override fun retainAll(elements: Collection<T>): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}


open class RelationRODelegate<E : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(val store: DataStoreFacade, val rel: KIRelationProperty<T, L>) : ReadOnlyProperty<E, T> {
    override fun getValue(thisRef: E, property: KProperty<*>): T = store.getRelationsSync(rel, thisRef).getOrDefault { throw it }.first()
}

open class RelationNullableRODelegate<E : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(val store: DataStoreFacade, val rel: KIRelationProperty<T, L>) : ReadOnlyProperty<E, T?> {
    override fun getValue(thisRef: E, property: KProperty<*>): T? = store.getRelationsSync(rel, thisRef).getOrDefault { throw it }.firstOrNull()
}


open class RelationDelegate<E : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(val store: DataStoreFacade, val rel: KIRelationProperty<T, L>) : ReadWriteProperty<E, T> {
    override fun getValue(thisRef: E, property: KProperty<*>): T = store.getRelationsSync(rel, thisRef).getOrDefault { throw it }.first()
    override fun setValue(thisRef: E, property: KProperty<*>, value: T) {
        store.replaceRelation(rel, thisRef, value)
    }
}

open class RelationNullableDelegate<E : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any>(val store: DataStoreFacade, val rel: KIRelationProperty<T, L>) : ReadWriteProperty<E, T?> {
    override fun getValue(thisRef: E, property: KProperty<*>): T? = store.getRelationsSync(rel, thisRef).getOrDefault { throw it }.firstOrNull()
    override fun setValue(thisRef: E, property: KProperty<*>, value: T?) {
        if (value != null)
            store.replaceRelation(rel, thisRef, value)
        else {
            val old = getValue(thisRef, property)
            if (old != null) store.removeRelation(rel, thisRef, old)
        }
    }
}


interface EntitySupport {
    val meta: KIEntityMeta
}
