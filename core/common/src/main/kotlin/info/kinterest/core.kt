package info.kinterest

import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty


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

interface EntitySupport {
    val meta: KIEntityMeta
}
