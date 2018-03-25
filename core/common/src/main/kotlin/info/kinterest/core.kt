package info.kinterest

import info.kinterest.meta.KIEntityMeta

val NULL : Any? = null

expect interface Klass<T:Any>

expect class UUID

interface Keyed<T:Comparable<T>> {
    val id : T
}

expect interface DataStore {
    val name : String
}

interface DataStoreManager {
    val type:String
    val dataStores : Map<String,DataStore>
    fun add(ds:DataStore)
}

/**
 * this will be a singleton on any platform instance
 */
interface DataStores {
    val types : Map<String,DataStoreManager>
    fun add(type:String, m:DataStoreManager)
}

interface KIEntity<T:Comparable<T>> : Keyed<T> {
    val _store : DataStore
    val _meta : KIEntityMeta<T>
    fun asTransient() : TransientEntity<T>
}

interface TransientEntity<T:Comparable<T>> : KIEntity<T> {
    val values: Map<String,Any?>
}
interface EntitySupport<E:KIEntity<K>,K:Comparable<K>> {
    /**
     * creates a new transient entity, requires that all properties are given in their ctor order
     */
    fun transient(id:K?, values : Map<String,Any?>) : TransientEntity<K>
    fun<DS:DataStore> create(ds:DS, id:K,values:Map<String,Any?>)
}

interface Versioned<V> {
    val _version : V
}