package info.kinterest

val NULL : Any? = null

interface Keyed<T:Comparable<T>> {
    val id : T;
}

interface DataStore {
    fun<K:Comparable<K>> retrieve(type:String,k:K) : KIEntity<K>?
    fun<E:KIEntity<K>,K:Comparable<K>> create(e:E) : E
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
    fun asTransient() : TransientEntity<T>
}

interface TransientEntity<T:Comparable<T>> : KIEntity<T>
interface EntitySupport<E:KIEntity<K>,K:Comparable<K>> {
    /**
     * creates a new transient entity, requires that all properties are given in their ctor order
     */
    fun transient(id:K?, values : Map<String,Any?>) : TransientEntity<K>
    fun<DS:DataStore> create(ds:DS, id:K,map:Map<String,Any?>) : E
}