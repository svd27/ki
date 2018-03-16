package info.kinterest

interface Keyed<T:Comparable<T>> {
    val id : T;
}

interface DataStore {
    fun<E:KIEntity<K>,K:Comparable<K>> retrieve(k:K) : E?
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
    val _storageType : String
    val _store : DataStore
}