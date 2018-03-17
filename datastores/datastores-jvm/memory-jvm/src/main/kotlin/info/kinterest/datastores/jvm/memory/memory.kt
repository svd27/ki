package info.kinterest.datastores.jvm.memory

import info.kinterest.DataStore
import info.kinterest.DataStoreManager
import info.kinterest.KIEntity
import info.kinterest.datastores.jvm.DataStores
import info.kinterest.jvm.KIJvmEntity
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

open class KIJvmMemEntity<T:Comparable<T>>(override val _store: DataStore, override val id: T) : KIJvmEntity<T> {
    override val _storageType: String
        get() = "jvm.mem"
}

object JvmMemoryDataStoreManager : DataStoreManager {
    override val type : String = "jvm.memory"
    init {
        DataStores.add(type, this)
    }

    private val rw = ReentrantReadWriteLock()
    override val dataStores: Map<String, DataStore>
        get() = rw.read { _stores.toMap() }

    private val _stores = mutableMapOf<String,DataStore>()

    override fun add(ds: DataStore) = rw.write { _stores[ds.name] = ds }
}

class JvmMemoryDataStore(override val name:String) : DataStore {
    init {
       JvmMemoryDataStoreManager.add(this)
    }

    override fun <E : KIEntity<K>, K : Comparable<K>> retrieve(k: K): E? =
        TODO("not implemented")


    override fun <E : KIEntity<K>, K : Comparable<K>> create(e: E): E =
        TODO("not implemented")
}