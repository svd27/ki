package info.kinterest.datastores.jvm.memory

import info.kinterest.DataStore
import info.kinterest.DataStoreManager
import info.kinterest.KIEntity
import info.kinterest.datastores.jvm.DataStores
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

object MemoryDataStoreManager : DataStoreManager {
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

class MemoryDataStore(override val name:String) : DataStore {
    init {
       MemoryDataStoreManager.add(this)
    }

    override fun <E : KIEntity<K>, K : Comparable<K>> retrieve(k: K): E? =
        TODO("not implemented")


    override fun <E : KIEntity<K>, K : Comparable<K>> create(e: E): E =
        TODO("not implemented")
}