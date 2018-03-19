package info.kinterest.datastores.jvm.memory

import info.kinterest.DataStore
import info.kinterest.DataStoreManager
import info.kinterest.KIEntity
import info.kinterest.datastores.jvm.DataStores
import info.kinterest.jvm.KIJvmEntity
import org.mapdb.DB
import org.mapdb.DBMaker
import java.nio.file.Path
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.reflect.full.memberProperties

abstract class KIJvmMemEntity<T : Comparable<T>>(override val _store: DataStore, override val id: T) : KIJvmEntity<T>()

object JvmMemoryDataStoreManager : DataStoreManager {
    override val type: String = "jvm.memory"

    init {
        DataStores.add(type, this)
    }

    private val rw = ReentrantReadWriteLock()
    override val dataStores: Map<String, DataStore>
        get() = rw.read { _stores.toMap() }

    private val _stores = mutableMapOf<String, DataStore>()

    override fun add(ds: DataStore) = rw.write { _stores[ds.name] = ds }
}

class JvmMemoryDataStore(override val name: String, val dir: Path) : DataStore {
    val db : DB
    init {
        JvmMemoryDataStoreManager.add(this)
        db =
                if(dir!=null)
                    DBMaker.fileDB(dir.resolve("$name.db").toFile()).make()
                else
                    DBMaker.memoryDB().make()

    }

    override fun <K : Comparable<K>> retrieve(type:String,k: K): KIEntity<K>? =
            TODO("not implemented")


    override fun <E : KIEntity<K>, K : Comparable<K>> create(e: E): E =
            TODO("not implemented")
}