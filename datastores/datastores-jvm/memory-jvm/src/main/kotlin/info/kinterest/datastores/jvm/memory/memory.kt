package info.kinterest.datastores.jvm.memory

import info.kinterest.DataStore
import info.kinterest.DataStoreManager
import info.kinterest.KIEntity
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactory
import info.kinterest.datastores.jvm.DataStores
import info.kinterest.datastores.jvm.JvmEntityMeta
import info.kinterest.jvm.KIJvmEntity
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import org.mapdb.DB
import org.mapdb.DBMaker
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties
import kotlinx.coroutines.experimental.channels.*

abstract class KIJvmMemEntity<T : Comparable<T>>(override val _store: DataStore, override val id: T) : KIJvmEntity<T>()

class JvmMemoryDataStoreFactory : DataStoreFactory {
    override fun create(cfg: DataStoreConfig): DataStore {
        TODO("not implemented")
    }
}

class JvmMemCfg(cfg:DataStoreConfig) : DataStoreConfig by cfg {
    val dirStr : String?  by cfg.config
    val dir = dirStr?.let{Paths.get(it)}
}

class JvmMemoryDataStore(cfg:JvmMemCfg) : DataStore {
    override val name: String = cfg.name
    val dir = cfg.dir
    private val metas : MutableMap<KClass<*>,JvmEntityMeta<*,*>> = mutableMapOf()

    val db : DB
    init {
        db =
                if(dir!=null)
                    DBMaker.fileDB(dir.resolve("$name.db").toFile()).make()
                else
                    DBMaker.memoryDB().make()

    }

    override fun <K : Comparable<K>> get(id: K): KIEntity<K> = TODO("not implemented")
    inline fun<reified E:KIEntity<K>,K:Comparable<K>> getVersion(id:K) : Long = 0

    inner class Buckets(val map:MutableMap<KClass<*>,Bucket>) : Map<KClass<*>,Bucket> by map {
        operator fun get(e:KIJvmEntity<*>) = if(e._me in this) this[e._me] else {
            map[e._me] = this@JvmMemoryDataStore.Bucket(e)
            this[e._me]
        }
    }
    inner class Bucket(e:KIJvmEntity<*>) {
        val bucket = db.hashMap(e._me.simpleName!!).createOrOpen()

        operator fun get(k:Any) : MutableMap<String,Any?>? = bucket[k] as MutableMap<String, Any?>?

        operator fun set(k:Any, prop:String, value: Any?) = db.apply {
            val e = this@Bucket[k]!!
            e[prop] = value
            commit()
        }
    }
    val buckets = Buckets(mutableMapOf())

    fun set(e:KIJvmEntity<*>, prop:String, value:Any?) = buckets[e._me].apply {
        async(CommonPool) {

        }
    }
}