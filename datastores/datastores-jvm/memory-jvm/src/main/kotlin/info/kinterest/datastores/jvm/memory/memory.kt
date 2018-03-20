package info.kinterest.datastores.jvm.memory

import info.kinterest.cast
import info.kinterest.Class
import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.Versioned
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactory
import info.kinterest.jvm.KIJvmEntity
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import org.mapdb.DB
import org.mapdb.DBMaker
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.reflect.KClass

abstract class KIJvmMemEntity<T : Comparable<T>>(override val _store: DataStore, override val id: T) : KIJvmEntity<T>()

class JvmMemoryDataStoreFactory : DataStoreFactory {
    override fun create(cfg: DataStoreConfig): DataStore = JvmMemoryDataStore(JvmMemCfg(cfg))
}

class JvmMemCfg(cfg:DataStoreConfig) : DataStoreConfig by cfg {
    private val dirStr : String?  = cfg.config["dir"]?.toString()
    val dir: Path? = dirStr?.let{Paths.get(it)}
}

class JvmMemoryDataStore(cfg:JvmMemCfg) : DataStore {
    override val name: String = cfg.name
    val dir = cfg.dir

    val db : DB
    init {
        db =
                if(dir!=null)
                    DBMaker.fileDB(dir.resolve("$name.db").toFile()).make()
                else
                    DBMaker.memoryDB().make()

    }

    override operator fun <K : Comparable<K>> get(type: Class<*>, id: K): KIEntity<K>? {
        TODO("not implemented")
    }


    inline fun<reified E:KIEntity<K>,K:Comparable<K>> getVersion(id:K) : Long? = this[E::class,id]?.cast<Versioned<Long>>()?._version

    inner class Buckets(val map:MutableMap<KClass<*>,Bucket>) : Map<KClass<*>,Bucket> by map {
        operator fun get(e:KIJvmEntity<*>) = if(e._me in this) this[e._me] else {
            map[e._me] = this@JvmMemoryDataStore.Bucket(e)
            this[e._me]
        }
    }
    inner class Bucket(e:KIJvmEntity<*>) {
        val versioned = e is Versioned<*>
        val bucket = db.hashMap(e._me.simpleName!!).createOrOpen()

        operator fun get(k:Any) : MutableMap<String,Any?>? = bucket[k]?.cast<MutableMap<String, Any?>>()

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