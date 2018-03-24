package info.kinterest.datastores.jvm

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.bind
import com.github.salomonbrys.kodein.instance
import info.kinterest.DataStore
import info.kinterest.DataStoreManager
import info.kinterest.Try
import kotlinx.coroutines.experimental.Deferred
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.reflect.KClass


class DataStoreConfigManager(kodein: Kodein) {
    val cfgs : List<DataStoreConfig> = kodein.instance("datastore-configs")
    val dataSores : DataStores = kodein.instance()
    init {
        @Suppress("UNUSED_VARIABLE")
        val subKodein = Kodein {
            cfgs.forEach {
                bind<DataStoreConfig>("cfg.${it.type}.${it.name}") to it
            }

        }

    }
}

interface DataStoreFactory {
    fun create(cfg: DataStoreConfig) : DataStore
}

class DataStoreFactoryProvider() {
    val factories = mutableMapOf<String,DataStoreFactory>()
    init {
        this.javaClass.classLoader.getResources("datasource-factory.properties").iterator().forEach {
            val props = Properties()
            it.openStream().use {
                props.load(it)
            }
            props.forEach { n, v ->
                factories[n.toString()] = (Class.forName(v.toString()).newInstance() as DataStoreFactory)
            }
        }
    }
}

class DataStores : info.kinterest.DataStores {
    private val rw  = ReentrantReadWriteLock()
    override val types: Map<String, DataStoreManager>
      get() = rw.read { _types.toMap() }
    private val _types : MutableMap<String,DataStoreManager> = mutableMapOf()
    override fun add(type: String, m: DataStoreManager) = rw.write {
        _types[type] = m
    }
}

/*
class JvmEntityMeta<E:KIEntity<K>, K:Comparable<K>>(val klass:KClass<E>, val impl : KClass<KIEntity<K>>) : KIJvmEntityMeta<E>() {
    val name : String = klass.simpleName!!
    val root : KClass<KIEntity<K>> by lazy {
        @Suppress("UNCHECKED_CAST")
        klass as KClass<KIEntity<K>>
    }

    private fun findRoot() : KClass<KIEntity<K>> = kotlin.run {
        fun findSuper(k:KClass<KIEntity<K>>) : KClass<KIEntity<K>>? = k.superclasses.firstOrNull{it.isSubclassOf(KIEntity::class)} as? KClass<KIEntity<K>>
        var ck = klass as KClass<KIEntity<K>>
        var k = ck
        while (ck!=null) {ck = findRoot(); if(ck!=null) k =ck}
        k
    }

}
*/

abstract class DataStoreJvm(override val name : String) : DataStore {
    abstract fun<K:Comparable<K>> create(type:KClass<*>,id:K, values: Map<String, Any?>) : Try<Deferred<Try<K>>>
    abstract fun<K:Comparable<K>> create(type:KClass<*>, values: Map<String, Any?>) : Try<Deferred<Try<K>>>
}