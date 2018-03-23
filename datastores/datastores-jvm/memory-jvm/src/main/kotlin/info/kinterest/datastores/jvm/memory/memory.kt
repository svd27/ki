package info.kinterest.datastores.jvm.memory

import arrow.data.Try
import info.kinterest.*
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactory
import info.kinterest.datastores.jvm.DataStoreJvm
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.KIJvmEntitySupport
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging
import org.mapdb.Atomic
import org.mapdb.DB
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongFieldUpdater
import kotlin.reflect.KClass
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.companionObjectInstance
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.javaMethod

abstract class KIJvmMemEntity<E:KIEntity<T>,T : Comparable<T>>(override val _store: DataStore, override val id: T) : KIJvmEntity<E,T>()

class JvmMemoryDataStoreFactory : DataStoreFactory {
    override fun create(cfg: DataStoreConfig): DataStore = JvmMemoryDataStore(JvmMemCfg(cfg))
}


class JvmMemCfg(cfg: DataStoreConfig) : DataStoreConfig by cfg {
    private val dirStr: String? = cfg.config["dir"]?.toString()
    val dir: Path? = dirStr?.let { Paths.get(it) }
}

val log = KotlinLogging.logger {  }
class JvmMemoryDataStore(cfg: JvmMemCfg) : DataStoreJvm(cfg.name) {
    private val pool = CommonPool
    private val dir = cfg.dir
    private val _metas = mutableMapOf<KClass<*>, KIJvmEntityMeta<*, *>>()
    private val metas: MutableMap<KClass<*>, KIJvmEntityMeta<*, *>> = object : MutableMap<KClass<*>, KIJvmEntityMeta<*, *>> by _metas {
        override fun get(key: KClass<*>): KIJvmEntityMeta<*, *>? = run {
            if (key !in this) {
                val pck = key.qualifiedName!!.split('.').dropLast(1).joinToString(".", postfix = ".jvm.mem.")
                val cn = "$pck${key.simpleName}JvmMem"
                val kc = java.lang.Class.forName(cn).kotlin
                assert(kc.companionObjectInstance is KIJvmEntitySupport<*, *>)
                val meta = kc.companionObjectInstance!!.cast<KIJvmEntitySupport<*, *>>().meta
                this[key] = meta
            }
            _metas[key]
        }
    }

    val db: DB

    init {
        db =
                if (dir != null)
                    DBMaker.fileDB(dir.resolve("$name.db").toFile()).make()
                else
                    DBMaker.memoryDB().make()

    }

    operator fun get(kc: KClass<*>): KIJvmEntityMeta<*, *>? = metas[kc]

    inline fun <reified E : KIEntity<K>, K : Comparable<K>> retrieve(id: K): Deferred<Try<E>> = run {
        val kc : KClass<E> = E::class
        async {
            Try {
                buckets[E::class]!!.let { bucket ->
                    val meta = this@JvmMemoryDataStore[kc]
                    bucket[id]!!.let { values ->
                        meta?.new(this@JvmMemoryDataStore, id)!!.cast<E>()
                    }
                }
            }
        }
    }

    inline operator fun <reified E : KIEntity<K>, K : Comparable<K>, reified V : Any> get(id: K, prop: KIJvmEntityMeta<E,K>.Property): Try<V?> = Try {
        buckets[E::class]!!.let { bucket ->
            bucket[id]?.get(prop.name)?.cast<V?>()
        }
    }

    inline fun <reified E : KIEntity<K>, K : Comparable<K>, reified V : Any> getProp(id: K, prop: KIJvmEntityMeta<E,K>.Property): V? = run {
        buckets[E::class]!!.let { bucket ->
            bucket[id]?.get(prop.name)?.cast<V?>()
        }
    }

    inline fun <reified E : KIEntity<K>, K : Comparable<K>, reified V : Any> setProp(id: K, prop: KIJvmEntityMeta<E,K>.Property,v:V?) = run {
        buckets[E::class]!!.let { bucket ->
            log.trace {  "setProp $id in $bucket entity: ${bucket[id]}" }
            bucket[id] = mapOf(prop.name to v)
            log.trace { "after setProp $id in $bucket entity: ${bucket[id]}" }
        }
    }

    override fun <K : Comparable<K>> create(type: KClass<*>, values: Map<String, Any?>): Try<Deferred<Try<K>>> = Try { TODO("not implemented") }

    override fun <K : Comparable<K>> create(type: KClass<*>, id: K, values: Map<String, Any?>): Try<Deferred<Try<K>>> = Try {
        async(pool) {
                Try {
                    @Suppress("UNCHECKED_CAST")
                    val meta = this@JvmMemoryDataStore[type] as KIJvmEntityMeta<*, K>?

                    meta!!.let { m ->
                        buckets[m.me]!!.let { bucket ->
                            assert(bucket[id]==null)
                            bucket.create(id, values)
                            id
                        }
                    }

                }
        }
    }

    @Suppress("UNCHECKED_CAST")
    operator fun<E:KIEntity<K>,K:Comparable<K>>get(type:KClass<*>, id:K) : E? = metas[type]!!.new(this, id) as? E

    inline fun <reified E : KIEntity<K>, K : Comparable<K>> version(id: K): Long? = buckets[E::class]!!.version(id)

    inner class Buckets(val map: MutableMap<KClass<*>, Bucket>) : Map<KClass<*>, Bucket> by map {
        override fun get(key: KClass<*>): Bucket? = if(key in map) map[key] else {
            metas[key]?.let {meta ->
                map[meta.me] = Bucket(meta)
                map[meta.me]
            }
        }
    }

    inner class Bucket(val meta: KIJvmEntityMeta<*,*>) {
        val versioned = Versioned::class.java.isAssignableFrom(meta.impl.java)
        @Suppress("UNCHECKED_CAST")
        val bucket = db.hashMap(meta.name).createOrOpen() as HTreeMap<Comparable<*>,MutableMap<String,Any?>>

        operator fun get(k: Any): MutableMap<String, Any?>? = bucket[k]?.cast<MutableMap<String, Any?>>()
        operator fun get(k: Any, prop: String): Any? = bucket[k]!!.cast<MutableMap<String, Any?>>()[prop]

        operator fun set(k: Comparable<*>, values: Map<String, Any?>) : Map<String,Any?> = run {
            assert(!versioned)
            log.trace { "set $k $values" }
            db.set(k, values)
        }

        private fun DB.set(k: Comparable<*>, values: Map<String, Any?>) : Map<String,Any?> = run {
            val e = bucket[k]!!
            val changed = values.filter { entry -> e[entry.key] != entry.value }
            log.trace {"changed $changed"}
            e.putAll(changed)
            bucket.put(k, e)
            index(k, changed)
            commit()
            changed
        }

        operator fun set(k: Comparable<*>, version:Long,values: Map<String, Any?>) : Map<String,Any?> = db.run {
            assert(versioned)
            val e = this@Bucket[k]!!
            e["_version"]!!.cast<Atomic.Long>().compareAndSet(version, version+1)
            db.set(k, values)
        }


        fun create(id:Comparable<*>,values: Map<String, Any?>) {
            bucket[id] = values.toMutableMap()
            if(versioned) {
                this[id]!!["_version"] = db.atomicLong("${meta.me.qualifiedName}.$id", 0)
            }
            index(id, values)

            db.commit()
        }

        fun version(id:Comparable<*>) : Long = (bucket[id]!!["_version"])!!.cast<AtomicLong>().get()

        @Suppress("UNUSED_PARAMETER")
        fun index(k: Any, values: Map<String, Any?>) {

        }
    }

    val buckets = Buckets(mutableMapOf())
}