package info.kinterest.datastores.jvm.memory

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.KodeinInjector
import com.github.salomonbrys.kodein.instance
import info.kinterest.*
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactory
import info.kinterest.datastores.jvm.DataStoreJvm
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.KIJvmEntitySupport
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import kotlinx.coroutines.experimental.*
import mu.KotlinLogging
import org.mapdb.DB
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.reflect.KClass
import kotlin.reflect.full.companionObjectInstance

abstract class KIJvmMemEntity<E : KIEntity<T>, T : Comparable<T>>(override val _store: DataStore, override val id: T) : KIJvmEntity<E, T>()

class JvmMemoryDataStoreFactory : DataStoreFactory {
    override lateinit var kodein: Kodein
    override val injector: KodeinInjector = KodeinInjector()

    init {
        onInjected({ k -> kodein = k })
    }


    override fun create(cfg: DataStoreConfig): DataStore = JvmMemoryDataStore(JvmMemCfg(cfg)).apply { inject(kodein) }
}


class JvmMemCfg(cfg: DataStoreConfig) : DataStoreConfig by cfg {
    private val dirStr: String? = cfg.config["dir"]?.toString()
    val dir: Path? = dirStr?.let { Paths.get(it) }
}

val log = KotlinLogging.logger { }

class JvmMemoryDataStore(cfg: JvmMemCfg) : DataStoreJvm(cfg.name) {
    private val events: Dispatcher<EntityEvent<*, *>> by instance("entities")
    @Suppress("MemberVisibilityCanBePrivate")
    val pool = CommonPool
    private val dir = cfg.dir
    private val _metas = mutableMapOf<KClass<*>, KIJvmEntityMeta>()
    private val metas: MutableMap<KClass<*>, KIJvmEntityMeta> = object : MutableMap<KClass<*>, KIJvmEntityMeta> by _metas {
        override fun get(key: KClass<*>): KIJvmEntityMeta? = run {
            if (key !in this) {
                val pck = key.qualifiedName!!.split('.').dropLast(1).joinToString(".", postfix = ".jvm.mem.")
                val cn = "$pck${key.simpleName}JvmMem"
                val kc = java.lang.Class.forName(cn).kotlin
                assert(kc.companionObjectInstance is KIJvmEntitySupport<*, *>)
                val meta = kc.companionObjectInstance!!.cast<KIJvmEntitySupport<*, *>>().meta
                this[key] = meta
                metaProvider.register(meta)
            }
            if (key !in this) throw DataStoreError.MetaDataNotFound(key, this@JvmMemoryDataStore)
            _metas[key]
        }
    }

    operator fun get(kc: KClass<*>): KIJvmEntityMeta = run {
        metas[kc]!!
    }

    val db: DB

    init {
        db =
                if (dir != null)
                    DBMaker.fileDB(dir.resolve("$name.db").toFile()).make()
                else
                    DBMaker.memoryDB().make()

    }

    override fun getValues(type: KIEntityMeta, id: Any): Map<String, Any?>? = buckets[type.me]?.get(id)
    override fun getValues(type: KIEntityMeta, id: Any, vararg props: KIProperty<*>): Map<String, Any?>? = getValues(type, id, props.toList())
    override fun getValues(type: KIEntityMeta, id: Any, props: Iterable<KIProperty<*>>): Map<String, Any?>? = getValues(type, id)?.filterKeys { key -> props.any { it.name == key } }

    override fun setValues(type: KIEntityMeta, id: Any, values: Map<KIProperty<*>, Any?>) {
        buckets[type.me]?.let { bucket ->
            bucket.set(id, values.map { it.key.name to it.value }.toMap())
        }
    }

    override fun <E : KIEntity<K>, K : Any> query(type: KIEntityMeta, f: EntityFilter<E, K>): Try<Deferred<Try<Iterable<K>>>> = Try {
        val bucket = buckets[type.me.cast()]
        bucket?.let {
            async {
                Try { bucket.query(f) }
            }
        } ?: throw DataStoreError.MetaDataNotFound(type.me.cast(), this)
    }

    override fun <E : KIEntity<K>, K : Any> retrieve(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>> = Try {
        val b = buckets[type.me as KClass<*>]
        b?.let { ab ->
            async(pool) {
                Try {
                    ab.let { bucket ->
                        ids.map { id ->
                            if (bucket[id] == null) throw DataStoreError.EntityError.EntityNotFound(type, id, this@JvmMemoryDataStore)
                            @Suppress("UNCHECKED_CAST")
                            type.new(this@JvmMemoryDataStore, id) as E
                        }
                    }
                }
            }
        } ?: throw DataStoreError.BatchError("failure to retrieve", type.me.cast(), this)

    }

    inline operator fun <reified E : KIEntity<K>, K : Any, reified V : Any> get(id: K, prop: KIProperty<V>): Try<V?> = Try {
        buckets[E::class]!!.let { bucket ->
            if (bucket[id] == null) throw DataStoreError.EntityError.EntityNotFound(this[E::class]!!, id, this)
            bucket[id]?.get(prop.name)?.cast<V?>()
        }
    }

    inline fun <reified E : KIEntity<K>, K : Any, reified V : Any> getProp(id: K, prop: KIProperty<V>): V? = run {
        buckets[E::class]!!.let { bucket ->
            bucket[id]?.get(prop.name)?.cast()
        }
    }

    inline fun <reified E : KIEntity<K>, K : Any, reified V : Any>
            setProp(id: K, prop: KIProperty<V>, v: V?): Deferred<Try<Unit>> =
            async(pool) {
                Try {
                    buckets[E::class]!!.let { bucket ->
                        log.trace { "setProp $id in $bucket entity: ${bucket[id]}" }
                        bucket[id] = mapOf(prop.name to v)
                        log.trace { "after setProp $id in $bucket entity: ${bucket[id]}" }
                    }
                }
            }

    inline fun <reified E : KIEntity<K>, K : Any, reified V : Any>
            setProp(id: K, prop: KIProperty<V>, v: V?, version: Long): Deferred<Try<Unit>> =
            prop(E::class, id, prop, v, version)

    fun <E : KIEntity<K>, K : Any, V : Any> prop(type: KClass<*>, id: K, prop: KIProperty<V>, v: V?, version: Long): Deferred<Try<Unit>> = async(pool) {
        Try {
            buckets[type]!!.let { bucket: Bucket ->
                log.trace { ">>> setProp $id in $bucket entity: ${bucket[id]} version: $version version current: ${bucket.version(id)}" }
                bucket[id, version] = mapOf(prop.name to v)
                log.trace { "<<< setProp $id in $bucket entity: ${bucket[id]} version: $version version current: ${bucket.version(id)}" }
            }
        }
    }

    override fun <K : Any> delete(type: KIEntityMeta, entities: Iterable<K>): Try<Deferred<Either<DataStoreError, Iterable<K>>>> = Try {
        async {
            Try {
                buckets[type.me as KClass<*>]!!.delete(entities)
            }.fold({ ex ->
                Either.left<DataStoreError, Iterable<K>>(DataStoreError.BatchError(
                        "error deleting entities", type, this@JvmMemoryDataStore, ex))
            },
                    { res -> Either.right(res) })
        }
    }

    override fun <K : Any> create(type: KIEntityMeta, entities: Iterable<Pair<K, Map<String, Any?>>>): Try<Deferred<Try<Iterable<K>>>> = Try {
        val b = buckets.get(type.me as KClass<*>)
        b?.let { bucket ->
            async {
                bucket.create(entities).cast<Try<Iterable<K>>>()
            }
        } ?: throw DataStoreError.MetaDataNotFound(type.me, this)
    }

    fun <K : Comparable<K>> create(type: KClass<*>, id: K, values: Map<String, Any?>): Try<Deferred<Try<K>>> = Try {
        async(pool) {
            Try {
                @Suppress("UNCHECKED_CAST")
                val meta = this@JvmMemoryDataStore[type]

                meta.let { m ->
                    buckets[m.me]!!.let { bucket ->
                        assert(bucket.bucket[id] == null)
                        if (bucket[id] != null) throw DataStoreError.EntityError.EntityExists(meta, id, this@JvmMemoryDataStore)
                        bucket.create(listOf(id to values))
                        id
                    }
                }

            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    inline operator fun <reified E : KIEntity<K>, K : Any> get(id: K): E? = this[E::class].new(this, id) as? E

    operator inline fun <reified E : KIEntity<K>, reified K : Any> contains(id: K): Boolean = buckets[this[E::class].me]!![id] != null

    inline fun <reified E : KIEntity<K>, K : Any> version(id: K): Long? = buckets[E::class]!!.version(id)

    private fun <R> DB.tx(tx: DB.() -> R): R = try {
        val res = this.tx()
        commit()
        res
    } catch (e: Throwable) {
        rollback()
        throw e
    }

    inner class Buckets(val map: MutableMap<KClass<*>, Bucket>) : Map<KClass<*>, Bucket> by map {
        override fun get(key: KClass<*>): Bucket? = if (key in map) map[key] else {
            metas[key]?.let { meta ->
                map[meta.me] = Bucket(meta)
                map[meta.me]
            }
        }
    }

    inner class Bucket(val meta: KIJvmEntityMeta) {
        val versioned = Versioned::class.java.isAssignableFrom(meta.impl.java)
        @Suppress("UNCHECKED_CAST")
        val bucket = db.hashMap(meta.name).createOrOpen() as HTreeMap<Any, MutableMap<String, Any?>>

        operator fun get(keyes: Iterable<Any>): Iterable<Map<String, Any?>> = keyes.map { get(it) }.filterNotNull()

        operator fun get(k: Any): MutableMap<String, Any?>? = bucket[k]?.apply {
            put("id", k)
            if (versioned) put("_version", version(k))
        }?.cast<MutableMap<String, Any?>>()

        operator fun set(k: Any, values: Map<String, Any?>): Map<String, Any?> = run {
            assert(!versioned)
            log.trace { "set $k $values" }
            db.set(k, values)
        }

        @Suppress("UNCHECKED_CAST")
        internal fun DB.set(k: Any, values: Map<String, Any?>): Map<String, Any?> = tx {
            val e = bucket[k]!!
            val changed = values.filter { entry -> e[entry.key] != entry.value }
            val olds = changed.map { it.key to e[it.key] }.toMap()
            log.trace { "changed $changed" }
            e.putAll(changed)
            bucket[k] = e
            index(k, changed)
            changed.apply {
                launch(pool) {
                    val upds = changed.map { val prop = meta.props[it.key]; EntityUpdateEvent<Any>(prop as KIProperty<Any>, olds[it.key], e[it.key]) }
                    events.incoming.send(EntityUpdatedEvent(k as Comparable<Any>, this@Bucket.meta as KIEntityMeta, upds))
                }
                Unit
            }
        }

        internal operator fun set(k: Any, version: Long, values: Map<String, Any?>): Map<String, Any?> = db.run {
            assert(versioned)
            if (bucket[k] == null) throw DataStoreError.EntityError.EntityNotFound(meta, k, this@JvmMemoryDataStore)
            val versionName = versionName(k)
            val current = Try { db.atomicLong(versionName).open() }.getOrElse {
                throw DataStoreError.EntityError.VersionNotFound(meta, k, this@JvmMemoryDataStore, cause = it)
            }
            if (current.compareAndSet(version, version + 1)) {
                db.set(k, values)
            } else throw DataStoreError.OptimisticLockException(meta, k, current, version, this@JvmMemoryDataStore)
        }


        private fun versionName(k: Any) = "${meta.me.simpleName}.$k._version"

        fun create(entites: Iterable<Pair<Any, Map<String, Any?>>>): Try<Iterable<Any>> = Try {
            db.tx {
                entites.map { (id, values) ->
                    val map = values.toMutableMap()
                    Try { bucket[id] = map }.getOrElse { throw DataStoreError.EntityError.EntityExists(meta, id, this@JvmMemoryDataStore) }
                    if (versioned) Try { db.atomicLong(versionName(id), 0).create() }.getOrElse {
                        throw DataStoreError.EntityError.VersionAlreadyExists(meta, id, this@JvmMemoryDataStore, it)
                    }
                    id
                }
            }.apply {
                val created = this
                launch(pool) {
                    for (c in created) {
                        @Suppress("UNCHECKED_CAST")
                        events.incoming.send(EntityCreateEvent(c as Comparable<Any>, meta))
                    }
                }
                Unit
            }
        }

        fun <K : Any> delete(ids: Iterable<K>): Iterable<K> = db.tx {
            ids.map { db.delete(it) }.map { del -> if (del.first == null) null else del.second }.filterNotNull().apply {
                launch(pool) {
                    @Suppress("UNCHECKED_CAST")
                    for (id in this@apply) events.incoming.send(EntityDeleteEvent(id, meta))
                }
                Unit
            }
        }

        private fun <K : Any> DB.delete(id: K): Pair<MutableMap<String, Any?>?, K> = run {
            if (versioned) {
                val atomic = atomicLong(versionName(id)).open()
                getStore().delete(atomic.recid, defaultSerializer)

            }
            bucket.remove(id) to id

        }

        fun version(id: Any): Long = db.atomicLong(versionName(id)).open().get()

        @Suppress("UNUSED_PARAMETER")
        fun index(k: Any, values: Map<String, Any?>) {

        }

        fun <K : Any> query(f: EntityFilter<*, K>): Iterable<K> = run {
            bucket.iterator().asSequence().filter { entry ->
                f.matches(entry.value.apply {
                    put("id", entry.key)
                })
            }.map { it.key as K }.asIterable()
        }
    }

    val buckets = Buckets(mutableMapOf())
}