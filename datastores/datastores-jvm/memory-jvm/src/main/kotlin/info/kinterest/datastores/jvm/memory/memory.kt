package info.kinterest.datastores.jvm.memory

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.KodeinInjector
import info.kinterest.*
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactory
import info.kinterest.datastores.jvm.DataStoreJvm
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.map
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page
import info.kinterest.query.Query
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.*
import mu.KLogging
import mu.KotlinLogging
import org.mapdb.DB
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.reflect.KClass
import kotlin.reflect.full.companionObjectInstance


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
    private val pool: CoroutineDispatcher = newFixedThreadPoolContext(8, "jvm.mem")
    private val dir = cfg.dir
    private val _metas = mutableMapOf<KClass<*>, KIJvmEntityMeta>()
    //TODO: remove this, metas should be handled only by MetaProvider
    private val metas: MutableMap<KClass<*>, KIJvmEntityMeta> = object : MutableMap<KClass<*>, KIJvmEntityMeta> by _metas {
        override fun get(key: KClass<*>): KIJvmEntityMeta? = run {
            if (key !in this) {
                val pck = key.qualifiedName!!.split('.').dropLast(1).joinToString(".", postfix = ".jvm.")
                val cn = "$pck${key.simpleName}Jvm"
                val kc = java.lang.Class.forName(cn).kotlin
                assert(kc.companionObjectInstance is EntitySupport)
                val meta = kc.companionObjectInstance!!.cast<EntitySupport>().meta
                this[key] = meta as KIJvmEntityMeta
                metaProvider.register(meta)
            }
            if (key !in this) throw DataStoreError.MetaDataNotFound(key.cast(), this@JvmMemoryDataStore)
            _metas[key]
        }
    }

    operator fun get(kc: KClass<*>): KIJvmEntityMeta = metas[kc]!!

    val db: DB

    init {
        db =
                if (dir != null)
                    DBMaker.fileDB(dir.resolve("$name.db").toFile()).make()
                else
                    DBMaker.memoryDB().make()

    }

    override fun getValues(type: KIEntityMeta, id: Any): Deferred<Try<Map<String, Any?>?>> = async(pool) { Try { buckets[type]?.get(id) } }
    override fun getValues(type: KIEntityMeta, id: Any, vararg props: KIProperty<*>): Deferred<Try<Map<String, Any?>?>> = getValues(type, id, props.toList())
    override fun getValues(type: KIEntityMeta, id: Any, props: Iterable<KIProperty<*>>): Deferred<Try<Map<String, Any?>?>> = getValues(type, id).map { it.map { it?.filterKeys { key -> props.any { it.name == key } } } }

    override fun setValues(type: KIEntityMeta, id: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>> =
            async(pool) {
                Try {
                    buckets[type]?.let { bucket ->
                        bucket.set(id, values.map { it.key.name to it.value }.toMap())
                    }
                    Unit
                }
            }


    override fun setValues(type: KIEntityMeta, id: Any, version: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>> = async(pool) {
        Try {
            require(version is Long)
            buckets[type]?.let { bucket ->
                bucket.set(id, version as Long, values.map { it.key.name to it.value }.toMap())
            }
            Unit
        }
    }

    override fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<Page<E, K>>>> = Try {
        val bucket = buckets[query.f.meta]!!
        async(pool) {
            Try {
                bucket.query(query)
            }
        }
    }

    override fun <E : KIEntity<K>, K : Any> retrieveLenient(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>> = Try {
        buckets[type]?.let { bucket ->
            val idf = ids.filter { bucket[it] != null }
            if (idf.isEmpty()) CompletableDeferred(Try { listOf<E>() })
            else retrieve<E, K>(type, idf).getOrElse { throw it }
        } ?: throw DataStoreError.MetaDataNotFound(type.me, this)
    }

    override fun <E : KIEntity<K>, K : Any> retrieve(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>> = Try {
        val b = buckets[type]
        log.debug { "retrieving $ids" }
        b?.let { ab ->
            async(pool) {
                Try {
                    ab.let { bucket ->
                        log.debug { "async retrieving $ids" }
                        ids.map { id ->
                            log.debug { "retrieving $id" }
                            if (bucket[id] == null) throw DataStoreError.EntityError.EntityNotFound(type, id, this@JvmMemoryDataStore)
                            @Suppress("UNCHECKED_CAST")
                            type.new(this@JvmMemoryDataStore, id) as E
                        }
                    }
                }
            }
        } ?: throw DataStoreError.BatchError("failure to retrieve $ids", type.me.cast(), this)

    }


    override fun <E : KIEntity<K>, K : Any> delete(type: KIEntityMeta, entities: Iterable<E>): Try<Deferred<Try<Iterable<K>>>> = Try {
        async {
            buckets[type]!!.delete(entities)
        }
    }


    override fun <E : KIEntity<K>, K : Any> create(type: KIEntityMeta, entities: Iterable<E>): Try<Deferred<Try<Iterable<E>>>> = Try {
        buckets[type]?.let { bucket ->
            async {
                Try { bucket.create<E, K>(entities) }
            }
        } ?: throw DataStoreError.MetaDataNotFound(type.me, this)
    }


    override fun <K : Any> version(type: KIEntityMeta, id: K): Any = buckets[type]!!.version(id)

    private fun <R> DB.tx(tx: DB.() -> R): R = Try {
        this.tx()
    }.fold({ throw it }, { it })

    inner internal class Buckets(val map: MutableMap<KIEntityMeta, Bucket>) : Map<KIEntityMeta, Bucket> by map {
        override fun get(key: KIEntityMeta): Bucket? = if (key in map) map[key] else {
            if (key.parent == null)
                map[key] = RootBucket(key)
            else map[key] = SubTypeBucket(key, this[key.hierarchy.first()]!!)
            map[key]
        }
    }


    internal interface Bucket {
        val meta: KIEntityMeta
        val versioned: Boolean
        @Suppress("UNCHECKED_CAST")
        val bucket: HTreeMap<Any, MutableMap<String, Any?>>

        operator fun get(keys: Iterable<Any>): Iterable<Map<String, Any?>>
        operator fun get(k: Any): MutableMap<String, Any?>?
        operator fun set(k: Any, values: Map<String, Any?>): Map<String, Any?>
        fun <E : KIEntity<K>, K : Any> create(entities: Iterable<E>): Iterable<E>
        fun <E : KIEntity<K>, K : Any> delete(entities: Iterable<E>): Try<Iterable<K>>
        fun version(id: Any): Long
        @Suppress("UNUSED_PARAMETER")
        fun index(k: Any, values: Map<String, Any?>)


        operator fun set(k: Any, version: Long, values: Map<String, Any?>): Map<String, Any?>

        fun <E : KIEntity<K>, K : Any> baseQuery(query: Query<E, K>): Sequence<E>

        @Suppress("UNCHECKED_CAST")
        fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Page<E, K> = run {
            val fs = baseQuery(query)
            val sortedWith = if (query.ordering === Ordering.NATURAL) fs else fs.sortedWith(query.ordering as Comparator<in E>)
            val entities = if (query.page.size >= 0) {
                val windowed = sortedWith.windowed(query.page.size, query.page.size, true)
                windowed.drop(query.page.offset / query.page.size).firstOrNull() ?: listOf()
            } else sortedWith.drop(query.page.offset).toList()
            Page(query.page, entities, if (query.page.size >= 0 && entities.size == query.page.size) 1 else 0)
        }
    }

    inner internal open class RootBucket(final override val meta: KIEntityMeta) : Bucket {
        override val versioned = meta.versioned
        @Suppress("UNCHECKED_CAST")
        override val bucket = db.hashMap(meta.name).createOrOpen() as HTreeMap<Any, MutableMap<String, Any?>>

        override operator fun get(keys: Iterable<Any>): Iterable<Map<String, Any?>> = keys.mapNotNull { get(it) }

        override operator fun get(k: Any): MutableMap<String, Any?>? = bucket[k]?.apply {
            put("id", k)
            if (versioned) put("_version", version(k))
        }?.cast<MutableMap<String, Any?>>()

        override operator fun set(k: Any, values: Map<String, Any?>): Map<String, Any?> = run {
            assert(!versioned)
            require(!versioned)
            log.trace { "set $k $values" }
            db.set(k, values)
        }

        override operator fun set(k: Any, version: Long, values: Map<String, Any?>): Map<String, Any?> = db.run {
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
                    val upds = changed.map { val prop = meta.props[it.key]; EntityUpdated(prop as KIProperty<Any>, olds[it.key], e[it.key]) }
                    events.incoming.send(EntityUpdatedEvent(meta.new(this@JvmMemoryDataStore, k), upds))
                }
                Unit
            }
        }


        private fun versionName(k: Any) = "${meta.me.simpleName}.$k._version"

        override fun <E : KIEntity<K>, K : Any> create(entities: Iterable<E>): Iterable<E> = run {
            db.tx {
                entities.map { e ->
                    e to
                            e._meta.props.map {
                                it.value.name to e.getValue(it.value)
                            }.toMap() +
                            (TYPES to e._meta.types.map { it.name }.toTypedArray())
                }.map { (e, values) ->
                    val id = e.id
                    @Suppress("UNCHECKED_CAST")
                    if (id in bucket) throw DataStoreError.EntityError.EntityExists(meta, id, this@JvmMemoryDataStore)

                    if (versioned) Try { db.atomicLong(versionName(id), 0).create() }.getOrElse {
                        throw DataStoreError.EntityError.VersionAlreadyExists(meta, id, this@JvmMemoryDataStore, it)
                    }
                    val map = values.toMutableMap()
                    bucket[id] = map
                    @Suppress("UNCHECKED_CAST")
                    e._meta.new(this@JvmMemoryDataStore, id) as E
                }.apply {
                    if (isNotEmpty())
                        runBlocking { events.incoming.send(EntityCreateEvent(this@apply)) }
                }
            }
        }


        override fun <E : KIEntity<K>, K : Any> delete(entities: Iterable<E>): Try<Iterable<K>> = Try {
            db.tx {
                entities.map { db.delete(it.id).second }
            }
        }

        private fun <K : Any> DB.delete(id: K): Pair<MutableMap<String, Any?>, K> = run {
            if (id !in bucket) Try.raise<Pair<MutableMap<String, Any?>, K>>(DataStoreError.EntityError.EntityNotFound(meta, id, this@JvmMemoryDataStore).cast())
            if (versioned) {
                val atomic = atomicLong(versionName(id)).open()
                getStore().delete(atomic.recid, defaultSerializer)

            }
            bucket.remove(id)!! to id
        }

        override fun version(id: Any): Long = db.atomicLong(versionName(id)).open().get()

        @Suppress("UNUSED_PARAMETER")
        override fun index(k: Any, values: Map<String, Any?>) {

        }

        @Suppress("UNCHECKED_CAST")
        override fun <E : KIEntity<K>, K : Any> baseQuery(query: Query<E, K>): Sequence<E> = bucket.iterator().asSequence().map { entry -> meta.new(this@JvmMemoryDataStore, entry.key as K) as E }.filter {
            query.f.matches(it)
        }


    }

    inner internal class SubTypeBucket(override val meta: KIEntityMeta, val parent: Bucket) : Bucket {
        override val bucket: HTreeMap<Any, MutableMap<String, Any?>>
            get() = parent.bucket
        override val versioned: Boolean
            get() = meta.versioned

        override fun get(keys: Iterable<Any>): Iterable<Map<String, Any?>> = parent.get(keys).filter {
            val types = it[TYPES]
            types is Array<*> && meta.name in types
        }

        fun typeFilter(m: Map<String, Any?>): Boolean = m[TYPES].let {
            it is Array<*> && meta.name in it
        }

        override fun get(k: Any): MutableMap<String, Any?>? = parent.get(k)?.let {
            val types = it[TYPES]
            if (types is Array<*> && meta.name in types) it else null
        }

        override fun set(k: Any, values: Map<String, Any?>): Map<String, Any?> = parent.set(k, values)
        override fun set(k: Any, version: Long, values: Map<String, Any?>): Map<String, Any?> = parent.set(k, version, values)

        override fun <E : KIEntity<K>, K : Any> create(entities: Iterable<E>): Iterable<E> = parent.create(entities)

        override fun <E : KIEntity<K>, K : Any> delete(entities: Iterable<E>): Try<Iterable<K>> = parent.delete(entities)
        override fun version(id: Any): Long = parent.version(id)

        @Suppress("UNCHECKED_CAST")
        override fun <E : KIEntity<K>, K : Any> baseQuery(query: Query<E, K>): Sequence<E> = bucket.iterator().asSequence().filter {
            typeFilter(it.value)
        }.map { entry -> meta.new(this@JvmMemoryDataStore, entry.key as K) as E }.filter {
            query.f.matches(it)
        }

        override fun index(k: Any, values: Map<String, Any?>) {

        }

    }

    private val buckets = Buckets(mutableMapOf())

    companion object : KLogging() {
        val TYPES = "_types"
    }
}