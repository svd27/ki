package info.kinterest.datastores.jvm.memory

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.KodeinInjector
import com.github.salomonbrys.kodein.instance
import info.kinterest.*
import info.kinterest.datastores.IRelationTrace
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactory
import info.kinterest.datastores.jvm.DataStoreJvm
import info.kinterest.datastores.jvm.RelationTrace
import info.kinterest.functional.Try
import info.kinterest.functional.flatten
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.addIncomingRelation
import info.kinterest.jvm.query.DiscriminatorsJvm
import info.kinterest.jvm.query.DistinctDiscriminators
import info.kinterest.jvm.removeIncomingRelation
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.meta.KIRelationProperty
import info.kinterest.meta.Relation
import info.kinterest.paging.Page
import info.kinterest.query.*
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import mu.KLogging
import mu.KotlinLogging
import org.mapdb.DB
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import org.mapdb.StoreTx
import org.mapdb.serializer.SerializerJava
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.Executors
import kotlin.coroutines.experimental.AbstractCoroutineContextElement
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.reflect.KClass
import kotlin.reflect.full.companionObjectInstance


class JvmMemoryDataStoreFactory : DataStoreFactory {
    override lateinit var kodein: Kodein
    override val injector: KodeinInjector = KodeinInjector()
    override val events: Channel<DataStoreEvent> by instance()

    init {
        onInjected({ k -> kodein = k })
    }


    override fun create(cfg: DataStoreConfig): DataStoreJvm = run {
        val ds = JvmMemoryDataStore(JvmMemCfg(cfg)).apply { inject(kodein) }
        runBlocking { events.send(StoreReady(ds)) }
        ds
    }
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

    override fun <E : KIEntity<K>, K : Any> querySync(query: Query<E, K>): Try<QueryResult<E, K>> = runBlocking { query(query).getOrElse { throw it }.await() }

    override fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<QueryResult<E, K>>>> = Try {
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
                Try { bucket.create(entities) }
            }
        } ?: throw DataStoreError.MetaDataNotFound(type.me, this)
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> addRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> = Try {
        async {
            Try {
                db.tx {
                    val target = rel.target
                    if (rel.rel.container == Set::class && getRelationsSync<S, K, T, L>(rel.rel, rel.source).getOrElse { throw it }.contains(rel.target)) false
                    else if (target is KIJvmEntity<*, *>) {
                        val success = target.addIncomingRelation(rel).getOrElse { throw it }
                        if (success) {
                            Try { buckets[rel.source._meta]?.addRelation(rel) }.getOrElse { target.removeIncomingRelation(rel); throw it }
                                    ?: false
                        } else false
                    } else throw DataStoreError.EntityError.EntityNotFound(rel.target._meta, rel.target.id, this@JvmMemoryDataStore)
                }
            }
        }
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> removeRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> = Try {
        async {
            Try {
                db.tx {
                    val target = rel.target
                    if (target is KIJvmEntity<*, *>) {
                        val success = target.removeIncomingRelation(rel).getOrElse { throw it }
                        if (success) {
                            Try { buckets[rel.source._meta]?.removeRelation(rel) }.getOrElse {
                                target.removeIncomingRelation(rel); throw it
                            } ?: false
                        } else false
                    } else throw DataStoreError.EntityError.EntityNotFound(rel.target._meta, rel.target.id, this@JvmMemoryDataStore)
                }
            }
        }
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> getRelations(rel: KIRelationProperty, source: S): Try<Deferred<Try<Iterable<T>>>> = Try {
        buckets.get(source._meta)?.let { bucket ->
            async { bucket.getRelations<S, K, T, L>(rel, source) }
        } ?: throw DataStoreError.MetaDataNotFound(source._meta.me, this)
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> getRelationsSync(rel: KIRelationProperty, source: S): Try<Iterable<T>> = getRelations<S, K, T, L>(rel, source).map { runBlocking { it.await().getOrElse { throw it } } }

    override fun <T : KIEntity<L>, L : Any, S : KIEntity<K>, K : Any> bookRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> = Try {
        buckets[this[rel.target._meta.me]]?.bookRelation(rel)?.getOrElse { throw it }
                ?: throw DataStoreError.MetaDataNotFound(rel.target._meta.me, this)
    }

    override fun <T : KIEntity<L>, L : Any, S : KIEntity<K>, K : Any> unbookRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> = Try {
        buckets[this[rel.target._meta.me]]?.unbookRelation(rel)?.getOrElse { throw it }
                ?: throw DataStoreError.MetaDataNotFound(rel.target._meta.me, this)
    }

    override fun getBookedRelationsSync(rel: KIRelationProperty, entity: KIEntity<Any>, sourceMeta: KIEntityMeta): Try<Iterable<IRelationTrace>> = Try {
        buckets[entity._meta]?.getBookedRelations(rel, entity, sourceMeta) ?: emptyList()
    }

    override fun <K : Any> version(type: KIEntityMeta, id: K): Any = buckets[type]!!.version(id)


    inner internal class Buckets(val map: MutableMap<KIEntityMeta, Bucket>) : Map<KIEntityMeta, Bucket> by map {
        override fun get(key: KIEntityMeta): Bucket? = if (key in map) map[key] else {
            if (key.parent == null)
                map[key] = RootBucket(key)
            else map[key] = SubTypeBucket(key, this[key.hierarchy.first()]!!)
            map[key]
        }
    }


    internal interface Bucket {
        val db: DB
        val ds: JvmMemoryDataStore
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

        fun <S : KIEntity<L>, L : Any, T : KIEntity<K>, K : Any> bookRelation(rel: Relation<S, T, L, K>): Try<Boolean> = Try {
            bucket[rel.target.id]?.let {
                db.tx {
                    @Suppress("UNCHECKED_CAST")
                    var incomings: Map<String, Map<String, List<RelationTrace>>> = it.getOrElse(INCOMING) { mapOf<String, Map<String, List<RelationTrace>>>() } as Map<String, Map<String, List<RelationTrace>>>
                    var tracesMap: Map<String, List<RelationTrace>> = incomings.getOrElse(rel.source._meta.name) { mapOf() }
                    var traces = tracesMap.getOrElse(rel.rel.name) { listOf() }
                    traces += RelationTrace(rel.target._meta.name, rel.source.id, rel.target._store.name)
                    tracesMap += rel.rel.name to traces
                    incomings += rel.source._meta.name to tracesMap

                    it[INCOMING] = incomings
                    bucket[rel.target.id] = it
                }
            } ?: throw DataStoreError.EntityError.EntityNotFound(meta, rel.target.id, ds)
            true
        }

        fun <S : KIEntity<L>, L : Any, T : KIEntity<K>, K : Any> unbookRelation(rel: Relation<S, T, L, K>): Try<Boolean> = Try {
            bucket[rel.target.id]?.let {
                db.tx {
                    @Suppress("UNCHECKED_CAST")
                    var incomings: Map<String, Map<String, List<RelationTrace>>> = it.getOrElse(INCOMING) { mapOf<String, Map<String, List<RelationTrace>>>() } as Map<String, Map<String, List<RelationTrace>>>
                    var tracesMap: Map<String, List<RelationTrace>> = incomings.getOrElse(rel.source._meta.name) { mapOf() }
                    var traces = tracesMap.getOrElse(rel.rel.name) { listOf() }
                    traces -= RelationTrace(rel.target._meta.name, rel.source.id, rel.target._store.name)
                    if (traces.isEmpty()) tracesMap -= rel.rel.name
                    else tracesMap += rel.rel.name to traces
                    if (tracesMap.isEmpty()) incomings -= rel.source._meta.name
                    else incomings += rel.source._meta.name to tracesMap

                    it[INCOMING] = incomings
                    bucket[rel.target.id] = it
                }
            } ?: throw DataStoreError.EntityError.EntityNotFound(meta, rel.target.id, ds)
            true
        }

        fun getBookedRelations(rel: KIRelationProperty, entity: KIEntity<*>, sourceMeta: KIEntityMeta): Iterable<RelationTrace> = bucket[entity.id]?.let {
            @Suppress("UNCHECKED_CAST")
            val incomings: Map<String, Map<String, List<RelationTrace>>> = it.getOrElse(INCOMING) {
                mapOf<String, Map<String, List<RelationTrace>>>()
            } as Map<String, Map<String, List<RelationTrace>>>
            (incomings[sourceMeta.name]?.get(rel.name))
        } ?: emptyList()



        operator fun set(k: Any, version: Long, values: Map<String, Any?>): Map<String, Any?>

        fun <E : KIEntity<K>, K : Any> baseQuery(query: Query<E, K>): Sequence<Pair<E, Map<String, Any?>>>

        @Suppress("UNCHECKED_CAST")
        fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): QueryResult<E, K> = run {
            val fs = baseQuery(query)


            val pres = query.projections.map { proj ->
                loadProjection(proj, fs.toList().asSequence())
            }.associateBy { it.projection }
            QueryResult(query, pres)
        }

        fun <E : KIEntity<K>, K : Any> loadProjection(proj: Projection<E, K>, fs: Sequence<Pair<E, Map<String, Any?>>>): ProjectionResult<E, K> {
            fun count(prop: KIProperty<*>, values: Map<String, Any?>): Long = when (prop) {
                is KIRelationProperty -> {
                    val rels = values[RELATIONS] as Map<String, List<RelationTrace>>
                    rels.getOrElse(prop.name) { listOf() }.size.toLong()
                }
                else -> if (values[prop.name] != null) 1 else 0
            }
            return when (proj) {
                is ValueProjection<E, K> ->
                    when (proj) {
                        is CountProjection<E, K> -> CountProjectionResult(proj, fs.map { count(proj.property, it.second) }.sum())
                        is ScalarProjection<E, K, *> ->
                            when (proj) {
                                is SumProjection<E, K, *> -> ScalarProjectionResult(proj as SumProjection<E, K, Number>, fs.map { it.second[proj.property.name] }.filterIsInstance<Number>().reduce { n1, n2 -> ScalarProjection.add(n1, n2) })
                            }
                    }

                is EntityProjection<E, K> -> {
                    val sortedWith = if (proj.ordering === Ordering.NATURAL) fs.map { it.first } else fs.map { it.first }.sortedWith(proj.ordering as Comparator<in E>)
                    val entities = if (proj.paging.size >= 0) {
                        val windowed = sortedWith.windowed(proj.paging.size, proj.paging.size, true)
                        windowed.drop(proj.paging.offset / proj.paging.size).firstOrNull() ?: listOf()
                    } else sortedWith.drop(proj.paging.offset).toList()

                    EntityProjectionResult(proj, Page(proj.paging, entities, if (proj.paging.size >= 0 && entities.size >= proj.paging.size) 1 else 0))
                }
                is BucketProjection<E, K, *> -> {
                    @Suppress("UNCHECKED_CAST")
                    val bucketProjection = proj as BucketProjection<E, K, Any>
                    @Suppress("UNCHECKED_CAST")
                    val disc = bucketProjection.discriminators as DiscriminatorsJvm<E, K, Any>
                    when (disc) {
                        is DistinctDiscriminators -> {
                            val buckets = fs.groupBy { it.second.get(disc.property.name) }.map { entry ->
                                val sequence = entry.value.asSequence()
                                val projectionBucket = ProjectionBucket(
                                        disc.property,
                                        disc.discriminatorFor(entry.key),
                                        bucketProjection)
                                @Suppress("UNCHECKED_CAST")
                                projectionBucket to loadProjection(projectionBucket, sequence) as ProjectionBucketResult<E, K, Any>

                            }.toMap()
                            BucketProjectionResult(bucketProjection, buckets)
                        }
                    }
                }
                is ProjectionBucket<E, K, *> -> {
                    val projectionBucket = proj as ProjectionBucket<E, K, Any>
                    ProjectionBucketResult(projectionBucket.bucket.projections.map {
                        loadProjection(it, fs).run { this.projection to this }
                    }.toMap(), projectionBucket)
                }
            }
        }

        fun <S : KIEntity<K>, T : KIEntity<L>, K : Any, L : Any> addRelation(rel: Relation<S, T, K, L>): Boolean = db.tx {
            val res = bucket[rel.source.id]?.let {
                @Suppress("UNCHECKED_CAST")
                val rels = it.getOrElse(RELATIONS) { mapOf<String, List<RelationTrace>>() } as Map<String, List<RelationTrace>>
                val trace = RelationTrace(rel.target._meta.name, rel.target.id, rel.target._store.name)
                if (rel.rel.container == Set::class && rels[rel.rel.name]?.contains(trace) ?: false)
                    false
                else {
                    it += RELATIONS to (rels + (rel.rel.name to (rels.getOrDefault(rel.rel.name, listOf()) + trace)))
                    logger.debug { "rel: $rel entity: $it" }
                    bucket[rel.source.id] = it
                    launch { ds.events.incoming.send(EntityRelationsAdded(listOf(rel))) }
                    true
                }
            }

            res
        } ?: throw DataStoreError.EntityError.EntityNotFound(rel.source._meta, rel.source.id, ds)

        fun <S : KIEntity<K>, T : KIEntity<L>, K : Any, L : Any> removeRelation(rel: Relation<S, T, K, L>): Boolean = db.tx {
            val res = bucket[rel.source.id]?.let {
                @Suppress("UNCHECKED_CAST")
                val rels = it.getOrElse(RELATIONS) { mapOf<String, List<RelationTrace>>() } as Map<String, List<RelationTrace>>
                logger.trace { "rels before remove: $rels" }

                val list = rels.getOrDefault(rel.rel.name, listOf())
                val relationTrace = RelationTrace(rel.target._meta.name, rel.target.id, rel.target._store.name)
                val res = relationTrace in list
                it += RELATIONS to (rels + (rel.rel.name to (list - relationTrace)))
                logger.trace { "rels after remove ${it[RELATIONS]}" }
                logger.debug { "rel: $rel entity: $it" }
                bucket[rel.source.id] = it
                res
            }
            launch { ds.events.incoming.send(EntityRelationsRemoved(listOf(rel))) }
            res
        } ?: throw DataStoreError.EntityError.EntityNotFound(rel.source._meta, rel.source.id, ds)


        fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> getRelations(rel: KIRelationProperty, source: S): Try<Iterable<T>> = Try {
            logger.debug { "rel: $rel, source: $source ${bucket[source.id]}" }
            bucket[source.id]?.let { values ->
                values[RELATIONS]?.let {
                    @Suppress("UNCHECKED_CAST")
                    val rels = it as Map<String, List<RelationTrace>>
                    @Suppress("UNCHECKED_CAST")
                    val ids = rels.getOrElse(rel.name) { listOf() }.map { it.id as L }
                    val dss = rels.getOrElse(rel.name) { listOf() }.map { DataStore(it.ds) }.toSet()
                    if (ids.isEmpty()) listOf() else
                        runBlocking { ds.qm.retrieve<T, L>(ds.metaProvider.meta(rel.target)!!, ids, dss).getOrElse { throw it }.await() }.getOrElse { throw it }
                }
            } ?: throw DataStoreError.EntityError.EntityNotFound(meta, source.id, ds)
        }



    }

    inner internal open class RootBucket(final override val meta: KIEntityMeta) : Bucket {
        override final val db: DB
            get() = this@JvmMemoryDataStore.db
        override val ds
            get() = this@JvmMemoryDataStore
        override val versioned = meta.versioned
        @Suppress("UNCHECKED_CAST")
        override val bucket = db.hashMap(meta.name).valueSerializer(SerializerJava.JAVA).createOrOpen() as HTreeMap<Any, MutableMap<String, Any?>>

        override operator fun get(keys: Iterable<Any>): Iterable<Map<String, Any?>> = keys.mapNotNull { get(it) }

        override operator fun get(k: Any): MutableMap<String, Any?>? = bucket[k]?.apply {
            put("id", k)
            if (versioned) put("_version", version(k))
        }?.cast<MutableMap<String, Any?>>()

        override operator fun set(k: Any, values: Map<String, Any?>): Map<String, Any?> = db.tx {
            assert(!versioned)
            require(!versioned)
            log.trace { "write $k $values" }
            write(k, values)
        }

        override operator fun set(k: Any, version: Long, values: Map<String, Any?>): Map<String, Any?> = db.tx {
            assert(versioned)
            if (bucket[k] == null) throw DataStoreError.EntityError.EntityNotFound(meta, k, this@JvmMemoryDataStore)
            val versionName = versionName(k)
            val current = Try { db.atomicLong(versionName).open() }.getOrElse {
                throw DataStoreError.EntityError.VersionNotFound(meta, k, this@JvmMemoryDataStore, cause = it)
            }
            if (current.compareAndSet(version, version + 1)) {
                write(k, values)
            } else throw DataStoreError.OptimisticLockException(meta, k, current, version, this@JvmMemoryDataStore)
        }

        @Suppress("UNCHECKED_CAST")
        internal fun write(k: Any, values: Map<String, Any?>): Map<String, Any?> = run {
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
                fun relations(e: E): Map<String, List<RelationTrace>>? = e._meta.props.values.filterIsInstance<KIRelationProperty>().map { rel ->
                    rel.name to if (e.getValue(rel) is Collection<*>) {
                        @Suppress("UNCHECKED_CAST")
                        (e.getValue(rel) as Collection<KIJvmEntity<*, *>>).map { target -> Relation(rel, e, target) to RelationTrace(target._meta.name, target.id, target._store.name) }
                    } else {
                        val target = e.getValue(rel) as KIJvmEntity<*, *>
                        listOf(Relation(rel, e, target) to RelationTrace(target._meta.name, target.id, target._store.name))
                    }
                }.map {
                    it.first to it.second.map {
                        it.first.target.addIncomingRelation(it.first); it.second
                    }
                }.toMap()
                entities.map { e ->
                    e to
                            e._meta.props.filter { it.value !is KIRelationProperty }.map {
                                it.value.name to e.getValue(it.value)
                            }.toMap() +
                            (TYPES to e._meta.types.map { it.name }.toTypedArray()) +
                            (TYPE to e._meta.name) +
                            (RELATIONS to relations(e))
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
                val trans = entities.map { it.asTransient() as E }
                val res = entities.map { db.delete(it.id).second }
                launch {
                    events.incoming.send(EntityDeleteEvent(trans))
                }
                res
            }
        }

        private fun <K : Any> DB.delete(id: K): Pair<MutableMap<String, Any?>, K> = run {
            if (id !in bucket) Try.raise<Pair<MutableMap<String, Any?>, K>>(DataStoreError.EntityError.EntityNotFound(meta, id, this@JvmMemoryDataStore).cast())
            bucket[id]?.let {
                it[INCOMING]?.let {
                    if (it is Map<*, *>) {
                        it.values.forEach {
                            (it as? List<*>)?.let { require(it.isEmpty()) }
                        }
                    }
                }
                it[RELATIONS]?.let {
                    if (it is Map<*, *>) {
                        @Suppress("UNCHECKED_CAST")
                        val map = it as Map<String, List<RelationTrace>>
                        map.forEach { entry ->
                            entry.value.forEach {
                                val tr = ds.qm.retrieve(ds.metaProvider.meta(it.type)!!, listOf(it.id), setOf(DataStore(it.ds)))
                                val res = tr.map { runBlocking { it.await() } }.flatten().getOrElse { throw it }.first()
                                val e = res as KIJvmEntity<*, *>
                                e.removeIncomingRelation(Relation(meta.props[entry.key]!!.cast(), meta.new(this@JvmMemoryDataStore, id), e))
                            }
                        }
                    }
                }
            }
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
        override fun <E : KIEntity<K>, K : Any> baseQuery(query: Query<E, K>): Sequence<Pair<E, Map<String, Any?>>> = bucket.iterator().asSequence().map { entry -> metaProvider.meta(entry.value[TYPE]!!.toString())!!.new(this@JvmMemoryDataStore, entry.key as K) as E to entry.value }.filter { query.f.matches(it.first) }


    }

    inner internal class SubTypeBucket(override val meta: KIEntityMeta, val parent: Bucket) : Bucket {
        override val db: DB
            get() = this@JvmMemoryDataStore.db
        override val ds
            get() = this@JvmMemoryDataStore
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
        override fun <E : KIEntity<K>, K : Any> baseQuery(query: Query<E, K>): Sequence<Pair<E, Map<String, Any?>>> = bucket.iterator().asSequence().filter {
            typeFilter(it.value)
        }.map { entry -> metaProvider.meta(entry.value[TYPE]!!.toString())!!.new(this@JvmMemoryDataStore, entry.key as K) as E to entry.value }.filter {
            query.f.matches(it.first)
        }

        override fun index(k: Any, values: Map<String, Any?>) {

        }

    }

    private val buckets = Buckets(mutableMapOf())

    companion object : KLogging() {
        val TYPES = "_types"
        val TYPE = "_type"
        val INCOMING = "_incoming"
        val RELATIONS = "_relations"
    }
}

var thread: Thread = Thread("tx")
val newSingleThreadExecutor = Executors.newSingleThreadExecutor { r -> thread = Thread(r, "tx"); log.info { "created $thread" }; thread }
private val txDispatcher: CoroutineDispatcher = newSingleThreadExecutor.asCoroutineDispatcher()
private val txEntry = newFixedThreadPoolContext(2, "txe")

private interface TX : CoroutineContext.Element {
    var tx: List<DB.() -> Any?>

    companion object TX : CoroutineContext.Key<info.kinterest.datastores.jvm.memory.TX>
}


private fun TXList(): TX = object : AbstractCoroutineContextElement(TX), TX {
    override var tx: List<DB.() -> Any?> = listOf()
}

private val txlist = TXList()

private val txl = ThreadLocal<TX>()

//private val log = KLogging()
fun <R> DB.doTx(tx: DB.() -> R): R = Try {
    txl.get().tx += tx
    val res = tx()
    txl.get().tx -= tx
    if (txl.get().tx.isEmpty()) {
        log.trace { "commit" }
        commit()
    } else log.trace { "${txl.get()} so no commit" }
    res
}.fold({
    txl.get().tx -= tx
    if (txl.get().tx.isEmpty())
        if (getStore() is StoreTx) rollback()
    throw it
}, { it }
)

internal fun <R> DB.tx(tx: DB.() -> R): R = run {
    log.trace { "tx current ${Thread.currentThread()} tx thread $thread" }
    if (Thread.currentThread() === thread) {
        log.trace { "tx in tx thread $thread" }
        doTx(tx)
    } else runBlocking(txDispatcher) {
        log.trace { "start new tx in $txDispatcher current: ${Thread.currentThread()} tx thread $thread" }
        txl.set(txlist)
        doTx(tx)
    }
}

