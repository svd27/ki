package info.kinterest.datastores.jvm.memory

import info.kinterest.*
import info.kinterest.datastores.IEntityTrace
import info.kinterest.datastores.jvm.DataStoreFactory
import info.kinterest.datastores.jvm.DataStoreJvm
import info.kinterest.datastores.jvm.EntityTrace
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.addIncomingRelation
import info.kinterest.jvm.datastores.DataStoreConfig
import info.kinterest.jvm.query.DiscriminatorsJvm
import info.kinterest.jvm.query.DistinctDiscriminators
import info.kinterest.jvm.tx.TxState
import info.kinterest.jvm.tx.jvm.AddRelationTransactionJvm
import info.kinterest.jvm.tx.jvm.CreateTransactionJvm
import info.kinterest.jvm.tx.jvm.RemoveRelationTransactionJvm
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.meta.KIRelationProperty
import info.kinterest.meta.Relation
import info.kinterest.paging.Page
import info.kinterest.query.*
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import mu.KLogging
import mu.KotlinLogging
import org.kodein.di.Kodein
import org.kodein.di.erased.instance
import org.mapdb.DB
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import org.mapdb.StoreTx
import org.mapdb.serializer.SerializerJava
import java.nio.file.Path
import java.nio.file.Paths
import java.time.OffsetDateTime
import kotlin.coroutines.experimental.AbstractCoroutineContextElement
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.coroutineContext
import kotlin.reflect.KClass
import kotlin.reflect.full.companionObjectInstance


class JvmMemoryDataStoreFactory(override var kodein: Kodein) : DataStoreFactory {
    override val events: Channel<DataStoreEvent> by instance()

    override fun create(cfg: DataStoreConfig): DataStoreJvm = run {
        val ds = JvmMemoryDataStore(JvmMemCfg(cfg), kodein)
        runBlocking { events.send(StoreReady(ds)) }
        ds
    }
}


class JvmMemCfg(cfg: DataStoreConfig) : DataStoreConfig by cfg {
    private val dirStr: String? = cfg.config["dir"]?.toString()
    val dir: Path? = dirStr?.let { Paths.get(it) }
}

val log = KotlinLogging.logger { }

class JvmMemoryDataStore(cfg: JvmMemCfg, kodein: Kodein) : DataStoreJvm(cfg.name, kodein) {
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
                        bucket.set(id, type, values.map { it.key.name to it.value }.toMap())
                    }
                    Unit
                }
            }


    override fun setValues(type: KIEntityMeta, id: Any, version: Any, values: Map<KIProperty<*>, Any?>, retries: Int): Deferred<Try<Unit>> = async(pool) {
        Try {
            require(version is Long)
            buckets[type]?.let { bucket ->
                Try { bucket.set(id, type, version as Long, values.map { it.key.name to it.value }.toMap()) }
            }?.fold({
                if (it is DataStoreError.OptimisticLockException && retries > 0) {
                    logger.debug { "retry $retries for $type, $id $version $values" }
                    runBlocking { setValues(type, id, version(type, id), values, retries - 1).await().getOrElse { throw it } }
                } else throw it
            }, { })
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
        async(pool) {
            buckets[type]!!.delete(entities)
        }
    }


    override fun <E : KIEntity<K>, K : Any> create(type: KIEntityMeta, entity: E): Try<Deferred<Try<E>>> = Try {
        buckets[type]?.let { bucket ->
            async(pool) {
                Try {
                    val id = runBlocking(coroutineContext) {
                        if (type.idInfo.generatedByDataStore)
                            generateKey(type)
                        else if (type.idInfo.generatedBy != null)
                        //TODO: generator framework
                            generateKey(type)
                        else entity.id
                    }
                    if (type.idInfo.guaranteedUnique) {
                        logger.debug { "guaranteed unique $id" }
                        bucket.create(id, entity)
                    } else {
                        var created: E? = null
                        logger.debug { "not guaranteed, creating tx for $id" }
                        tm.add(CreateTransactionJvm.Transient(this@JvmMemoryDataStore, System.nanoTime(), null, OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, type.name, id, this@JvmMemoryDataStore.name)) { res ->
                            res.map {
                                logger.debug { "done create tx $it" }
                                created = bucket.create(id, entity)
                            }
                        }.getOrElse { throw it }.second.run { runBlocking { await() } }.getOrElse { throw it }
                        created!!
                    }
                }.apply {
                    map { launch(pool) { events.incoming.send(EntityCreateEvent(listOf(it))) } }
                }
            }
        } ?: throw DataStoreError.MetaDataNotFound(type.me, this)
    }


    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> addRelation(rel: Relation<S, T, K, L>):
            Try<Deferred<Try<Boolean>>> = tm.add(AddRelationTransactionJvm.Transient(this, System.nanoTime(), null,
            OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, rel.rel.name,
            EntityTrace(rel.source._meta.name, rel.source.id, rel.source._store.name), EntityTrace(rel.target._meta.name, rel.target.id, rel.target._store.name))) {
        logger.debug { "addRelation TX result: $it" }
    }.map { it.second.map { launch { events.incoming.send(EntityRelationsAdded(listOf(rel))) }; it } }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> setRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> = Try {
        async(pool) {
            Try {
                buckets[rel.source._meta]?.addRelation(rel)
                        ?: throw DataStoreError.MetaDataNotFound(rel.source._meta.me, this@JvmMemoryDataStore)
            }
        }
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> setRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> = setRelation(rel).map { runBlocking { it.await().getOrElse { throw it } } }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> removeRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> = tm.add(RemoveRelationTransactionJvm.Transient(this, System.nanoTime(), null,
            OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, rel.rel.name,
            EntityTrace(rel.source._meta.name, rel.source.id, rel.source._store.name), EntityTrace(rel.target._meta.name, rel.target.id, rel.target._store.name))) {
        logger.debug { "addRelation TX result: $it" }
    }.map { it.second.map { launch { events.incoming.send(EntityRelationsRemoved(listOf(rel))) }; it } }


    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> unsetRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> = Try {
        async {
            db.tx {
                Try {
                    buckets[rel.source._meta]?.removeRelation(rel)
                            ?: throw DataStoreError.MetaDataNotFound(rel.source._meta.me, this@JvmMemoryDataStore)
                }
            }
        }
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> unsetRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> = unsetRelation(rel).map { runBlocking { it.await().getOrElse { throw it } } }

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

    override fun getBookedRelationsSync(rel: KIRelationProperty, entity: KIEntity<Any>, sourceMeta: KIEntityMeta): Try<Iterable<IEntityTrace>> = Try {
        buckets[entity._meta]?.getBookedRelations(rel, entity, sourceMeta) ?: emptyList()
    }

    override fun <K : Any> version(type: KIEntityMeta, id: K): Any = buckets[type]!!.version(id)


    override suspend fun <K : Any> generateKey(type: KIEntityMeta): K = when (type.idInfo.idType) {
        Long::class -> {
            val seqName = "${type.idInfo.sequence}.sequence"
            db.tx {
                @Suppress("UNCHECKED_CAST")
                db.atomicLong(seqName).createOrOpen().incrementAndGet() as K
            }
        }
        else -> throw IllegalArgumentException("bad type ${type.idInfo.idType}")
    }


    inner internal class Buckets(val map: MutableMap<KIEntityMeta, Bucket>) : Map<KIEntityMeta, Bucket> by map {
        override fun get(key: KIEntityMeta): Bucket? = if (key in map) map[key] else {
            if (key.parent == null)
                map[key] = RootBucket(key)
            else map[key] = SubTypeBucket(key, this[key.hierarchy.first()]!!)
            map[key]
        }
    }

    val writeLock = Mutex()
    val writers: MutableMap<IEntityTrace, Mutex> = mutableMapOf()

    private suspend fun lock(et: IEntityTrace): Mutex = writeLock.withLock {
        if (et !in writers) {
            writers[et] = Mutex()
        }
        writers[et]!!
    }

    private suspend fun unlock(et: IEntityTrace) = writeLock.withLock {
        if (et in writers && !(writers[et]?.isLocked ?: false)) writers.remove(et)
    }

    private suspend fun <T> write(et: IEntityTrace, cb: () -> T): T = try {
        lock(et).withLock {
            cb()
        }
    } finally {
        unlock(et)
    }

    internal abstract inner class Bucket {
        abstract val db: DB
        abstract val ds: JvmMemoryDataStore
        abstract val meta: KIEntityMeta
        abstract val versioned: Boolean
        @Suppress("UNCHECKED_CAST")
        abstract val bucket: HTreeMap<Any, MutableMap<String, Any?>>

        abstract operator fun get(keys: Iterable<Any>): Iterable<Map<String, Any?>>
        abstract operator fun get(k: Any): MutableMap<String, Any?>?
        abstract operator fun set(k: Any, type: KIEntityMeta, values: Map<String, Any?>): Map<String, Any?>
        abstract fun <E : KIEntity<K>, K : Any> create(id: K, entity: E): E
        abstract fun <E : KIEntity<K>, K : Any> delete(entities: Iterable<E>): Try<Iterable<K>>
        abstract fun version(id: Any): Long
        @Suppress("UNUSED_PARAMETER")
        abstract fun index(k: Any, values: Map<String, Any?>)

        fun <S : KIEntity<L>, L : Any, T : KIEntity<K>, K : Any> bookRelation(rel: Relation<S, T, L, K>): Try<Boolean> = Try {

            runBlocking {
                db.tx {
                    write(rel.target.asTrace()) {
                        bucket[rel.target.id]?.let {
                            @Suppress("UNCHECKED_CAST")
                            var incomings: Map<String, Map<String, List<EntityTrace>>> = it.getOrElse(INCOMING) { mapOf<String, Map<String, List<EntityTrace>>>() } as Map<String, Map<String, List<EntityTrace>>>
                            var tracesMap: Map<String, List<EntityTrace>> = incomings.getOrElse(rel.source._meta.name) { mapOf() }
                            var traces = tracesMap.getOrElse(rel.rel.name) { listOf() }
                            val entityTrace = EntityTrace(rel.target._meta.name, rel.source.id, rel.target._store.name)
                            val res = if (rel.rel.container == Set::class && entityTrace in traces) {
                                false
                            } else {
                                traces += entityTrace; true
                            }
                            tracesMap += rel.rel.name to traces
                            incomings += rel.source._meta.name to tracesMap

                            it[INCOMING] = incomings
                            bucket[rel.target.id] = it
                            res
                        } ?: throw DataStoreError.EntityError.EntityNotFound(meta, rel.target.id, ds)
                    }
                }
            }
        }

        fun <S : KIEntity<L>, L : Any, T : KIEntity<K>, K : Any> unbookRelation(rel: Relation<S, T, L, K>): Try<Boolean> = Try {

            runBlocking {
                db.tx {
                    write(rel.target.asTrace()) {
                        bucket[rel.target.id]?.let {
                            @Suppress("UNCHECKED_CAST")
                            var incomings: Map<String, Map<String, List<EntityTrace>>> = it.getOrElse(INCOMING) { mapOf<String, Map<String, List<EntityTrace>>>() } as Map<String, Map<String, List<EntityTrace>>>
                            var tracesMap: Map<String, List<EntityTrace>> = incomings.getOrElse(rel.source._meta.name) { mapOf() }
                            var traces = tracesMap.getOrElse(rel.rel.name) { listOf() }
                            traces -= EntityTrace(rel.target._meta.name, rel.source.id, rel.target._store.name)
                            if (traces.isEmpty()) tracesMap -= rel.rel.name
                            else tracesMap += rel.rel.name to traces
                            if (tracesMap.isEmpty()) incomings -= rel.source._meta.name
                            else incomings += rel.source._meta.name to tracesMap

                            it[INCOMING] = incomings
                            bucket[rel.target.id] = it
                        } ?: throw DataStoreError.EntityError.EntityNotFound(meta, rel.target.id, ds)
                    }
                }
            }
            true
        }

        fun getBookedRelations(rel: KIRelationProperty, entity: KIEntity<*>, sourceMeta: KIEntityMeta): Iterable<EntityTrace> = bucket[entity.id]?.let {
            @Suppress("UNCHECKED_CAST")
            val incomings: Map<String, Map<String, List<EntityTrace>>> = it.getOrElse(INCOMING) {
                mapOf<String, Map<String, List<EntityTrace>>>()
            } as Map<String, Map<String, List<EntityTrace>>>
            (incomings[sourceMeta.name]?.get(rel.name))
        } ?: emptyList()


        abstract operator fun set(k: Any, type: KIEntityMeta, version: Long, values: Map<String, Any?>): Map<String, Any?>

        abstract fun <E : KIEntity<K>, K : Any> baseQuery(query: Query<E, K>): Sequence<Pair<E, Map<String, Any?>>>

        @Suppress("UNCHECKED_CAST")
        fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): QueryResult<E, K> = run {
            val fs = baseQuery(query).toList()
            logger.debug { "$query result after filter $fs" }

            val pres = query.projections.map { proj ->
                loadProjection(proj, fs.asSequence())
            }.associateBy { it.projection }
            QueryResult(query, pres)
        }

        private fun <E : KIEntity<K>, K : Any> loadProjection(proj: Projection<E, K>, fs: Sequence<Pair<E, Map<String, Any?>>>): ProjectionResult<E, K> {
            fun count(prop: KIProperty<*>, values: Map<String, Any?>): Long = when (prop) {
                is KIRelationProperty -> {
                    @Suppress("UNCHECKED_CAST")
                    val rels = values[RELATIONS] as Map<String, List<EntityTrace>>
                    rels.getOrElse(prop.name) { listOf() }.size.toLong()
                }
                else -> if (values[prop.name] != null) 1 else 0
            }
            @Suppress("UNCHECKED_CAST")
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

        fun <S : KIEntity<K>, T : KIEntity<L>, K : Any, L : Any> addRelation(rel: Relation<S, T, K, L>): Boolean = runBlocking {
            db.tx {
                write(rel.source.asTrace()) {
                    bucket[rel.source.id]?.let {
                        @Suppress("UNCHECKED_CAST")
                        val rels = it.getOrElse(RELATIONS) { mapOf<String, List<EntityTrace>>() } as Map<String, List<EntityTrace>>
                        val trace = EntityTrace(rel.target._meta.name, rel.target.id, rel.target._store.name)
                        if (rel.rel.container == Set::class && rels[rel.rel.name]?.contains(trace) ?: false)
                            false
                        else {
                            it += RELATIONS to (rels + (rel.rel.name to (rels.getOrDefault(rel.rel.name, listOf()) + trace)))
                            logger.debug { "rel: $rel entity: $it" }
                            bucket[rel.source.id] = it
                            true
                        }
                    } ?: throw DataStoreError.EntityError.EntityNotFound(rel.source._meta, rel.source.id, ds)
                }
            }
        }

        fun <S : KIEntity<K>, T : KIEntity<L>, K : Any, L : Any> removeRelation(rel: Relation<S, T, K, L>): Boolean = runBlocking {
            db.tx {
                write(rel.source.asTrace()) {
                    bucket[rel.source.id]?.let {
                @Suppress("UNCHECKED_CAST")
                val rels = it.getOrElse(RELATIONS) { mapOf<String, List<EntityTrace>>() } as Map<String, List<EntityTrace>>
                logger.trace { "rels before remove: $rels" }

                val list = rels.getOrDefault(rel.rel.name, listOf())
                val relationTrace = EntityTrace(rel.target._meta.name, rel.target.id, rel.target._store.name)
                val res = relationTrace in list
                it += RELATIONS to (rels + (rel.rel.name to (list - relationTrace)))
                logger.trace { "rels after remove ${it[RELATIONS]}" }
                logger.debug { "rel: $rel entity: $it" }
                bucket[rel.source.id] = it
                res
                    } ?: throw DataStoreError.EntityError.EntityNotFound(rel.source._meta, rel.source.id, ds)
                }
            }
        }


        fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> getRelations(rel: KIRelationProperty, source: S): Try<Iterable<T>> = Try {
            logger.debug { "rel: $rel, source: $source ${bucket[source.id]}" }
            bucket[source.id]?.let { values ->
                values[RELATIONS]?.let {
                    @Suppress("UNCHECKED_CAST")
                    val rels = it as Map<String, List<EntityTrace>>
                    @Suppress("UNCHECKED_CAST")
                    val ids = rels.getOrElse(rel.name) { listOf() }.map { it.id as L }
                    val dss = rels.getOrElse(rel.name) { listOf() }.map { DataStore(it.ds) }.toSet()
                    if (ids.isEmpty()) listOf() else
                        runBlocking { ds.qm.retrieve<T, L>(ds.metaProvider.meta(rel.target)!!, ids, dss).getOrElse { throw it }.await() }.getOrElse { throw it }
                }
            } ?: throw DataStoreError.EntityError.EntityNotFound(meta, source.id, ds)
        }


    }

    inner internal open class RootBucket(final override val meta: KIEntityMeta) : Bucket() {
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

        override operator fun set(k: Any, type: KIEntityMeta, values: Map<String, Any?>): Map<String, Any?> = runBlocking {
            db.tx {
                write(EntityTrace(type.name, k, this@JvmMemoryDataStore.name)) {
                    assert(!versioned)
                    require(!versioned)
                    log.trace { "write $k $values" }
                    write(k, values)
                }
            }
        }

        override operator fun set(k: Any, type: KIEntityMeta, version: Long, values: Map<String, Any?>): Map<String, Any?> = runBlocking {
            db.tx {
                write(EntityTrace(type.name, k, this@JvmMemoryDataStore.name)) {
            if (bucket[k] == null) throw DataStoreError.EntityError.EntityNotFound(meta, k, this@JvmMemoryDataStore)
            val versionName = versionName(k)
            val current = Try { db.atomicLong(versionName).open() }.getOrElse {
                throw DataStoreError.EntityError.VersionNotFound(meta, k, this@JvmMemoryDataStore, cause = it)
            }
            logger.debug { "setting ${meta.name} with $k and version $version and values: $values on current: $current" }
            if (current.compareAndSet(version, version + 1)) {
                write(k, values)
            } else {
                logger.debug { "errot setting $values on $meta $k" }
                throw DataStoreError.OptimisticLockException(meta, k, version, current.get(), this@JvmMemoryDataStore)
            }
                }
            }
        }

        @Suppress("UNCHECKED_CAST")
        internal fun write(k: Any, values: Map<String, Any?>): Map<String, Any?> = run {
            val e = bucket[k]
            if (e == null) throw DataStoreError.EntityError.EntityNotFound(meta, k, this@JvmMemoryDataStore)
            val changed = values.filter { entry -> e[entry.key] != entry.value }
            val olds = changed.map { it.key to e[it.key] }.toMap()
            log.trace { "changed $changed" }
            e.putAll(changed)
            val meta = metaProvider.meta(e[TYPE]!!.toString())!!
            bucket[k] = e
            index(k, changed)
            changed.apply {
                launch(pool) {
                    val upds = changed.map { val prop = meta.props[it.key]; EntityUpdated(prop as KIProperty<Any>, olds[it.key], e[it.key]) }
                    events.incoming.send(EntityUpdatedEvent(meta.new(this@JvmMemoryDataStore, k), upds))
                }
            }
        }


        private fun versionName(k: Any) = "${meta.me.simpleName}.$k._version"

        override fun <E : KIEntity<K>, K : Any> create(id: K, entity: E): E = runBlocking {
            db.tx {
            fun relations(e: E): Map<String, List<EntityTrace>>? = e._meta.props.values.filterIsInstance<KIRelationProperty>().map { rel ->
                rel.name to if (e.getValue(rel) is Collection<*>) {
                    @Suppress("UNCHECKED_CAST")
                    (e.getValue(rel) as Collection<KIJvmEntity<*, *>>).map { target -> Relation(rel, e, target) to EntityTrace(target._meta.name, target.id, target._store.name) }
                } else {
                    val target = e.getValue(rel) as KIJvmEntity<*, *>
                    listOf(Relation(rel, e, target) to EntityTrace(target._meta.name, target.id, target._store.name))
                }
            }.map {
                it.first to it.second.map {
                    it.first.target.addIncomingRelation(it.first); it.second
                }
            }.toMap()

            val e = entity
            val map =
                    e._meta.props.filter { it.value !is KIRelationProperty && it.value.name != "id" }.map {
                        it.value.name to e.getValue(it.value)
                    }.toMap() +
                            (TYPES to e._meta.types.map { it.name }.toTypedArray()) +
                            (TYPE to e._meta.name) +
                            (RELATIONS to relations(e)) + ("id" to id)


            @Suppress("UNCHECKED_CAST")
            if (id in bucket) throw DataStoreError.EntityError.EntityExists(meta, id, this@JvmMemoryDataStore)

            if (versioned) Try { db.atomicLong(versionName(id), 0).create() }.getOrElse {
                throw DataStoreError.EntityError.VersionAlreadyExists(meta, id, this@JvmMemoryDataStore, it)
            }

                write(EntityTrace(e._meta.name, id, this@JvmMemoryDataStore.name)) {
                    bucket[id] = map.toMutableMap()
                }
            @Suppress("UNCHECKED_CAST")
            e._meta.new(this@JvmMemoryDataStore, id) as E
            }
        }


        override fun <E : KIEntity<K>, K : Any> delete(entities: Iterable<E>): Try<Iterable<K>> = Try {
            runBlocking {
                db.tx {
                @Suppress("UNCHECKED_CAST")
                val trans = entities.map { it.asTransient() as E }
                val res = entities.map { db.delete(it.id).second }
                launch {
                    events.incoming.send(EntityDeleteEvent(trans))
                }
                res
                }
            }
        }

        private fun <K : Any> DB.delete(id: K): Pair<MutableMap<String, Any?>, K> = runBlocking {
            tx {
            if (id !in bucket) Try.raise<Pair<MutableMap<String, Any?>, K>>(DataStoreError.EntityError.EntityNotFound(meta, id, this@JvmMemoryDataStore).cast())
                bucket[id]?.let { entityMap ->
                    entityMap[INCOMING]?.let {
                    if (it is Map<*, *>) {
                        it.values.forEach {
                            (it as? Map<*, *>)?.let {
                                (it as? Map<*, *>)?.let {
                                    it.values.forEach {
                                        (it as? List<*>)?.let {
                                            if (it.isNotEmpty()) throw IllegalStateException("${entityMap[TYPE]}($id) has incoming relations $it")
                                        }
                                    }
                                }
                            }
                        }
                    }
                    }
                    entityMap[RELATIONS]?.let {
                    if (it is Map<*, *>) {
                        @Suppress("UNCHECKED_CAST")
                        val map = it as Map<String, List<EntityTrace>>
                        map.forEach { entry ->
                            entry.value.forEach {
                                val type = entityMap[TYPE]!!
                                tm.add(
                                        RemoveRelationTransactionJvm.Transient(this@JvmMemoryDataStore, null, null,
                                                OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, entry.key, EntityTrace(type.toString(), id, this@JvmMemoryDataStore.name), it)
                                ).getOrElse { throw it }.second.run { runBlocking { await() } }.getOrElse { throw it }
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
        }

        override fun version(id: Any): Long = db.atomicLong(versionName(id)).open().get()

        @Suppress("UNUSED_PARAMETER")
        override fun index(k: Any, values: Map<String, Any?>) {

        }

        private fun <E : KIEntity<K>, K : Any> new(e: MutableMap.MutableEntry<Any, MutableMap<String, Any?>>): E = run {
            val type = e.value[TYPE]!!
            val meta1 = metaProvider.meta(type.toString())!!
            @Suppress("UNCHECKED_CAST")
            meta1.new(this@JvmMemoryDataStore, e.key as K) as E
        }

        @Suppress("UNCHECKED_CAST")
        override fun <E : KIEntity<K>, K : Any> baseQuery(query: Query<E, K>): Sequence<Pair<E, Map<String, Any?>>> = bucket.iterator().asSequence().map { entry -> new<E, K>(entry) to entry.value }.filter { query.f.matches(it.first) }


    }

    inner internal class SubTypeBucket(override val meta: KIEntityMeta, val parent: Bucket) : Bucket() {
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

        override fun set(k: Any, type: KIEntityMeta, values: Map<String, Any?>): Map<String, Any?> = parent.set(k, type, values)
        override fun set(k: Any, type: KIEntityMeta, version: Long, values: Map<String, Any?>): Map<String, Any?> = parent.set(k, type, version, values)

        override fun <E : KIEntity<K>, K : Any> create(id: K, entity: E): E = parent.create(id, entity)

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


private interface TX : CoroutineContext.Element {
    var tx: List<suspend DB.() -> Any?>

    companion object TX : CoroutineContext.Key<info.kinterest.datastores.jvm.memory.TX>
}


private fun TXList(): TX = object : AbstractCoroutineContextElement(TX), TX {
    override var tx: List<suspend DB.() -> Any?> = listOf()
}

//private val log = KLogging()
suspend fun <R> DB.doTx(tx: suspend DB.() -> R): R = try {
    coroutineContext[TX]!!.tx += tx
    val res = tx()
    coroutineContext[TX]!!.tx -= tx
    if (coroutineContext[TX]!!.tx.isEmpty()) {
        log.trace { "commit" }
        commit()
    } else {
        val txc = coroutineContext[TX]!!.tx
        log.trace { "${txc} so no commit" }
    }
    res
} catch (e: Exception) {
    coroutineContext[TX]!!.tx -= tx
    log.error(e) { }
    if (coroutineContext[TX]!!.tx.isEmpty()) {
        log.trace { "commit" }
        if (getStore() is StoreTx) rollback()
    } else {
        val txc = coroutineContext[TX]!!.tx
        log.trace { "${txc} so no commit" }
    }
    throw e
}

internal suspend fun <R> DB.tx(tx: suspend DB.() -> R): R = run {
    val ctx = if (coroutineContext[TX] == null) {
        coroutineContext + TXList()
    } else coroutineContext
    async(ctx) {
        doTx(tx)
    }.await()
}

