package info.kinterest.query

import info.kinterest.*
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.filter.*
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.meta.KINumberProperty
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.CompletableDeferred
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.async

data class Path(val name: String, val parent: Path?) {
    val root: Path
        get() = if (parent == null) this else parent.root

    override fun toString(): String = if (parent == null) name else "$parent.$name"

    fun isSubPath(p: Path) = toString().commonPrefixWith(p.toString()).length == p.toString().length
}

fun <T, R> Deferred<T>.map(map: (T) -> R): Deferred<R> = async {
    map(await())
}

sealed class Projection<E : KIEntity<K>, K : Any>(val name: String, var parent: Projection<E, K>? = null) {
    open fun adapt(ds: Iterable<DataStoreFacade>): Projection<E, K> = this
    abstract fun combine(results: Iterable<ProjectionResult<E, K>>): ProjectionResult<E, K>
    val path: Path get() = parent?.let { Path(name, it.path) } ?: Path(name, null)
    open val amendFilter: Filter<E, K>? = null

    fun amendQuery(q: Query<E, K>) = amendFilter?.let {
        q.copy(f = q.f.and(it), projections = listOf(this))
    } ?: q.copy(projections = listOf(this))


    override fun toString(): String = "${this::class.simpleName}($name)"

    override fun equals(other: Any?): Boolean = if (other === this) true else {
        other is Projection<*, *> && other.path == path
    }

    override fun hashCode(): Int = path.hashCode()

    abstract fun clone(): Projection<E, K>
}

@Suppress("EqualsOrHashCode")
class EntityProjection<E : KIEntity<K>, K : Any>(var ordering: Ordering<E, K>, var paging: Paging, parent: Projection<E, K>? = null) : Projection<E, K>("entities", parent) {
    override fun adapt(ds: Iterable<DataStoreFacade>): Projection<E, K> = if (ds.count() < 2) this else EntityProjection(ordering, Paging(0, if (paging.size >= 0) paging.offset + paging.size else -1), parent)

    override fun combine(results: Iterable<ProjectionResult<E, K>>): ProjectionResult<E, K> = if (results.count() == 1) {
        EntityProjectionResult(this, results.filterIsInstance<EntityProjectionResult<E, K>>().first().page)
    } else {
        fun <E : KIEntity<K>, K : Any> MutableList<MutableList<E>>.minAndDrop(ordering: Ordering<E, K>): E? = run {
            val rem = flatMap {
                if (it.isEmpty()) listOf(it) else listOf()
            }

            rem.forEach { remove(it) }

            var min: MutableList<E>? = null
            for (l in this) {
                val nm = l.minWith(ordering.cast())!!
                if (min == null || min.size == 0 || ordering.compare(nm, min.first()) < 0) {
                    min = l
                }
            }
            min?.removeAt(0)
        }

        val pages = results.filterIsInstance<EntityProjectionResult<E, K>>().map { it.page.entities.toMutableList() }.toMutableList()
        var offset = 0
        while (offset < paging.offset && pages.isNotEmpty()) {
            offset++
            pages.minAndDrop(ordering)
        }
        var entities: List<E> = listOf()
        while (pages.isNotEmpty() && (entities.size < paging.size) || paging.size < 0) {
            val minAndDrop = pages.minAndDrop(ordering)
            if (minAndDrop != null)
                entities += minAndDrop
        }
        EntityProjectionResult(this, Page(paging, entities, if (entities.size >= paging.size) 1 else 0))
    }

    override fun equals(other: Any?): Boolean = super.equals(other) && other is EntityProjection<*, *>

    override fun clone(): Projection<E, K> = EntityProjection(ordering, paging, parent)
}

@Suppress("EqualsOrHashCode")
sealed class ValueProjection<E : KIEntity<K>, K : Any>(open val property: KIProperty<Any>, name: String, parent: Projection<E, K>?) : Projection<E, K>(name, parent) {
    override fun equals(other: Any?): Boolean = super.equals(other) && other is ValueProjection<*, *>
}

class CountProjection<E : KIEntity<K>, K : Any>(property: KIProperty<Any>, parent: Projection<E, K>? = null) : ValueProjection<E, K>(property, "count(${property.name})", parent) {
    override fun combine(results: Iterable<ProjectionResult<E, K>>): ProjectionResult<E, K> =
            CountProjectionResult(this,
                    results.filterIsInstance<CountProjectionResult<E, K>>().map { it.count }.reduce { n1, n2 -> n1 + n2 })

    override fun clone(): Projection<E, K> = CountProjection(property, parent)
}

sealed class ScalarProjection<E : KIEntity<K>, K : Any, S : Number>(property: KIProperty<S>, name: String, parent: Projection<E, K>?) : ValueProjection<E, K>(property, name, parent) {
    companion object {
        @Suppress("IMPLICIT_CAST_TO_ANY", "UNCHECKED_CAST")
        fun <V : Number> add(v1: V, v2: V): V = when (v1) {
            is Byte -> v1 + v2.toByte()
            is Short -> v1 + v2.toShort()
            is Int -> v1 + v2.toInt()
            is Long -> v1 + v2.toLong()
            is Float -> v1 + v2.toFloat()
            is Double -> v1 + v2.toDouble()
            else -> throw Exception("Bad type ${v1::class}")
        } as V

        @Suppress("IMPLICIT_CAST_TO_ANY", "UNCHECKED_CAST", "unused")
        fun <V : Number> max(v1: V, v2: V): V = when (v1) {
            is Byte -> maxOf(v1, v2.toByte())
            is Short -> maxOf(v1, v2.toShort())
            is Int -> maxOf(v1, v2.toInt())
            is Long -> maxOf(v1, v2.toLong())
            is Float -> maxOf(v1, v2.toFloat())
            is Double -> maxOf(v1, v2.toDouble())
            else -> throw Exception("Bad type ${v1::class}")
        } as V
    }
}

class SumProjection<E : KIEntity<K>, K : Any, V : Number>(override val property: KINumberProperty<V>, parent: Projection<E, K>?) : ScalarProjection<E, K, V>(property, "sum(${property.name})", parent) {
    override fun clone(): Projection<E, K> = SumProjection(property, parent)

    override fun combine(results: Iterable<ProjectionResult<E, K>>): ProjectionResult<E, K> =
            ScalarProjectionResult(this,
                    results.filterIsInstance<ScalarProjectionResult<E, K, V>>().map { it.sum }.reduce { n1, n2 -> add(n1, n2) })
}

interface Discriminators<E : KIEntity<K>, K : Any, V : Any> {
    val name: String
    fun discriminatorFor(v: V?): Discriminator<E, K, V>
}

interface Discriminator<E : KIEntity<K>, K : Any, V : Any> {
    val name: String
    fun inside(v: V?): Boolean
    fun asFilter(): Filter<E, K>
}

class ProjectionBucket<E : KIEntity<K>, K : Any, B : Any>(
        val property: KIProperty<B>, val discriminator: Discriminator<E, K, B>, val bucket: BucketProjection<E, K, B>) :
        Projection<E, K>(discriminator.name, bucket) {
    override fun combine(results: Iterable<ProjectionResult<E, K>>): ProjectionBucketResult<E, K, B> = ProjectionBucketResult(results.filterIsInstance<ProjectionBucketResult<E, K, B>>().map { it.result }.reduce { macc, map ->
        macc.map { it.key to it.value.projection.combine(listOf(map[it.key]).filterNotNull()) }.toMap()
    }, this)

    override val amendFilter: Filter<E, K>?
        get() = discriminator.asFilter()

    override fun clone(): Projection<E, K> = ProjectionBucket(property, discriminator, bucket)
}


class BucketProjection<E : KIEntity<K>, K : Any, B : Any>(parent: Projection<E, K>?, val projections: List<Projection<E, K>>, val discriminators: Discriminators<E, K, B>, val property: KIProperty<B>) : Projection<E, K>(discriminators.name, parent) {
    override fun combine(results: Iterable<ProjectionResult<E, K>>): ProjectionResult<E, K> = results.filterIsInstance<BucketProjectionResult<E, K, B>>().reduce { b1, b2 ->
        BucketProjectionResult(b1.bucketProjection, b1.buckets.map {
            it.key to
                    it.value.projection.combine(listOf(it.value, b2.buckets[it.key]).filterNotNull())
        }.filterIsInstance<Pair<ProjectionBucket<E, K, B>, ProjectionBucketResult<E, K, B>>>().toMap())
    }

    override fun clone(): Projection<E, K> = BucketProjection(parent, projections, discriminators, property)
}

class ProjectionBucketResult<E : KIEntity<K>, K : Any, B : Any>(result: Map<Projection<E, K>, ProjectionResult<E, K>>, val bucket: ProjectionBucket<E, K, B>) : ProjectionResult<E, K>(bucket) {
    val result: Map<Projection<E, K>, ProjectionResult<E, K>> = result.map {
        val p = it.key.clone().apply { parent = this@ProjectionBucketResult.projection }
        p to it.value.clone(p)
    }.toMap()

    private fun relevant(ev: FilterEvent<E, K>): FilterEvent<E, K>? = when (ev) {
        is FilterCreateEvent<*, *> -> {
            val filtered = ev.entities.filter { bucket.discriminator.inside(it.getValue(bucket.property)) }
            @Suppress("UNCHECKED_CAST")
            if (filtered.isEmpty()) null else FilterCreateEvent<E, K>(filtered as Iterable<E>, ev.want, ev.filter)
        }
        is FilterDeleteEvent<*, *> -> {
            val filtered = ev.entities.filter { bucket.discriminator.inside(it.getValue(bucket.property)) }
            @Suppress("UNCHECKED_CAST")
            if (filtered.isEmpty()) null else FilterDeleteEvent<E, K>(filtered as Iterable<E>, ev.want, ev.filter)
        }
        is FilterUpdateEvent<*, *> -> {
            @Suppress("UNCHECKED_CAST")
            val want = bucket.discriminator.asFilter().wants(ev.upds as EntityUpdatedEvent<E, K>)
            if (want in setOf(FilterWant.OUTOUT, FilterWant.NONE)) null
            else ev
        }
        is FilterRelationEvent<*, *> -> {
            @Suppress("UNCHECKED_CAST")
            val want = bucket.discriminator.asFilter().wants(ev.upds as EntityRelationEvent<E, K, *, *>)
            if (want in setOf(FilterWant.OUTOUT, FilterWant.NONE)) null
            else ev
        }
    }

    override fun <I : Interest<E, K>> digest(i: I, evts: Iterable<FilterEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit): ProjectionResult<E, K> = run {
        val rel = evts.map { relevant(it) }.filterNotNull()
        if (rel.any { it is FilterCreateEvent || it is FilterDeleteEvent }) {
            events(listOf(ProjectionChanged(bucket)))
            ReloadProjectionResult(bucket)
        } else
            ProjectionBucketResult(result.map { it.key to it.value.digest(i, rel, events) }.toMap(), bucket)
    }


    override fun retrieve(path: Path, q: Query<E, K>, qm: QueryManager): Try<Deferred<Try<ProjectionResult<E, K>>>> = Try {
        if (path == bucket.path) CompletableDeferred(Try { this }) else {
            val cp = result.keys.firstOrNull {
                path.isSubPath(it.path)
            }
            if (cp != null)
                result[cp]?.let {
                    it.retrieve(path, cp.amendQuery(q), qm).getOrElse { throw it }
                } ?: throw QueryError(q, qm, "no projection $path found")
            else throw QueryError(q, qm, "no projection $path found")
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun clone(projection: Projection<E, K>): ProjectionResult<E, K> = ProjectionBucketResult(result, projection as ProjectionBucket<E, K, B>)
}

class BucketProjectionResult<E : KIEntity<K>, K : Any, B : Any>(val bucketProjection: BucketProjection<E, K, B>, buckets: Map<ProjectionBucket<E, K, B>, ProjectionResult<E, K>>) : ProjectionResult<E, K>(bucketProjection) {
    val buckets: Map<ProjectionBucket<E, K, B>, ProjectionResult<E, K>> = buckets.map {
        @Suppress("UNCHECKED_CAST")
        val p = it.key.clone().apply {
            parent = this@BucketProjectionResult.projection
        } as ProjectionBucket<E, K, B>
        @Suppress("UNCHECKED_CAST")
        p to it.value.clone(p)
    }.toMap()

    private fun relevant(ev: FilterEvent<E, K>): FilterEvent<E, K>? = when (ev) {
        is FilterCreateEvent<*, *> -> ev
        is FilterDeleteEvent<*, *> -> ev
        is FilterUpdateEvent<*, *> -> {
            val ue = ev as FilterUpdateEvent<E, K>
            if (ue.upds.history(bucketProjection.property).count() > 0) ev else null
        }
        is FilterRelationEvent<*, *> -> if (ev.upds.relations.any { it.rel == bucketProjection.property }) ev else null
    }

    override fun <I : Interest<E, K>> digest(i: I, evts: Iterable<FilterEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit): ProjectionResult<E, K> = run {
        val rel = evts.map { relevant(it) }.filterNotNull()
        if (rel.any { it is FilterCreateEvent<*, *> && it.entities.any { e -> buckets.keys.none { p -> p.discriminator.inside(e.getValue(p.property)) } } }) {
            events(listOf(ProjectionChanged(projection)))
            ReloadProjectionResult(bucketProjection)
        } else if (rel.any { ev ->
                    ev is FilterUpdateEvent<*, *> && ev.upds.history(bucketProjection.property).count() > 0 &&
                            buckets.keys.none { p -> p.discriminator.inside(ev.upds.history(bucketProjection.property).last()) }
                }) {
            events(listOf(ProjectionChanged(projection)))
            ReloadProjectionResult(bucketProjection)
        } else {
            BucketProjectionResult(
                    bucketProjection, buckets.map { it.key to it.value.digest(i, evts, events) }.toMap())
        }
    }


    override fun retrieve(path: Path, q: Query<E, K>, qm: QueryManager): Try<Deferred<Try<ProjectionResult<E, K>>>> = Try {
        if (path == bucketProjection.path) CompletableDeferred(Try { this }) else {
            val cp = buckets.keys.firstOrNull {
                path.isSubPath(it.path)
            }
            if (cp != null)
                buckets[cp]?.let {
                    it.retrieve(path, cp.amendQuery(q), qm).getOrElse { throw it }
                } ?: throw QueryError(q, qm, "no projection $path found")
            else throw QueryError(q, qm, "no projection $path found")
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun clone(projection: Projection<E, K>): ProjectionResult<E, K> = BucketProjectionResult(projection as BucketProjection<E, K, B>, buckets)
}


sealed class ProjectionResult<E : KIEntity<K>, K : Any>(open val projection: Projection<E, K>) {
    val name
        get() = projection.name

    abstract fun <I : Interest<E, K>> digest(i: I, evts: Iterable<FilterEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit): ProjectionResult<E, K>

    open fun retrieve(path: Path, q: Query<E, K>, qm: QueryManager): Try<Deferred<Try<ProjectionResult<E, K>>>> = Try {
        if (path == projection.path) CompletableDeferred(Try { this }) else DONTDOTHIS("in ${projection.path}: could not find $path ")
    }

    abstract fun clone(projection: Projection<E, K>): ProjectionResult<E, K>

    override fun toString(): String = "${this::class.simpleName}($name)"
}

class ReloadProjectionResult<E : KIEntity<K>, K : Any>(projection: Projection<E, K>) : ProjectionResult<E, K>(projection) {
    override fun <I : Interest<E, K>> digest(i: I, evts: Iterable<FilterEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit): ProjectionResult<E, K> = this
    override fun retrieve(path: Path, q: Query<E, K>, qm: QueryManager): Try<Deferred<Try<ProjectionResult<E, K>>>> = Try {
        async {
            val query = projection.amendQuery(q)
            val orElse = qm.query(query).getOrElse { throw it }
            val deferred = orElse.map { it.map { it.projections[projection]!!.retrieve(path, query, qm).getOrElse { throw it }.map { it.getOrElse { throw it } } } }


            val awit = deferred.await().getOrElse { throw it }.await()
            Try { awit }
        }
    }

    override fun clone(projection: Projection<E, K>): ProjectionResult<E, K> = ReloadProjectionResult(projection)
}


class EntityProjectionResult<E : KIEntity<K>, K : Any>(override val projection: EntityProjection<E, K>, var page: Page<E, K>) : ProjectionResult<E, K>(projection) {
    override fun <I : Interest<E, K>> digest(i: I, evts: Iterable<FilterEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit): ProjectionResult<E, K> = run {
        val ordering = projection.ordering
        val paging = projection.paging
        val oldsize = page.entities.size

        val match = object {
            val added: MutableList<E> = mutableListOf()
            val removed: MutableList<E> = mutableListOf()
            val page = this@EntityProjectionResult.page.entities.toMutableList()
            val addevts: MutableList<E> = mutableListOf()
            val remevts: MutableList<E> = mutableListOf()
            operator fun plus(e: E) {
                added += e
                removed -= e
                page += e
            }

            operator fun minus(e: E) {
                added -= e
                removed += e
                page -= e
            }

            fun evtAdd(e: E) {
                addevts += e
            }

            fun evtRem(e: E) {
                remevts += e
            }
        }


        val page = match.page

        evts.forEach { ev ->
            when (ev) {
                is FilterCreateEvent -> {
                    ev.entities.forEach { entity ->
                        when {
                            page.size < paging.size -> match + entity
                            ordering.isIn(entity, page.first() to page.last()) -> {
                                match + entity
                                match - page.last()
                            }
                        }
                        match.evtAdd(entity)
                    }
                }
                is FilterDeleteEvent -> {
                    ev.entities.forEach { entity ->
                        if (entity in page) {
                            match - entity
                        }
                        match.evtRem(entity)
                    }
                }
                is FilterUpdateEvent -> {
                    val upd = ev.upds
                    if (ev.want in setOf(FilterWant.OUTIN, FilterWant.ININ)) {
                        if ((ordering == Ordering.NATURAL && match.page.size < paging.size) || ordering.isIn(upd.entity, page.firstOrNull() to page.lastOrNull())) {
                            match + upd.entity
                        }
                        match.evtAdd(upd.entity)
                    } else if (ev.want in setOf(FilterWant.INOUT)) {
                        if (upd.entity in page) match - upd.entity
                        match.evtRem(upd.entity)
                    }
                }
            }
        }
        val dist = match.page.distinct()
        match.page.clear()
        match.page += dist
        if (match.page.size < oldsize && oldsize >= paging.size) {
            //ordering, Paging(paging.offset + match.page.size, paging.size - match.page.size))
            match.page + i.query(Query(i.query.f.cast(),
                    listOf(EntityProjection(projection.ordering,
                            Paging(paging.offset + match.page.size, paging.size - match.page.size), null)))).getOrElse { throw it }.projections.map { it.value }.filterIsInstance<EntityProjectionResult<E, K>>().first().page.entities
        }
        match.page.sortWith(ordering.cast())
        val adds = match.added.map { match.page.indexOf(it) to it }.filter { it.first >= 0 }
        val rems = match.removed
        val pe = if (adds.isNotEmpty() || rems.isNotEmpty()) ProjectionPageChanged(projection, rems, adds) else null
        val ae = if (match.addevts.isNotEmpty()) ProjectionEntitiesAdded(projection, match.addevts) else null
        val re = if (match.remevts.isNotEmpty()) ProjectionEntitiesRemoved(projection, match.remevts) else null
        this.page = Page(paging, match.page, if (match.page.size >= paging.size) 1 else 0)
        events(listOf(ae, re, pe).filterNotNull())
        this
    }

    override fun clone(projection: Projection<E, K>): ProjectionResult<E, K> = EntityProjectionResult(projection as EntityProjection<E, K>, page)
}

sealed class ValueProjectionResult<E : KIEntity<K>, K : Any, V : Any>(override val projection: ValueProjection<E, K>, val result: V) : ProjectionResult<E, K>(projection) {
    override fun <I : Interest<E, K>> digest(i: I, evts: Iterable<FilterEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit): ProjectionResult<E, K> = run {
        val el = evts.any { e ->
            when (e) {
                is FilterCreateEvent -> true
                is FilterDeleteEvent -> true
                is FilterUpdateEvent -> {
                    if (e.want in setOf(FilterWant.INOUT, FilterWant.OUTIN) || e.upds.history(projection.property).count() > 0) {
                        true
                    } else false
                }
                is FilterRelationEvent<*, *> -> if (e.upds.relations.first().rel == projection.property) true else false
            }
        }
        if (!el) {
            this
        } else {
            events(listOf(ProjectionChanged(projection)))
            ReloadProjectionResult(projection)
        }
    }
}

class CountProjectionResult<E : KIEntity<K>, K : Any>(projection: CountProjection<E, K>, val count: Long) : ValueProjectionResult<E, K, Long>(projection, count) {
    override fun clone(projection: Projection<E, K>): ProjectionResult<E, K> = CountProjectionResult(projection as CountProjection<E, K>, count)
}

class ScalarProjectionResult<E : KIEntity<K>, K : Any, V : Number>(override val projection: ScalarProjection<E, K, V>, val sum: V) : ValueProjectionResult<E, K, V>(projection, sum) {
    @Suppress("UNCHECKED_CAST")
    override fun clone(projection: Projection<E, K>): ProjectionResult<E, K> = ScalarProjectionResult(projection as ScalarProjection<E, K, V>, sum)
}