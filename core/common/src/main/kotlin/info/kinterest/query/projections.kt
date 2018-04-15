package info.kinterest.query

import info.kinterest.*
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.getOrElse
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.sorting.Ordering

sealed class Projection<E : KIEntity<K>, K : Any>(val name: String, var parent: Projection<E, K>? = null) {
    open fun nameMe(prefix: String, name: String, postFix: String): String = "$prefix$name$postFix"
    open fun adapt(ds: Iterable<DataStoreFacade>): Projection<E, K> = this
    abstract fun combine(results: Iterable<ProjectionResult<E, K>>): ProjectionResult<E, K>
}

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
}

class SumProjection<E : KIEntity<K>, K : Any, V : Number>(val property: KIProperty<V>, parent: Projection<E, K>?) : Projection<E, K>(parent?.nameMe("count(", "", ")")
        ?: "count()", parent) {


    override fun combine(results: Iterable<ProjectionResult<E, K>>): ProjectionResult<E, K> =
            SumProjectionResult(this,
                    results.filterIsInstance<SumProjectionResult<E, K, V>>().map { it.sum }.reduce { n1, n2 -> add(n1, n2) })

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
    }
}

sealed class ProjectionResult<E : KIEntity<K>, K : Any>(open val projection: Projection<E, K>) {
    val name
        get() = projection.name

    abstract fun <I : Interest<E, K>> digest(i: I, evts: Iterable<EntityEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit): ProjectionResult<E, K>
}

class ReloadProjectionResult<E : KIEntity<K>, K : Any>(projection: Projection<E, K>) : ProjectionResult<E, K>(projection) {
    override fun <I : Interest<E, K>> digest(i: I, evts: Iterable<EntityEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit): ProjectionResult<E, K> = this
}


class EntityProjectionResult<E : KIEntity<K>, K : Any>(override val projection: EntityProjection<E, K>, var page: Page<E, K>) : ProjectionResult<E, K>(projection) {
    override fun <I : Interest<E, K>> digest(i: I, evts: Iterable<EntityEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit): ProjectionResult<E, K> = run {
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
                is EntityCreateEvent -> {
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
                is EntityDeleteEvent -> {
                    ev.entities.forEach { entity ->
                        if (entity in page) {
                            match - entity
                        }
                        match.evtRem(entity)
                    }
                }
                is EntityUpdatedEvent -> {
                    //we know the event is relevant else the filter wouldnt have called us
                    //so no need to call filter,wants again
                    if (i.query.f.matches(ev.entity)) {
                        if ((ordering == Ordering.NATURAL && match.page.size < paging.size) || ordering.isIn(ev.entity, page.first() to page.last())) {
                            match + ev.entity
                        }
                        match.evtAdd(ev.entity)
                    } else {
                        if (ev.entity in page) match - ev.entity
                        match.evtRem(ev.entity)
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
}

class SumProjectionResult<E : KIEntity<K>, K : Any, V : Number>(override val projection: SumProjection<E, K, V>, val sum: V) : ProjectionResult<E, K>(projection) {
    override fun <I : Interest<E, K>> digest(i: I, evts: Iterable<EntityEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit): ProjectionResult<E, K> = run {
        val el = evts.any { e ->
            when (e) {
                is EntityCreateEvent -> true
                is EntityDeleteEvent -> true
                is EntityUpdatedEvent -> {
                    if (e.history(projection.property).count() > 0) {
                        true
                    } else false
                }
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