package info.kinterest.jvm.interest

import info.kinterest.*
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.map
import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.query.Query
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel

class InterestJvm<E : KIEntity<K>, K : Any>(override val id: Any, q: Query<E, K>, private val manager: InterestManager, private val subscriber: suspend (Iterable<InterestContainedEvent<Interest<E, K>, E, K>>) -> Unit) : Interest<E, K> {
    private var query: Query<E, K> = q
        set(value) {
            page = Page(paging, emptyList(), 0)
            runBlocking(pool) {
                page = query(value).getOrElse {
                    throw InterestError.InterestQueryError(this@InterestJvm, it.message ?: "", it)
                }
            }
            field = value
        }


    override var paging
        get() = query.page
        set(value) {
            query = Query(filter.cast(), ordering, value)
        }

    private var _page: Page<E, K> = Page(paging, emptyList(), 1)
    private var page: Page<E, K>
        get() = _page
        set(value) {
            _page = value
            fire(InterestPaged(this, value))
        }
    override val entities: Page<E, K> get() = page
    private val events: Channel<EntityEvent<E, K>> = Channel()
    @Suppress("UNCHECKED_CAST")
    private val filter = (q.f as EntityFilter.FilterWrapper<E, K>)

    override var ordering: Ordering<E, K>
        get() = query.ordering
        set(value) {
            query = Query(filter.cast(), value, paging)
        }

    init {
        launch(pool) {
            for (ev in events) {
                digest(ev)
            }
        }

        launch(pool) {
            query = q
            manager.qm.addFilter(query.f as EntityFilter.FilterWrapper<*, *>)
            manager.created(this@InterestJvm)
        }

        filter.listener = events
    }

    fun close() {
        filter.listener = null
        events.close()
        manager.qm.removeFilter(query.f as EntityFilter.FilterWrapper<*, *>)
    }

    private suspend fun digest(vararg evts: EntityEvent<E, K>) = page.let { p ->
        val ordering = this.ordering
        val paging = this.paging
        val oldsize = p.entites.size

        class Match {
            val added: MutableList<E> = mutableListOf()
            val removed: MutableList<E> = mutableListOf()
            val page = p.entites.toMutableList()
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

        val match = Match()
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
                    if (filter.matches(ev.entity)) {
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
        if (match.page.size < oldsize && oldsize >= paging.size) {
            match.page + query(Query(filter.cast(), ordering, Paging(paging.offset + match.page.size, paging.size - match.page.size))).map { it }.getOrElse { Page(paging, emptyList(), 0) }
        }
        match.page.sortWith(ordering.cast())
        val adds = match.added.map { match.page.indexOf(it) to it }.filter { it.first >= 0 }
        val rems = match.removed
        val pe = if (adds.isNotEmpty() || rems.isNotEmpty()) InterestPageChanged(this, rems, adds) else null
        val ae = if (match.addevts.isNotEmpty()) InterestEntitiesAdded(this, match.addevts) else null
        val re = if (match.remevts.isNotEmpty()) InterestEntitiesRemoved(this, match.remevts) else null
        this._page = Page(query.page, match.page, if (match.page.size >= query.page.size) 1 else 0)
        fire(ae, re, pe)
        Unit
    }


    private suspend fun query(query: Query<E, K>): Try<Page<E, K>> = manager.qm.query(query).getOrElse {
        throw InterestError.InterestQueryError(this, it.message ?: "", it)
    }.await()



    private fun fire(vararg evts: InterestContainedEvent<InterestJvm<E, K>, E, K>?) {
        val list = evts.filterNotNull()
        if (list.isNotEmpty()) launch(pool) {
            subscriber(list)
        }
    }


    override fun get(k: K): Deferred<Try<E>> = page.let { page ->
        val e = page.entites.filter { it.id == k }
        if (e.isEmpty()) manager.qm.retrieve<E, K>(filter.meta, listOf(k)).getOrElse { throw it }.map {
            it.map { it.first() }
        } else CompletableDeferred(Try { e.first() })
    }

    override fun get(idx: Int): E? = page.let { p ->
        p[idx]
    }

    companion object {
        val pool: CoroutineDispatcher = newFixedThreadPoolContext(8, "interests")
    }
}