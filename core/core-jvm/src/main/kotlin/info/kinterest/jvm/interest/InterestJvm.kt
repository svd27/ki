package info.kinterest.jvm.interest

import info.kinterest.*
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.query.Query
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.runBlocking

class InterestJvm<E : KIEntity<K>, K : Any>(override val id: Any, q: Query<E, K>) : Interest<E, K> {
    private var page: MutableList<E> = mutableListOf()
        set(value) {
            field = value
            fire(InterestPaged(this, Page(paging, page, if (page.size >= paging.size) 1 else 0)))
        }
    override val entities: Iterable<E> get() = page.toList()
    private val events: Channel<EntityEvent<E, K>> = Channel()
    @Suppress("UNCHECKED_CAST")
    private val filter = (q.f as EntityFilter.FilterWrapper<E, K>)
    private var query: Query<E, K> = Query.NOQUERY.cast()
        set(value) {
            page.clear()
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
    override var ordering: Ordering<E, K>
        get() = query.ordering
        set(value) {
            query = Query(filter.cast(), value, paging)
        }

    init {
        query = q
        launch(pool) {
            for (ev in events) {
                digest(ev)
            }
        }

        filter.listener = events
    }

    private suspend fun digest(vararg evts: EntityEvent<E, K>) = page.let { p ->
        val ordering = this.ordering
        val paging = this.paging
        val oldsize = p.size

        class Match {
            val added: MutableList<E> = mutableListOf()
            val removed: MutableList<E> = mutableListOf()
            val page = p
            val addevts: MutableList<E> = mutableListOf()
            val remevts: MutableList<E> = mutableListOf()
            operator fun plus(e: E) {
                added + e
                removed - e
                page + e
            }

            operator fun minus(e: E) {
                added - e
                removed + e
                page - e
            }

            fun evtAdd(e: E) {
                addevts + e
            }

            fun evtRem(e: E) {
                remevts + e
            }
        }

        val match = Match()
        val page = match.page

        evts.forEach { ev ->
            when (ev) {
                is EntityCreateEvent -> {
                    when {
                        page.size < paging.size -> match + ev.entity
                        ordering.isIn(ev.entity, page.first() to page.last()) -> {
                            match + ev.entity
                            match - page.last()
                        }
                    }
                    match.evtAdd(ev.entity)
                }
                is EntityDeleteEvent -> {
                    if (ev.entity in page) {
                        match - ev.entity
                    }
                    match.evtRem(ev.entity)
                }
                is EntityUpdatedEvent -> {
                    //we know the event is relevant else the filter wouldnt have called us
                    //so no need to call filter,wants again
                    if (filter.matches(ev.entity)) {
                        if (ordering.isIn(ev.entity, page.first() to page.last())) {
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
            match.page + query(Query(filter.cast(), ordering, Paging(paging.offset + match.page.size, paging.size - match.page.size))).map { it }.getOrElse { mutableListOf() }
        }
        match.page.sortWith(ordering.cast())
        val adds = match.added.map { match.page.indexOf(it) to it }.filter { it.first < 0 }
        val rems = match.removed
        val pe = if (adds.isNotEmpty() || rems.isNotEmpty()) InterestPageChanged(this, rems, adds) else null
        val ae = if (match.addevts.isNotEmpty()) InterestEntitiesAdded(this, match.addevts) else null
        val re = if (match.remevts.isNotEmpty()) InterestEntitiesRemoved(this, match.remevts) else null
        fire(ae, re, pe)
        Unit
    }


    private suspend fun query(query: Query<E, K>): Try<MutableList<E>> = filter.ds.query(query).getOrElse {
        throw InterestError.InterestQueryError(this, it.message ?: "", it)
    }.await().map { list ->
        list.toMutableList()
    }


    private fun fire(vararg evts: InterestEvent<InterestJvm<E, K>, E, K>?) {
        val list = evts.filterNotNull()
        if (list.isNotEmpty()) launch(pool) {
            for (s in subscibers) {
                s(list)
            }
        }
    }

    private var subscibers: List<suspend (Iterable<InterestEvent<Interest<E, K>, E, K>>) -> Unit> = listOf()


    override fun addSubscriber(s: suspend (Iterable<InterestEvent<Interest<E, K>, E, K>>) -> Unit) {
        subscibers += s
    }

    override fun removeSubscriber(s: suspend (Iterable<InterestEvent<Interest<E, K>, E, K>>) -> Unit) {
        subscibers -= s
    }

    override fun get(k: K): E? = filter.ds.retrieve<E, K>(filter.meta, listOf(k)).map {
        runBlocking(pool) { it.await() }.getOrElse { throw it }.firstOrNull()
    }.getOrElse { throw it }

    override fun get(idx: Int): E? = page.let { p ->
        if (idx < 0 || idx >= p.size) null else p[idx]
    }

    companion object {
        val pool: CoroutineDispatcher = newFixedThreadPoolContext(4, "interests")
    }
}