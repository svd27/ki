package info.kinterest.jvm.query

import info.kinterest.*
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Try
import info.kinterest.functional.getOrDefault
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.filter.tree.FilterTree
import info.kinterest.meta.KIEntityMeta
import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.query.Query
import info.kinterest.query.QueryManager
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.selects.select
import mu.KLogging


class QueryManagerJvm(val filterTree: FilterTree) : QueryManager {
    var _stores: Set<DataStoreFacade> = setOf()
    override val stores: Set<DataStoreFacade>
        get() = _stores
    val dataStores = Channel<DataStoreEvent>()

    val pool: CoroutineDispatcher = newFixedThreadPoolContext(4, "${QueryManagerJvm::class.simpleName}")

    init {
        launch(pool) {
            for (ev in dataStores) {
                when (ev) {
                    is StoreReady -> {
                        val ds = ev.ds
                        if (ds is DataStoreFacade) _stores += ds else throw QueryManagerGenericError(this@QueryManagerJvm, "wrong type of DataStore $ds")
                    }
                    is StoreDown -> {
                        val ds = ev.ds
                        if (ds is DataStoreFacade) _stores -= ds else throw QueryManagerGenericError(this@QueryManagerJvm, "wrong type of DataStore $ds")
                    }
                }
            }
        }
    }

    fun addFilter(f: EntityFilter.FilterWrapper<*, *>) {
        filterTree += f
    }

    fun removeFilter(f: EntityFilter.FilterWrapper<*, *>) {
        filterTree -= f
    }

    override fun <E : KIEntity<K>, K : Any> query(q: Query<E, K>): Try<Deferred<Try<Page<E, K>>>> = Try {
        val dss = if (q.ds == Query.ALL) stores else q.ds.mapNotNull { ads -> stores.filter { it.name == ads.name }.firstOrNull() }
        when {
            dss.isEmpty() -> throw QueryManagerRetrieveError(this, "no DataStores found to query")
            dss.size == 1 -> dss.first().query(q).getOrElse { throw it }
            else -> {
                val deferreds: List<Deferred<Try<Page<E, K>>>> = dss.map { it.query(Query(q.f, q.ordering, Paging(0, q.page.offset + q.page.size))) }.map { it.getOrElse { throw it } }
                determinePage(q, deferreds)

            }
        }
    }

    private fun <E : KIEntity<K>, K : Any> determinePage(q: Query<E, K>, defs: List<Deferred<Try<Page<E, K>>>>): Deferred<Try<Page<E, K>>> = run {
        var deferreds = defs
        async(pool) {
            Try {
                runBlocking {
                    var pages = listOf<Page<E, K>>()
                    while (deferreds.isNotEmpty()) {
                        pages += select<Page<E, K>> {
                            deferreds.map { d ->
                                d.onAwait {
                                    deferreds -= d
                                    it.getOrElse { throw it }
                                }
                            }
                        }
                    }
                    var entities: List<E> = listOf()
                    val el = pages.map { it.entites.toMutableList() }.toMutableList()
                    var off = 0
                    logger.debug { "el before drop of ${q.page.offset} $el" }
                    while (el.isNotEmpty() && off < q.page.offset) {
                        el.minAndDrop(q.ordering)
                        off++
                    }
                    logger.debug { "el after drop $el offset $off" }
                    while (el.isNotEmpty() && (entities.size < q.page.size) || q.page.size < 0) {
                        val minAndDrop = el.minAndDrop(q.ordering)
                        if (minAndDrop != null)
                            entities += minAndDrop
                    }
                    Page(q.page, entities, if (entities.size >= q.page.size) 1 else 0)
                }
            }
        }
    }

    fun <E : KIEntity<K>, K : Any> MutableList<MutableList<E>>.minAndDrop(ordering: Ordering<E, K>): E? = run {
        val rem = flatMap {
            if (it.isEmpty()) listOf(it) else listOf()
        }

        logger.trace { "removing $rem" }
        rem.forEach { remove(it) }

        var min: MutableList<E>? = null
        for (l in this) {
            val nm = l.minWith(ordering.cast())!!
            if (min == null || min.size == 0 || ordering.compare(nm, min.first()) < 0) {
                min = l
            }
        }
        logger.trace { "min is $min rest: $this" }
        min?.removeAt(0)
    }

    override fun <E : KIEntity<K>, K : Any> retrieve(meta: KIEntityMeta, ids: Iterable<K>, stores: Set<DataStore>): Try<Deferred<Try<Iterable<E>>>> = Try {
        val dss: Collection<DataStoreFacade> = if (stores == Query.ALL) this.stores else stores.mapNotNull { ads ->
            val dataStoreFacade = this.stores.filter { it.name == ads.name }.firstOrNull()
            dataStoreFacade
        }
        logger.debug { dss }
        when {
            dss.isEmpty() -> throw QueryManagerRetrieveError(this, "no DataStores found to query")
            dss.size == 1 -> dss.first().retrieveLenient<E, K>(meta, ids).getOrElse { throw it }
            else -> {
                var deferreds = dss.map { it.retrieveLenient<E, K>(meta, ids) }.map { it.getOrElse { throw it } }
                async(pool) {
                    Try {
                        runBlocking {
                            withTimeout(30000) {
                                var res: Iterable<E> = listOf()
                                while (deferreds.isNotEmpty()) {
                                    res += select<Iterable<E>> {
                                        deferreds.map {
                                            @Suppress("USELESS_CAST")
                                            (it as Deferred<Try<Iterable<E>>>).onAwait { res ->
                                                deferreds -= it
                                                logger.debug { "res: ${res.getOrDefault { null }} deferreds: $deferreds" }
                                                res.getOrElse { throw it }
                                            }
                                        }
                                    }
                                }
                                res
                            }
                        }
                    }
                }
            }
        }
    }

    companion object : KLogging()
}