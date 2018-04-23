package info.kinterest.jvm.query

import info.kinterest.*
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Try
import info.kinterest.functional.getOrDefault
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.filter.tree.FilterTree
import info.kinterest.meta.KIEntityMeta
import info.kinterest.query.Query
import info.kinterest.query.QueryManager
import info.kinterest.query.QueryResult
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.selects.select
import mu.KLogging


class QueryManagerJvm(val filterTree: FilterTree) : QueryManager {
    var _stores: Set<DataStoreFacade> = setOf()
    override val stores: Set<DataStoreFacade>
        get() = _stores
    val dataStores = Channel<DataStoreEvent>()

    override fun storesFor(meta: KIEntityMeta): Set<DataStoreFacade> = stores

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

    fun addFilter(f: EntityFilter.LiveFilterWrapper<*, *>) {
        filterTree += f
    }

    fun removeFilter(f: EntityFilter.LiveFilterWrapper<*, *>) {
        filterTree -= f
    }

    override fun <E : KIEntity<K>, K : Any> query(q: Query<E, K>): Try<Deferred<Try<QueryResult<E, K>>>> = runBlocking { q.query(this@QueryManagerJvm) }




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