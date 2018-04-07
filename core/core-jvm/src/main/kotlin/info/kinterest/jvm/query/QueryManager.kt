package info.kinterest.jvm.query

import info.kinterest.DataStore
import info.kinterest.DataStoreEvent
import info.kinterest.KIEntity
import info.kinterest.QueryError
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.meta.KIEntityMeta
import info.kinterest.paging.Page
import info.kinterest.query.Query
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.channels.Channel

interface QueryManager {
    val chDataStores: Channel<DataStoreEvent>
    val stores: Set<DataStoreFacade>
    fun <E : KIEntity<K>, K : Any> query(q: Query<E, K>): Try<Deferred<Try<Page<E, K>>>> = Try {
        val dss = if (q.ds == Query.ALL) stores else q.ds.mapNotNull { ads -> stores.filter { it.name == ads.name }.firstOrNull() }
        when {
            dss.isEmpty() -> throw QueryError(q, "no DataStores found to query")
            dss.size == 1 -> dss.first().query(q).getOrElse { throw it }
            else -> dss.first().query(q).getOrElse { throw it }
        }
    }

    fun <E : KIEntity<K>, K : Any> retrieve(meta: KIEntityMeta, ids: Iterable<K>, stores: Set<DataStore> = Query.ALL): Try<Deferred<Try<Iterable<E>>>>
}


interface RemoteQueryManager : QueryManager {
    var _stores: MutableSet<DataStoreFacade>
    override val stores: Set<DataStoreFacade>
        get() = _stores.toSet()

}