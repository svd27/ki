package info.kinterest.query

import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Try
import info.kinterest.meta.KIEntityMeta
import kotlinx.coroutines.experimental.Deferred

interface QueryManager {
    val stores: Set<DataStoreFacade>

    fun storesFor(meta: KIEntityMeta): Set<DataStoreFacade>

    fun <E : KIEntity<K>, K : Any> query(q: Query<E, K>): Try<Deferred<Try<QueryResult<E, K>>>>

    fun <E : KIEntity<K>, K : Any> retrieve(meta: KIEntityMeta, ids: Iterable<K>, stores: Set<DataStore> = Query.ALL): Try<Deferred<Try<Iterable<E>>>>
}


interface RemoteQueryManager : QueryManager {
    var _stores: Set<DataStoreFacade>
    override val stores: Set<DataStoreFacade>
        get() = _stores.toSet()

}