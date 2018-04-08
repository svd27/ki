package info.kinterest.datastores

import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.functional.Try
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page
import info.kinterest.query.Query
import kotlinx.coroutines.experimental.CompletableDeferred
import kotlinx.coroutines.experimental.Deferred


interface DataStoreFacade : DataStore {
    override val name: String
    fun <K : Any> version(type: KIEntityMeta, id: K): Any
    fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<Page<E, K>>>>
    fun <E : KIEntity<K>, K : Any> retrieve(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>>
    fun <E : KIEntity<K>, K : Any> retrieveLenient(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>>
    fun <E : KIEntity<K>, K : Any> create(type: KIEntityMeta, entities: Iterable<E>): Try<Deferred<Try<Iterable<E>>>>
    fun <E : KIEntity<K>, K : Any> delete(type: KIEntityMeta, entities: Iterable<E>): Try<Deferred<Try<Iterable<K>>>>
    fun getValues(type: KIEntityMeta, id: Any): Deferred<Try<Map<String, Any?>?>>
    fun getValues(type: KIEntityMeta, id: Any, vararg props: KIProperty<*>): Deferred<Try<Map<String, Any?>?>>
    fun getValues(type: KIEntityMeta, id: Any, props: Iterable<KIProperty<*>>): Deferred<Try<Map<String, Any?>?>>
    fun setValues(type: KIEntityMeta, id: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>>
    fun setValues(type: KIEntityMeta, id: Any, version: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>>
}

sealed class QueryMessage
data class QueryMsg(val q: Query<*, *>, val id: CompletableDeferred<Pair<Boolean, Long>>) : QueryMessage()
sealed class QueryResult : QueryMessage()
data class QueryTrySuccess(val id: Long, val page: Page<*, *>) : QueryResult()
data class QueryTryFailure(val id: Long) : QueryResult()



