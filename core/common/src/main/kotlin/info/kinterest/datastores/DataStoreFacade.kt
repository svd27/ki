package info.kinterest.datastores

import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.MetaProvider
import info.kinterest.functional.Try
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.meta.KIRelationProperty
import info.kinterest.meta.Relation
import info.kinterest.query.Query
import info.kinterest.query.QueryManager
import info.kinterest.query.QueryResult
import kotlinx.coroutines.experimental.CompletableDeferred
import kotlinx.coroutines.experimental.Deferred


abstract class DataStoreFacade(name: String) : DataStore(name) {
    abstract val qm: QueryManager
    abstract val metaProvider: MetaProvider
    abstract fun <K : Any> version(type: KIEntityMeta, id: K): Any
    abstract fun <E : KIEntity<K>, K : Any> querySync(query: Query<E, K>): Try<QueryResult<E, K>>
    abstract fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<QueryResult<E, K>>>>
    abstract fun <E : KIEntity<K>, K : Any> retrieve(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>>
    abstract fun <E : KIEntity<K>, K : Any> retrieveLenient(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>>
    abstract fun <E : KIEntity<K>, K : Any> create(type: KIEntityMeta, entities: Iterable<E>): Try<Deferred<Try<Iterable<E>>>>
    abstract fun <E : KIEntity<K>, K : Any> delete(type: KIEntityMeta, entities: Iterable<E>): Try<Deferred<Try<Iterable<K>>>>
    abstract fun getValues(type: KIEntityMeta, id: Any): Deferred<Try<Map<String, Any?>?>>
    abstract fun getValues(type: KIEntityMeta, id: Any, vararg props: KIProperty<*>): Deferred<Try<Map<String, Any?>?>>
    abstract fun getValues(type: KIEntityMeta, id: Any, props: Iterable<KIProperty<*>>): Deferred<Try<Map<String, Any?>?>>
    abstract fun setValues(type: KIEntityMeta, id: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>>
    abstract fun setValues(type: KIEntityMeta, id: Any, version: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>>
    abstract fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> addRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>>
    abstract fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> removeRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>>
    fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> replaceRelation(rel: KIRelationProperty, source: S, target: T): Try<Deferred<Try<Boolean>>> = TODO()
    abstract fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> getRelations(rel: KIRelationProperty, source: S): Try<Deferred<Try<Iterable<T>>>>
    abstract fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> getRelationsSync(rel: KIRelationProperty, source: S): Try<Iterable<T>>
    abstract fun <T : KIEntity<L>, L : Any, S : KIEntity<K>, K : Any> bookRelationSync(rel: Relation<S, T, K, L>): Try<Boolean>
    abstract fun <T : KIEntity<L>, L : Any, S : KIEntity<K>, K : Any> unbookRelationSync(rel: Relation<S, T, K, L>): Try<Boolean>
    abstract fun getBookedRelationsSync(rel: KIRelationProperty, entity: KIEntity<Any>, sourceMeta: KIEntityMeta): Try<Iterable<IRelationTrace>>
}

interface IRelationTrace {
    val type: String
    val id: Any
    val ds: String
}

sealed class QueryMessage
data class QueryMsg(val q: Query<*, *>, val id: CompletableDeferred<Pair<Boolean, Long>>) : QueryMessage()
sealed class QueryResultMsg : QueryMessage()
data class QueryTrySuccess(val id: Long, val result: QueryResult<*, *>) : QueryResultMsg()
data class QueryTryFailure(val id: Long) : QueryResultMsg()



