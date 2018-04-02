package info.kinterest.jvm.datastores

import info.kinterest.DataStore
import info.kinterest.DataStoreError
import info.kinterest.KIEntity
import info.kinterest.functional.Either
import info.kinterest.functional.Try
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page
import info.kinterest.query.Query
import kotlinx.coroutines.experimental.Deferred

interface DataStoreFacade : DataStore {
    override val name: String
    fun <E : KIEntity<K>, K : Any> query(type: KIEntityMeta, f: EntityFilter<E, K>): Try<Deferred<Try<Iterable<K>>>>
    fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<Page<E, K>>>>
    fun <E : KIEntity<K>, K : Any> retrieve(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>>
    fun <K : Any> create(type: KIEntityMeta, entities: Iterable<Pair<K, Map<String, Any?>>>): Try<Deferred<Try<Iterable<K>>>>
    fun <K : Any> delete(type: KIEntityMeta, entities: Iterable<K>): Try<Deferred<Either<DataStoreError, Iterable<K>>>>
    fun getValues(type: KIEntityMeta, id: Any): Deferred<Try<Map<String, Any?>?>>
    fun getValues(type: KIEntityMeta, id: Any, vararg props: KIProperty<*>): Deferred<Try<Map<String, Any?>?>>
    fun getValues(type: KIEntityMeta, id: Any, props: Iterable<KIProperty<*>>): Deferred<Try<Map<String, Any?>?>>
    fun setValues(type: KIEntityMeta, id: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>>
    fun setValues(type: KIEntityMeta, id: Any, version: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>>
}