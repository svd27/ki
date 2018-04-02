package info.kinterest

import info.kinterest.functional.Either
import info.kinterest.functional.Try
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.query.Query
import kotlinx.coroutines.experimental.Deferred
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.reflect.KClass

actual typealias UUID = UUID
actual typealias Klass<T> = KClass<T>

actual typealias LocalDate = LocalDate
actual typealias LocalDateTime = LocalDateTime
actual typealias OffsetDateTime = OffsetDateTime


actual interface DataStore {
    actual val name : String
    fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<Iterable<E>>>>
    fun <E : KIEntity<K>, K : Any> query(type: KIEntityMeta, f: EntityFilter<E, K>): Try<Deferred<Try<Iterable<K>>>>
    fun <E : KIEntity<K>, K : Any> retrieve(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>>
    fun <K : Any> create(type: KIEntityMeta, entities: Iterable<Pair<K, Map<String, Any?>>>): Try<Deferred<Try<Iterable<K>>>>
    fun <K : Any> delete(type: KIEntityMeta, entities: Iterable<K>): Try<Deferred<Either<DataStoreError, Iterable<K>>>>
    fun getValues(type: KIEntityMeta, id: Any): Map<String, Any?>?
    fun getValues(type: KIEntityMeta, id: Any, vararg props: KIProperty<*>): Map<String, Any?>?
    fun getValues(type: KIEntityMeta, id: Any, props: Iterable<KIProperty<*>>): Map<String, Any?>?
    fun setValues(type: KIEntityMeta, id: Any, values: Map<KIProperty<*>, Any?>)
}
