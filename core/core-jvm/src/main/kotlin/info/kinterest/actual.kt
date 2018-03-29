package info.kinterest

import info.kinterest.jvm.filter.KIFilter
import kotlinx.coroutines.experimental.Deferred
import kotlin.reflect.KClass
import java.util.UUID

actual typealias UUID = UUID
actual typealias Klass<T> = KClass<T>


actual interface DataStore {
    actual val name : String
    fun<K> query(f:KIFilter<K>) : Try<Deferred<Try<Iterable<K>>>>
}