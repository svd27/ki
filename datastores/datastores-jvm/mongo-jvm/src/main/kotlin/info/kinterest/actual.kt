package info.kinterest
import kotlin.reflect.KClass
import java.util.UUID

actual typealias UUID = UUID

actual typealias Klass<T> = KClass<T>

actual interface DataStore {
    actual val name: String
}