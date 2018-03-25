package info.kinterest

import java.util.UUID
import kotlin.reflect.KClass

actual typealias UUID = java.util.UUID
actual typealias Klass<T> = KClass<T>
actual interface DataStore {
    actual val name: String
}