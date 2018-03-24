package info.kinterest

import kotlin.reflect.KClass
actual typealias Class<T> = KClass<T>
actual interface DataStore {
    actual val name: String
}