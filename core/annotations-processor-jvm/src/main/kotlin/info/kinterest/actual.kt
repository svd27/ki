package info.kinterest

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.reflect.KClass

actual typealias LocalDate = LocalDate
actual typealias LocalDateTime = LocalDateTime
actual typealias OffsetDateTime = OffsetDateTime


actual typealias UUID = UUID
actual typealias Klass<T> = KClass<T>

actual interface DataStore {
    actual val name: String
}