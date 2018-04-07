package info.kinterest

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime

actual typealias LocalDate = LocalDate
actual typealias LocalDateTime = LocalDateTime
actual typealias OffsetDateTime = OffsetDateTime

actual typealias UUID = java.util.UUID
actual interface DataStore {
    actual val name: String
}