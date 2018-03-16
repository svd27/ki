package info.kinterest.annotations.processor

import info.kinterest.Entity
import info.kinterest.GeneratedId
import info.kinterest.KIEntity
import info.kinterest.StorageTypes
import io.kotlintest.matchers.exist
import io.kotlintest.matchers.should
import io.kotlintest.specs.StringSpec
import java.io.File
import java.util.*

@Entity()
@StorageTypes(arrayOf("jvm.mem"))
interface TotalTest : KIEntity<UUID> {
    @get:GeneratedId()
    override val id: UUID

    val total : Int?

}

class FileSpec : StringSpec( {
    "correct file should be generated" {
        File("./build") should exist()
        File("./build/generated/source/kaptKotlin/test/TotalTestJvmMem.kt") should exist()
    }
})