package info.kinterest.annotations.processor

import info.kinterest.annotations.Entity
import info.kinterest.annotations.GeneratedId
import info.kinterest.KIEntity
import info.kinterest.annotations.StorageTypes
import info.kinterest.annotations.Versioned
import info.kinterest.annotations.processor.jvm.mem.TotalTestJvmMem
import info.kinterest.annotations.processor.jvm.mem.VersionedTestJvmMem
import io.kotlintest.matchers.*
import io.kotlintest.specs.ShouldSpec
import java.io.File
import java.util.*
import kotlin.reflect.full.memberProperties

@Entity()
@StorageTypes(arrayOf("jvm.mem"))
interface TotalTest : KIEntity<UUID> {
    @get:GeneratedId()
    override val id: UUID
    val total : Int?
    var adapt : Boolean?

}

@Entity()
@StorageTypes(arrayOf("jvm.mem"))
@Versioned
interface VersionedTest : KIEntity<Long> {
    @get:GeneratedId()
    override val id: Long
    val total : Int?
    var adapt : Boolean?

}

class FileSpec : ShouldSpec( {
    "correct file should be generated" should {
        File("./build") should exist()
        File("./build/generated/source/kaptKotlin/test/TotalTestJvmMem.kt") should exist()
    }
    "the loaded class" should {
        val kc = TotalTestJvmMem::class
        "id should be available and of proper type" {
            val idProp = TotalTestJvmMem::id
            idProp shouldNotBe null
            idProp.returnType.classifier shouldEqual UUID::class
            val totalProp = kc.memberProperties.filter { it.name == "total" }.firstOrNull()
            totalProp shouldNotBe null
        }
    }
})

class VersionedSpec() : ShouldSpec({
    "the class " should {
        val kc = VersionedTestJvmMem::class
        kc shouldNotBe null
    }
})