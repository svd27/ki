package info.kinterest.annotations.processor

import info.kinterest.Entity
import info.kinterest.GeneratedId
import info.kinterest.KIEntity
import info.kinterest.StorageTypes
import info.kinterest.annotations.processor.jvm.mem.TotalTestJvmMem
import info.kinterest.datastores.jvm.memory.JvmMemoryDataStore
import io.kotlintest.matchers.*
import io.kotlintest.specs.ShouldSpec
import io.kotlintest.specs.StringSpec
import java.io.File
import java.util.*
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.memberProperties

@Entity()
@StorageTypes(arrayOf("jvm.mem"))
interface TotalTest : KIEntity<UUID> {
    @get:GeneratedId()
    override val id: UUID
    val total : Int?
    var adapt : Boolean?

}

class FileSpec : ShouldSpec( {
    "correct file should be generated" should {
        File("./build") should exist()
        File("./build") should exist()
        File("./build/generated/source/kaptKotlin/test/TotalTestJvmMem.kt") should exist()
    }
    "the loaded class" should {
        val kc = TotalTestJvmMem::class
        "id should be available and of proper type" {
            val idProp = kc.memberProperties.filter { it.name == "id" }.firstOrNull()
            idProp shouldNotBe null
            idProp!!.returnType.classifier shouldEqual UUID::class
            val totalProp = kc.memberProperties.filter { it.name == "total" }.firstOrNull()
            totalProp shouldNotBe null
        }
    }
})