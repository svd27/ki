package info.kinterest.annotations.processor

import info.kinterest.annotations.Entity
import info.kinterest.annotations.GeneratedId
import info.kinterest.KIEntity
import info.kinterest.annotations.StorageTypes
import info.kinterest.annotations.Versioned
import info.kinterest.annotations.processor.jvm.mem.TotalTestJvmMem
import info.kinterest.annotations.processor.jvm.mem.VersionedTestJvmMem
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.io.File
import java.util.*
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.superclasses

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

object FileSpec : Spek( {
    given("a generator") {
        val root = File(".")
        println("${root.absolutePath}")
        val file = File("./build/generated/source/kaptKotlin/test/TotalTestJvmMem.kt")
        on("a kapt run") {
            it("the file should exist") {
                file.exists() `should be` true
            }
        }
    }
    val kc = TotalTestJvmMem::class
    given("the class is loaded") {
        it("") {
            val idProp = TotalTestJvmMem::id
            idProp `should not be` null
            idProp.returnType.classifier `should equal` UUID::class
            val totalProp = kc.memberProperties.filter { it.name == "total" }.firstOrNull()
            totalProp `should not be` null
        }
    }
})

object VersionedSpec : Spek({
    val kc = VersionedTestJvmMem::class
    given("the class") {
        on("checking the type") {
            it("should be versioned") {
                kc.superclasses.toList() `should contain` info.kinterest.Versioned::class
            }
        }
    }
})