package info.kinterest.annotations.processor

import info.kinterest.KIEntity
import info.kinterest.KIVersionedEntity
import info.kinterest.annotations.processor.jvm.TotalTestJvm
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.annotations.GeneratedId
import info.kinterest.jvm.annotations.StorageTypes
import info.kinterest.jvm.annotations.Versioned
import org.amshove.kluent.`should be`
import org.amshove.kluent.`should equal`
import org.amshove.kluent.`should not be`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.io.File
import java.util.*
import kotlin.reflect.full.memberProperties

@Entity()
@StorageTypes(["jvm.mem"])
interface TotalTest : KIEntity<UUID> {
    @get:GeneratedId()
    override val id: UUID
    val total : Int?
    var adapt : Boolean?

}

@Entity()
@StorageTypes(arrayOf("jvm.mem"))
@Versioned
interface VersionedTest : KIVersionedEntity<Long> {
    @get:GeneratedId()
    override val id: Long
    val total : Int?
    var adapt : Boolean?
}

object FileSpec : Spek( {
    given("a generator") {
        val root = File(".")
        println(root.absolutePath)
        val file = File("./build/generated/source/kaptKotlin/test/TotalTestJvm.kt")
        on("a kapt run") {
            it("the file should exist") {
                file.exists() `should be` true
            }
        }
    }
    val kc = TotalTestJvm::class
    given("the class is loaded") {
        it("") {
            val idProp = TotalTestJvm::id
            idProp `should not be` null
            idProp.returnType.classifier `should equal` UUID::class
            val totalProp = kc.memberProperties.filter { it.name == "total" }.firstOrNull()
            totalProp `should not be` null
        }
    }
})
