package info.kinterest.datastores.jvm.memory

import arrow.data.getOrDefault
import arrow.data.getOrElse
import info.kinterest.KIEntity
import info.kinterest.annotations.Entity
import info.kinterest.annotations.StorageTypes
import info.kinterest.cast
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.datastores.jvm.memory.jvm.mem.TestRootJvmMem
import io.kotlintest.matchers.beOfType
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldNotBe
import io.kotlintest.specs.WordSpec
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import java.util.concurrent.TimeUnit
import arrow.syntax.*

@Entity
@StorageTypes(["jvm.mem"])
interface TestRoot : KIEntity<String> {
    override val id : String
    val name : String
    var online : Boolean?
}

class TestJvmMem : WordSpec({
    val fac = DataStoreFactoryProvider()

    val cfg = object : DataStoreConfig {
        override val name: String
            get() = "test"
        override val type: String
            get() = "jvm.mem"
        override val config: Map<String, Any?>
            get() = emptyMap()
    }
    val ds = fac.factories[cfg.type]!!.create(cfg)
     "a datastore" should {
         "have a proper type" {
             println("ds $ds")
             ds should beOfType<JvmMemoryDataStore>()
             ds.name shouldBe "test"
         }
         "create an entity" {
             val kc = TestRootJvmMem::class
             kc.simpleName shouldBe "TestRootJvmMem"
             ds should beOfType<JvmMemoryDataStore>()
             val mem = ds.cast<JvmMemoryDataStore>()
             val k = mem.create(TestRoot::class, "a", mapOf("name" to "aname"))
             k.failed() shouldNotBe true
             val deferred = k.getOrDefault { null }
             deferred shouldNotBe null
             println("deferred: $deferred")
             val tryk = runBlocking { deferred?.await()  }
             println("tryk = $tryk")
             if(tryk?.isFailure() == true) {
                 tryk.getOrElse { t ->  log.error { t }; ""}
             }
             tryk?.isSuccess() shouldBe true

             tryk?.map { key -> println("k: $key"); key}
             val da = mem.retrieve<TestRoot,String>("a")
             val atry = runBlocking {
                 da.await()
             }
             atry.isSuccess() shouldBe true
             val e = atry.getOrElse { null}!!
             e.id shouldBe "a"
             e.name shouldBe "aname"
             e.online shouldBe null
             println("e: ${e.name} ${e.online}")
             e.online = true

             runBlocking { delay(200, TimeUnit.MILLISECONDS) }
             e.online shouldBe true
             log.debug { "second map ${e.online}" }
         }
     }
})