package info.kinterest.datastores.jvm.memory

import info.kinterest.*
import info.kinterest.annotations.Entity
import info.kinterest.annotations.StorageTypes
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.datastores.jvm.memory.jvm.mem.TestRootJvmMem
import info.kinterest.jvm.DataStoreError
import info.kinterest.jvm.KIJvmEntity
import io.kotlintest.matchers.beOfType
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldNotBe
import io.kotlintest.specs.WordSpec
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import java.util.concurrent.TimeUnit

@Entity
@StorageTypes(["jvm.mem"])
interface TestRoot : KIEntity<String> {
    override val id : String
    val name : String
    var online : Boolean?
}

@Entity
@StorageTypes(["jvm.mem"])
@info.kinterest.annotations.Versioned
interface TestVersioned : KIEntity<Long> {
    override val id : Long
    val name : String
    var online : Boolean?
    var someone : String
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
             k.isFailure shouldNotBe true
             val deferred = k.getOrDefault { null }
             deferred shouldNotBe null
             println("deferred: $deferred")
             val tryk = runBlocking { deferred?.await()  }
             println("tryk = $tryk")
             if(tryk?.isFailure == true) {
                 tryk.getOrElse { t ->  log.error { t }; ""}
             }
             tryk?.isSuccess shouldBe true

             tryk?.map { key -> println("k: $key"); key}
             val da = mem.retrieve<TestRoot,String>("a")
             val atry = runBlocking {
                 da.await()
             }
             atry.isSuccess shouldBe true
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

class TestVersion : WordSpec({
    val fac = DataStoreFactoryProvider()

    val cfg = object : DataStoreConfig {
        override val name: String
            get() = "test"
        override val type: String
            get() = "jvm.mem"
        override val config: Map<String, Any?>
            get() = emptyMap()
    }

    val ds = fac.factories[cfg.type]!!.create(cfg);

    "creating an entity" should {
        val mem = ds.cast<JvmMemoryDataStore>()
        val tryDefer = mem.create(TestVersioned::class, 0.toLong(), mapOf("name" to "aname", "someone" to "not me"))
        val kdefer = tryDefer.getOrElse { null }!!
        val tryk = runBlocking { kdefer.await() }
        "be successfull" {
            tryDefer.isSuccess shouldBe true
            tryk.getOrElse { log.error(it) { it } }
            tryk.isSuccess shouldBe true
        }


        val k = tryk.getOrElse { null }!!

        val versionInitial = mem.version<TestVersioned, Long>(k)
        "have an initial version of 0" {
            log.info { "<><><> ${mem.version<TestVersioned, Long>(k)}" }
            versionInitial shouldBe 0.toLong()
        }
        val retrDeferred = mem.retrieve<TestVersioned, Long>(k)
        val tryVersioned = runBlocking { retrDeferred.await() }
        val entity = tryVersioned.getOrDefault { null }

        "retrieve a proper entity" {
            entity shouldNotBe null
        }

        entity?.someone = "not me, but someone else"
        runBlocking { delay(100, TimeUnit.MILLISECONDS) }

        "allow setting a property" {
            entity?.someone shouldBe "not me, but someone else"
        }


        val versionInc = entity?.cast<Versioned<Long>>()?._version
        "version should be increased" {
            versionInc shouldBe 1.toLong()
        }


        val deferSet = mem.setProp<TestVersioned, Long, String>(entity!!.id, entity.cast<KIJvmEntity<*, *>>()._meta["someone"]!!.cast(), "oh no", 0)
        val trySet = runBlocking { deferSet.await() }
        val ex = trySet.toEither().swap().getOrElse { null }!!
        "fail on trying a change with the wrong version" {
            trySet.getOrElse { log.trace(it) { } }
            trySet.isFailure shouldBe true
            log.trace(ex) { }
            ex shouldBe beOfType<DataStoreError.OptimisticLockException>()
        }

        val optimisticLockException = ex.cast<DataStoreError.OptimisticLockException>()

        optimisticLockException.ds shouldBe mem
        optimisticLockException.kc shouldBe entity.cast<KIJvmEntity<*,*>>()._me
        optimisticLockException.key shouldBe k

    }
})