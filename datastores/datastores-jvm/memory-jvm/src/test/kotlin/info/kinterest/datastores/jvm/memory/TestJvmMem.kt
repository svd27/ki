package info.kinterest.datastores.jvm.memory

import info.kinterest.*
import info.kinterest.annotations.Entity
import info.kinterest.annotations.StorageTypes
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.datastores.jvm.memory.jvm.mem.TestRootJvmMem
import info.kinterest.jvm.DataStoreError
import info.kinterest.jvm.KIJvmEntity
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
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


object TestJvmMem : Spek({
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
    given("a datasore") {
        on("its type") {
            it("should have the proper type and name") {
                ds `should be instance of` JvmMemoryDataStore::class
                ds.name `should be` "test"
            }

        }

        val mem = ds.cast<JvmMemoryDataStore>()
        val kdef = mem.create(TestRoot::class, "a", mapOf("name" to "aname"))
        val deferred = kdef.getOrDefault { null }
        val tryk = runBlocking { deferred?.await()  }
        val k = tryk?.getOrDefault { null }
        val da = mem.retrieve<TestRoot,String>(k!!)
        val atry = runBlocking { da.await() }
        atry.isSuccess shouldBe true
        val e = atry.getOrElse { null}!!

        on("creating an entity") {

            if (tryk?.isFailure == true) {
                tryk.getOrElse { t -> log.error(t) { tryk }; "" }
            }

            it("should work") {
                kdef.isFailure `should not be` true
                deferred `should not be` null
                tryk?.isSuccess `should be` true
            }
        }



        on("retrieving an entity") {
            it("should have the right values") {
                e.id `should be equal to` "a"
                e.name `should be equal to`  "aname"
                e.online.`should be null`()
            }
        }


        fun create(k:String) : TestRoot = run {
            val kdef = mem.create(TestRoot::class, k, mapOf("name" to "aname"))
            val deferred = kdef.getOrDefault { null }
            val tryk = runBlocking { deferred?.await()  }
            val k = tryk?.getOrDefault { null }
            val da = mem.retrieve<TestRoot,String>(k!!)
            val atry = runBlocking { da.await() }
            atry.getOrElse { null}!!
        }

        val e1 = create("b")
        e1.online = true

        on("changing a property") {
            runBlocking { delay(200, TimeUnit.MILLISECONDS) }
            it("should reflect the change") {
                e1.online.`should not be null`()
                e1.online!! `should be equal to` true
            }
        }

        on("creating an entity with an existing id") {
            val kdef = mem.create(TestRoot::class, "a", mapOf("name" to "aname"))
            val deferred = kdef.getOrDefault { null }
            val tryk = runBlocking { deferred?.await()  }
            it("should fail") {
                tryk!!.isSuccess.`should be false`()
            }
            val ex = tryk!!.fold({ it }, { null })
            it("should have an exception of proper type") {
                ex.`should not be null`()
                ex `should be instance of` DataStoreError.EntityExists::class
            }
        }
    }
})


class TestVersion : Spek({
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

    val mem = ds.cast<JvmMemoryDataStore>()
    val tryDefer = mem.create(TestVersioned::class, 0.toLong(), mapOf("name" to "aname", "someone" to "not me"))
    val kdefer = tryDefer.getOrElse { null }!!
    val tryk = runBlocking { kdefer.await() }
    val k = tryk.getOrDefault { log.error(it) { it }; (-1).toLong() }
    val retrDeferred = mem.retrieve<TestVersioned, Long>(k)
    val tryVersioned = runBlocking { retrDeferred.await() }
    val entity = tryVersioned.getOrDefault { null }

    fun create() : TestVersioned = run {
        val tryDefer = mem.create(TestVersioned::class, 0.toLong(), mapOf("name" to "aname", "someone" to "not me"))
        val kdefer = tryDefer.getOrElse { null }!!
        val tryk = runBlocking { kdefer.await() }
        val k = tryk.getOrDefault { log.error(it) { it }; (-1).toLong() }
        val retrDeferred = mem.retrieve<TestVersioned, Long>(k)
        val tryVersioned = runBlocking { retrDeferred.await() }
        tryVersioned.getOrDefault { null }!!
    }
    given("an entity") {
        on("checking the result") {
            it("should be fine") {
                tryDefer.isSuccess `should be equal to`  true
                tryk.isSuccess `should be equal to` true
            }
        }

        val versionInitial = mem.version<TestVersioned, Long>(k)
        on("checking the version") {
            it("should be initial") {
                versionInitial.`should not be null`()
                versionInitial!! `should be equal to` 0.toLong()
            }
        }


        on("retrieving the entity") {
            it("should exist") {
              entity.`should not be null`()
            }
        }

        entity?.someone = "not me, but someone else"
        runBlocking { delay(100, TimeUnit.MILLISECONDS) }

        on("setting a property") {
            it("should reflect the value") {
                entity?.someone.`should not be null or blank`()
                entity?.someone!! `should be equal to`  "not me, but someone else"
            }
            val versionInc = entity?.cast<Versioned<Long>>()?._version
            it("should have a new version") {
                versionInc!! `should be equal to` 1.toLong()
            }

        }



        on("attempting to set a values with the wrong version") {
            val deferSet = mem.setProp<TestVersioned, Long, String>(entity!!.id, entity.cast<KIJvmEntity<*, *>>()._meta["someone"]!!.cast(), "oh no", 0)
            val trySet = runBlocking { deferSet.await() }
            val ex = trySet.toEither().swap().getOrElse { null }!!
            trySet.getOrElse { log.trace(it) { } }

            log.trace(ex) { }

            it("should throw a proper exception") {
                trySet.isFailure `should be equal to`  true
                ex `should be instance of`  DataStoreError.OptimisticLockException::class
                val optimisticLockException = ex.cast<DataStoreError.OptimisticLockException>()

                optimisticLockException.ds shouldBe mem
                optimisticLockException.kc shouldBe entity.cast<KIJvmEntity<*,*>>()._me
                optimisticLockException.key shouldBe k
            }
        }



    }
})

object TestDelete : Spek({
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

    val mem = ds.cast<JvmMemoryDataStore>()
    fun create(k:String) : TestRoot = run {
        val kdef = mem.create(TestRoot::class, k, mapOf("name" to "aname"))
        val deferred = kdef.getOrDefault { null }
        val tryk = runBlocking { deferred?.await()  }
        val k = tryk?.getOrDefault { null }
        val da = mem.retrieve<TestRoot,String>(k!!)
        val atry = runBlocking { da.await() }
        atry.isSuccess shouldBe true
        atry.getOrElse { null}!!
    }

    val entity = create("k")
    on("a created entity") {
        it("should really exist") {
            entity.`should not be null`()
        }
    }
    val tdel = mem.delete<TestRoot,String>(listOf("k"))
    val res = tdel.map { runBlocking { it.await() } }.fold({ null }, { it })?.fold({null}, {it})
    val tret = mem.retrieve<TestRoot, String>("k").let { runBlocking { it.await() } }
    on("deleting the entity") {
        it("can be called") {
            tdel.isSuccess.`should be true`()
        }
        it("the result should be correct") {
            res `should equal` listOf("k")
        }
    }

    on("retrieving a deleted entity") {
        it("should not succeed") {
            tret.isFailure.`should be true`()
        }
        it("should deliver a proper exception") {
            val ex = tret.fold({ it }, { null })
            ex.`should not be null`()
            ex `should be instance of` DataStoreError.EntityNotFound::class
        }
    }

})
