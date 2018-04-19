package info.kinterest.datastores.jvm.memory

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import info.kinterest.*
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.datastores.jvm.datasourceKodein
import info.kinterest.datastores.jvm.memory.jvm.TestRootJvm
import info.kinterest.datastores.jvm.memory.jvm.TestVersionedJvm
import info.kinterest.functional.flatten
import info.kinterest.functional.getOrDefault
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.annotations.StorageTypes
import info.kinterest.jvm.annotations.Versioned
import info.kinterest.jvm.coreKodein
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
@Versioned
interface TestVersioned : KIVersionedEntity<Long> {
    override val id: Long
    val name : String
    var online : Boolean?
    var someone : String
}



object TestJvmMem : Spek({

    given("a datasore") {
        val kodein = Kodein {
            import(coreKodein)
            import(datasourceKodein)
        }
        kodein.instance<DataStoreFactoryProvider>().inject(kodein)
        val fac = kodein.instance<DataStoreFactoryProvider>()
        val metaProvider = kodein.instance<MetaProvider>()

        val cfg = object : DataStoreConfig {
            override val name: String
                get() = "test"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = emptyMap()
        }
        val ds = fac.factories[cfg.type]!!.create(cfg)

        on("its type") {
            it("should have the proper type and name") {
                ds `should be instance of` JvmMemoryDataStore::class
                ds.name `should be` "test"
            }

        }

        val mem = ds.cast<JvmMemoryDataStore>()
        mem[TestRoot::class]
        val kdef = mem.create<TestRoot, String>(TestRootJvm.meta, listOf(TestRootJvm.Companion.Transient(mem, "a", "aname", null)))
        val deferred = kdef.getOrDefault { null }
        val tryk = runBlocking { deferred?.await()  }
        val k = tryk?.getOrDefault { throw it }!!.first().id
        val da = mem.retrieve<TestRoot, String>(metaProvider.meta(TestRoot::class)!!, listOf(k))
        val atry = da.map { runBlocking { it.await() } }.flatten()
        atry.isSuccess shouldBe true
        val e = atry.getOrElse { null}!!.first()

        on("creating an entity") {

            if (tryk.isFailure) {
                tryk.getOrElse { t -> log.error(t) { tryk }; "" }
            }

            it("should work") {
                kdef.isFailure `should not be` true
                deferred `should not be` null
                tryk.isSuccess `should be` true
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
            val kd = mem.create<TestRoot, String>(TestRootJvm.meta, listOf(TestRootJvm.Companion.Transient(mem, k, "aname", null)))
            val def = kd.getOrDefault { throw it }
            val tk = runBlocking { def.await() }
            val key = tk.getOrDefault { throw it }.first().id
            val dret = mem.retrieve<TestRoot, String>(TestRootJvm.meta, listOf(key))
            val tryret = dret.map { runBlocking { it.await() } }.flatten()
            tryret.getOrElse { listOf()}.first()
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
            val kd = mem.create(TestRootJvm.meta, listOf(TestRootJvm.Companion.Transient(mem, "a", "aname", null)))
            val def = kd.getOrDefault { throw it }
            val tk = runBlocking { def.await() }
            it("should fail") {
                tk.isSuccess.`should be false`()
            }
            val ex = tk.fold({ it }, { null })
            it("should have an exception of proper type") {
                ex.`should not be null`()
                ex `should be instance of` DataStoreError.EntityError.EntityExists::class
            }
        }
    }
})


class TestVersion : Spek({

    given("an entity") {
        val kodein = Kodein {
            import(coreKodein)
            import(datasourceKodein)
        }
        kodein.instance<DataStoreFactoryProvider>().inject(kodein)
        val fac = kodein.instance<DataStoreFactoryProvider>()

        val cfg = object : DataStoreConfig {
            override val name: String
                get() = "test"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = emptyMap()
        }

        val ds = fac.factories[cfg.type]!!.create(cfg)

        val mem = ds.cast<JvmMemoryDataStore>()
        val tryDefer = mem.create<TestVersioned, Long>(TestVersionedJvm.meta, listOf(TestVersionedJvm.Companion.Transient(mem, 0.toLong(), "aname", null, "not me")))
        val kdefer = tryDefer.getOrElse { throw it }
        val tryk = runBlocking { kdefer.await() }
        val k = tryk.getOrElse { throw it }.first().id
        val retrDeferred = mem.retrieve<TestVersioned, Long>(TestVersionedJvm.meta, listOf(k))
        val tryVersioned = retrDeferred.map { runBlocking { it.await() } }.flatten()
        val entity = tryVersioned.getOrDefault { listOf() }.first()

        @Suppress("unused")
        fun create(id: Long): TestVersioned = run {
            val td = mem.create(TestVersionedJvm.meta, listOf(TestVersionedJvm.Companion.Transient(mem, id, "aname", null, "someone")))
            val kd = td.getOrElse { null }!!
            val tk = runBlocking { kd.await() }
            val key = tk.getOrElse { throw it }.first().id
            val rd = mem.retrieve<TestVersioned, Long>(TestVersionedJvm.meta, listOf(key))
            val tv = rd.map { runBlocking { it.await() } }.flatten()
            tv.getOrElse { throw it }.first()
        }
        on("checking the result") {
            it("should be fine") {
                tryDefer.isSuccess `should be equal to`  true
                tryk.isSuccess `should be equal to` true
            }
        }

        val versionInitial = mem.version(TestVersionedJvm.meta, k)
        on("checking the version") {
            it("should be initial") {
                versionInitial.`should not be null`()
                versionInitial as Long `should be equal to` 0.toLong()
            }
        }


        on("retrieving the entity") {
            it("should exist") {
              entity.`should not be null`()
            }
        }

        entity.someone = "not me, but someone else"
        runBlocking { delay(100, TimeUnit.MILLISECONDS) }

        on("setting a property") {
            it("should reflect the value") {
                entity.someone.`should not be null or blank`()
                entity.someone `should be equal to` "not me, but someone else"
            }
            val versionInc = entity._version
            it("should have a new version") {
                versionInc as Long `should be equal to` 1.toLong()
            }

        }



        on("attempting to set a values with the wrong version") {
            val deferSet = mem.setValues(TestVersionedJvm.meta, entity.id, 0.toLong(), mapOf(TestVersionedJvm.meta.PROP_SOMEONE to "oh no"))
            val trySet = runBlocking { deferSet.await() }
            val ex = trySet.toEither().swap().getOrElse { null }!!
            trySet.getOrElse { log.trace(it) { } }

            log.trace(ex) { }

            it("should throw a proper exception") {
                trySet.isFailure `should be equal to`  true
                ex `should be instance of`  DataStoreError.OptimisticLockException::class
                val optimisticLockException = ex.cast<DataStoreError.OptimisticLockException>()

                optimisticLockException.ds shouldBe mem
                optimisticLockException.meta shouldBe entity.cast<KIJvmEntity<*,*>>()._meta
                optimisticLockException.key shouldBe k
            }
        }



    }
})

object TestDelete : Spek({
    given("") {
        val kodein = Kodein {
            import(coreKodein)
            import(datasourceKodein)
        }
        kodein.instance<DataStoreFactoryProvider>().inject(kodein)
        val fac = kodein.instance<DataStoreFactoryProvider>()

        val cfg = object : DataStoreConfig {
            override val name: String
                get() = "test"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = emptyMap()
        }

        val ds = fac.factories[cfg.type]!!.create(cfg)

        val mem = ds.cast<JvmMemoryDataStore>()
        fun create(k: String): TestRoot = run {
            val kdef = mem.create<TestRoot, String>(TestRootJvm.meta, listOf(TestRootJvm.Companion.Transient(mem, k, "aname", null)))
            val deferred = kdef.getOrElse { throw it }
            val tryk = runBlocking { deferred.await() }
            val key = tryk.getOrElse { throw it }.first().id
            val da = mem.retrieve<TestRoot, String>(TestRootJvm.meta, listOf(key))
            val atry = da.map { runBlocking { it.await() } }.flatten()
            atry.isSuccess shouldBe true
            atry.getOrElse { listOf() }.first()
        }

        val entity = create("k")
        on("a created entity") {
            it("should really exist") {
                entity.`should not be null`()
            }
        }
        val tdel = mem.delete(TestRootJvm.meta, listOf(entity))
        val res = tdel.map { runBlocking { it.await() } }.fold({ null }, { it })?.fold({ null }, { it })
        val tret = mem.retrieve<TestRoot, String>(TestRootJvm.meta, listOf("k")).let { it.map { runBlocking { it.await() } } }.flatten()
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
                ex `should be instance of` DataStoreError.EntityError.EntityNotFound::class
            }
        }
    }

})
