package info.kinterest.datastores.jvm.memory.tx

import com.github.salomonbrys.kodein.instance
import info.kinterest.QueryError
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.memory.BaseMemTest
import info.kinterest.datastores.jvm.memory.RelPerson
import info.kinterest.datastores.jvm.memory.jvm.RelPersonJvm
import info.kinterest.functional.Try
import info.kinterest.functional.flatten
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.tx.TransactionManager
import info.kinterest.jvm.tx.TxState
import info.kinterest.jvm.tx.jvm.CreateTransactionJvm
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeout
import mu.KLogging
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.time.OffsetDateTime

class TxTest : Spek({
    val base = BaseMemTest(object : DataStoreConfig {
        override val name: String = "ds1"
        override val type: String = "jvm.mem"
        override val config: Map<String, Any?> = emptyMap()
    }, object : DataStoreConfig {
        override val name: String = "ds2"
        override val type: String = "jvm.mem"
        override val config: Map<String, Any?> = emptyMap()
    })
    val ds1 = base.dss.first()
    val ds2 = base.dss.last()
    val tm: TransactionManager = base.kodein.instance()
    base.metaProvider.register(CreateTransactionJvm.meta)
    base.metaProvider.register(RelPersonJvm.meta)
    Try.errorHandler = { ex ->
        logger.debug(ex) {}
        val here = Exception()
        logger.debug(here) { "Caught in" }
    }
    given("two datastores") {
        logger.debug { tm.qm.stores }
        on("creating an entity") {
            val createTx = CreateTransactionJvm.Transient(ds1, 0, null, OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, RelPersonJvm.meta.name, 1, ds1.name)
            val def = tm.add(createTx).getOrElse { throw it }.second
            val res = runBlocking {
                withTimeout(500) {
                    def.await()
                }
            }
            val retry = res.getOrElse { throw it }
            it("should have the id available") {
                res.isSuccess.`should be true`()

                retry `should equal` 1
            }

            val ret = tm.add(CreateTransactionJvm.Transient(ds1, 1.toLong(), null, OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, RelPersonJvm.meta.name, 1, ds1.name)) { res1 ->
                ds1.create(RelPersonJvm.meta, listOf(RelPersonJvm.Transient(ds1, (res1.getOrElse { throw it } as Number).toLong(), "a", mutableSetOf()))).getOrElse { throw it }.await().getOrElse { throw it }
            }

            it("should be created") {
                ret.isSuccess.`should be true`()
                val res1 = runBlocking { ret.getOrElse { throw it }.second.await() }
                res1.isSuccess.`should be true`()
            }

        }

        on("creating the entity a second time") {
            val ret = runBlocking { ds1.retrieve<RelPerson, Long>(RelPersonJvm.meta, listOf(1)).getOrElse { throw it }.await().getOrElse { throw it } }
            it("the created entity should exist") {
                ret.count() `should equal` 1
            }
            val fail = (tm.add(CreateTransactionJvm.Transient(ds1, 2, null, OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, RelPersonJvm.meta.name, 1.toLong(), ds1.name))).getOrElse { throw it }.second
            val fr = runBlocking { withTimeout(500) { fail.await() } }
            it("should fail") {
                fr.isFailure.`should be true`()
                val r = { fr.getOrElse { throw it } }
                r `should throw` AnyException
            }
        }

        on("trying to create an entity with another id in another ds") {
            val create = (tm.add(CreateTransactionJvm.Transient(ds1, 3, null, OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, RelPersonJvm.meta.name, 2.toLong(), ds1.name))).getOrElse { throw it }.second
            val fr = runBlocking { withTimeout(500) { create.await() } }
            val created = fr.map {
                ds2.create<RelPerson, Long>(RelPersonJvm.meta, listOf(RelPersonJvm.Transient(ds2, it as Long, "b", mutableSetOf())))
            }.getOrElse { throw it }.map { runBlocking { it.await() } }.flatten()
            it("should work") {
                fr.isSuccess.`should be true`()
                val r = { fr.getOrElse { throw it } }
                r `should not throw` AnyException
            }
            it("the entity is created") {
                created.isSuccess.`should be true`()
                val orElse = created.getOrElse { throw it }
                orElse.count() `should equal` 1
                orElse.first().id `should equal` 2
            }
        }

        on("trying to create an entity with this other id in another ds") {
            val create = (tm.add(CreateTransactionJvm.Transient(ds1, 4, null, OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, RelPersonJvm.meta.name, 2.toLong(), ds1.name))).getOrElse { throw it }.second
            val fr = runBlocking { withTimeout(500) { create.await() } }
            fr.map {
                ds1.create<RelPerson, Long>(RelPersonJvm.meta, listOf(RelPersonJvm.Transient(ds2, it as Long, "b", mutableSetOf())))
            }
            it("should not work") {
                fr.isFailure.`should be true`()
                val r = { fr.getOrElse { throw it } }
                r `should throw` AnyException
            }
        }

        val chlock: Channel<Int> = Channel()
        val chunlock: Channel<Int> = Channel()


        on("trying to add a CreateTransaction while another has already been added") {
            val tx1 = CreateTransactionJvm.Transient(ds1, 5, null, OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, RelPersonJvm.meta.name, 55.toLong(), "ds1")
            val tt1 = tm.add(tx1) { res ->
                chunlock.receive()
            }
            val tx2 = CreateTransactionJvm.Transient(ds1, 6, null, OffsetDateTime.now(), OffsetDateTime.now(), TxState.NEW, RelPersonJvm.meta.name, 55.toLong(), "ds1")
            val tt2 = tm.add(tx2) { res ->
                chlock.send(5)
            }
            it("should add them") {
                tt1.isSuccess.`should be true`()
                tt2.isSuccess.`should be true`()
            }
            runBlocking { chlock.receive(); chunlock.send(5) }
            val td1 = tt1.getOrElse { throw it }.second
            val td2 = tt2.getOrElse { throw it }.second
            val res1 = runBlocking { td1.await() }
            val res2 = runBlocking { td2.await() }
            it("one tx should be successfull") {
                res1.isSuccess.`should be true`()
            }
            it("the second tx should fail") {
                res2.isFailure.`should be true`()
                val ex = { res2.getOrElse { throw it } }
                ex `should throw` QueryError::class
            }
        }
    }

}) {
    companion object : KLogging()
}