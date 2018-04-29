package info.kinterest.datastores.jvm.memory.tx

import com.github.salomonbrys.kodein.instance
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.memory.BaseMemTest
import info.kinterest.datastores.jvm.memory.RelPerson
import info.kinterest.datastores.jvm.memory.jvm.RelPersonJvm
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.tx.TransactionManager
import info.kinterest.jvm.tx.TxState
import info.kinterest.jvm.tx.jvm.CreateTransactionJvm
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeout
import mu.KLogging
import org.amshove.kluent.AnyException
import org.amshove.kluent.`should be true`
import org.amshove.kluent.`should equal`
import org.amshove.kluent.`should throw`
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
            val createTx = CreateTransactionJvm.Transient(ds1, 0, null, OffsetDateTime.now(), TxState.NEW, RelPersonJvm.meta.name, 1, ds1.name)
            val def = tm + createTx
            val res = runBlocking {
                withTimeout(500) {
                    def.getOrElse { throw it }.await().getOrElse { throw it }
                }
            }
            it("should work") {
                res `should equal` 1
            }
            runBlocking {
                withTimeout(500) {
                    val ret = tm + CreateTransactionJvm.Transient(ds1, 1.toLong(), null, OffsetDateTime.now(), TxState.NEW, RelPersonJvm.meta.name, 1, ds1.name)
                    val id = ret.getOrElse { throw it }.await().getOrElse { throw it }
                    logger.debug { "creating entity id $id" }
                    ds1.create(RelPersonJvm.meta, listOf(RelPersonJvm.Transient(ds1, (id as Number).toLong(), "a", mutableSetOf())))
                }
            }

        }

        on("creating the entity a second time") {
            val ret = runBlocking { ds1.retrieve<RelPerson, Long>(RelPersonJvm.meta, listOf(1)).getOrElse { throw it }.await().getOrElse { throw it } }
            it("the created entity should exist") {
                ret.count() `should equal` 1
            }
            val fail = (tm + CreateTransactionJvm.Transient(ds1, 2, null, OffsetDateTime.now(), TxState.NEW, RelPersonJvm.meta.name, 1.toLong(), ds1.name)).getOrElse { throw it }
            val fr = runBlocking { withTimeout(500) { fail.await() } }
            it("should fail") {
                fr.isFailure.`should be true`()
                val r = { fr.getOrElse { throw it } }
                r `should throw` AnyException
            }
        }
    }

}) {
    companion object : KLogging()
}