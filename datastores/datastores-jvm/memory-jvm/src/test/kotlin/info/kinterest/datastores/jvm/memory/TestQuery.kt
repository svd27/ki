package info.kinterest.datastores.jvm.memory

import info.kinterest.KIEntity
import info.kinterest.annotations.Entity
import info.kinterest.annotations.StorageTypes
import info.kinterest.core.jvm.filters.parse
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.functional.flatten
import info.kinterest.functional.getOrDefault
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.MetaProvider
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import org.amshove.kluent.`should be before`
import org.amshove.kluent.`should be equal to`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.time.LocalDate
import java.time.Month

@Entity
@StorageTypes(["jvm.mem"])
interface QueryEntity : KIEntity<String> {
    override val id : String
    val name : String
    var job: String?
    var dob : LocalDate
}

class TestQuery : Spek( {
    given("some entities") {
        val base = BaseMemTest(object : DataStoreConfig {
            override val name: String
                get() = "test"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = mapOf()
        })
        val provider = MetaProvider()
        @Suppress("UNCHECKED_CAST")
        val meta = base.ds[QueryEntity::class]
        provider.register(meta)

        val keys = ('A'..'Z').map { id ->
            base.create<QueryEntity, String>("$id",
                    mapOf(
                            "name" to "sasa",
                            "job" to null,
                            "dob" to LocalDate.now()
                    )
            )
        }.map { it.getOrElse { throw it } }
        val entities = base.retrieve<QueryEntity, String>(keys).getOrDefault { listOf() }.toList()

        val f = parse<QueryEntity,String>("QueryEntity{job < \"a\"}", provider, base.ds)
        val tq = base.ds.query(meta, f)
        on("a simple query") {
            it("a query succeed") {
                tq.isSuccess `should be equal to` true
                val tres = tq.map { runBlocking { it.await() } }.flatten()
                tres.isSuccess `should be equal to` true
                val res = tres.getOrElse { throw it }
                res.count() `should be equal to` 26
            }
        }

        val tres = tq.map { runBlocking { it.await() } }.flatten()
        val res = tres.getOrElse { throw it }
        on("a query on a different field") {
            entities[0].dob = LocalDate.of(1968, Month.SEPTEMBER, 27)
            runBlocking { delay(10.toLong()) }
            val f = parse<QueryEntity, String>("dob < date(\"28.9.1968\", \"d.M.yyyy\")", provider, base.ds)

            it("a query should work for all fields") {
                val filtered = entities.filter { f.matches(it) }
                filtered.size `should be equal to` 1
                println(filtered)
                filtered.first().dob `should be before` LocalDate.of(1968, Month.SEPTEMBER, 28)
            }
        }
    }
})