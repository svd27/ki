package info.kinterest.datastores.jvm.memory

import info.kinterest.KIEntity
import info.kinterest.cast
import info.kinterest.core.jvm.filters.parse
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.memory.jvm.QueryEntityJvm
import info.kinterest.functional.flatten
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.annotations.StorageTypes
import info.kinterest.paging.Paging
import info.kinterest.query.EntityProjection
import info.kinterest.query.EntityProjectionResult
import info.kinterest.query.Query
import info.kinterest.sorting.Ordering
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
        val provider = base.metaProvider
        @Suppress("UNCHECKED_CAST")
        val meta = QueryEntityJvm.meta
        provider.register(meta)

        val keys = ('A'..'Z').map { id ->

            base.create<QueryEntity, String>(QueryEntityJvm.Transient(base.ds, "$id", "sasa", null, LocalDate.now()))
        }.map { it.getOrElse { throw it } }

        val f = parse<QueryEntity, String>("QueryEntity{job < \"a\"}", meta)
        val projection = EntityProjection<QueryEntity, String>(Ordering.NATURAL.cast(), Paging(0, -1))
        val tq = base.ds.query(Query<QueryEntity, String>(f.cast(), listOf(projection)))
        on("a simple query") {
            it("a query succeed") {
                tq.isSuccess `should be equal to` true
                val tres = tq.map { runBlocking { it.await() } }.flatten()
                tres.isSuccess `should be equal to` true
                val queryResult = tres.getOrElse { throw it }
                val proj = queryResult.projections[projection] as EntityProjectionResult
                proj.page.entities.size `should be equal to` 26
            }
        }

        val tres = tq.map { runBlocking { it.await() } }.flatten()
        tres.getOrElse { throw it }
        on("a query on a different field") {
            keys[0].dob = LocalDate.of(1968, Month.SEPTEMBER, 27)
            runBlocking { delay(10.toLong()) }
            val f1 = parse<QueryEntity, String>("dob < date(\"28.9.1968\", \"d.M.yyyy\")", meta)

            it("a query should work for all fields") {
                val filtered = keys.filter { f1.matches(it) }
                filtered.size `should be equal to` 1
                println(filtered)
                filtered.first().dob `should be before` LocalDate.of(1968, Month.SEPTEMBER, 28)
            }
        }
    }
})