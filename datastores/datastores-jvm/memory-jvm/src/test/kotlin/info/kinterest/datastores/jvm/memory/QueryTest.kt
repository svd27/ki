package info.kinterest.datastores.jvm.memory

import info.kinterest.cast
import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.datastores.jvm.memory.jvm.QueryEntityJvm
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.datastores.DataStoreConfig
import info.kinterest.jvm.filter.filter
import info.kinterest.paging.Paging
import info.kinterest.query.EntityProjection
import info.kinterest.query.EntityProjectionResult
import info.kinterest.query.Query
import info.kinterest.sorting.Ordering
import info.kinterest.sorting.asc
import kotlinx.coroutines.experimental.runBlocking
import org.amshove.kluent.`should be instance of`
import org.amshove.kluent.`should be true`
import org.amshove.kluent.`should equal`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.time.LocalDate

class QueryTest : Spek({
    val meta = QueryEntityJvm.Meta
    given("a query") {
        val base = BaseMemTest(object : DataStoreConfig {
            override val name: String
                get() = "test"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = mapOf()
        })
        base.metaProvider.register(meta)
        repeat(26) {
            base.create<QueryEntity, String>(QueryEntityJvm.Transient(base.ds, "${'A' + it}", "${'Z' - it}anne", null, LocalDate.now()))
        }
        val f = filter<QueryEntity, String>(meta) {
            parse("""id > "H" """, meta)
        }
        val projection = EntityProjection<QueryEntity, String>(Ordering(listOf(meta.PROP_NAME.asc())), Paging(0, 3))
        val q = Query<QueryEntity, String>(f.cast(), listOf(projection))
        on("querying by name ascending") {
            val tq = base.ds.query(q)
            it("should not fail") {
                tq.isSuccess.`should be true`()
            }
            val qd = tq.getOrElse { throw it }
            val td = runBlocking { qd.await() }
            it("should in all be a success") {
                td.isSuccess.`should be true`()
            }
            val queryResult = td.getOrElse { throw it }
            it("should return a proper page") {
                queryResult.projections[projection] `should be instance of` EntityProjectionResult::class
                val proj = queryResult.projections[projection] as EntityProjectionResult
                proj.page.entities.count() `should equal` 3
                proj.page.entities.elementAt(0).name `should equal` "Aanne"
            }
        }

        on("changing the page") {
            val projection1 = EntityProjection(projection.ordering, projection.paging.next)
            val q1 = Query(q.f, listOf(projection1))
            val tq = base.ds.query(q1)
            it("should initially work") {
                tq.isSuccess.`should be true`()
            }
            val qd = tq.getOrElse { throw it }
            val queryResult = runBlocking { qd.await() }.getOrElse { throw it }

            it("should work") {
                queryResult.projections[projection1] `should be instance of` EntityProjectionResult::class
                val proj = queryResult.projections[projection1] as EntityProjectionResult
                proj.page.entities.elementAt(0).name `should equal` "Danne"
            }

        }
    }
})