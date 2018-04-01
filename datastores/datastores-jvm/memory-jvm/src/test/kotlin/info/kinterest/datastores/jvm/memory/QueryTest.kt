package info.kinterest.datastores.jvm.memory

import info.kinterest.cast
import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.memory.jvm.mem.QueryEntityJvmMem
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.filter.filter
import info.kinterest.paging.Paging
import info.kinterest.query.Query
import info.kinterest.sorting.Ordering
import info.kinterest.sorting.asc
import kotlinx.coroutines.experimental.runBlocking
import org.amshove.kluent.`should be true`
import org.amshove.kluent.`should equal`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.time.LocalDate

class QueryTest : Spek({
    val meta = QueryEntityJvmMem.Companion.Meta
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
        repeat(26) { base.create<QueryEntity, String>("${'A' + it}", mapOf("name" to "${'Z' - it}anne", "dob" to LocalDate.now())) }
        val f = filter<QueryEntity, String>(base.ds, meta) {
            parse("""id > "H" """, base.metaProvider)
        }
        val q = Query<QueryEntity, String>(f.cast(), Ordering(listOf(meta.PROP_NAME.asc())), Paging(0, 3))
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
            val page = td.getOrElse { throw it }
            it("should return a proper page") {
                page.entites.count() `should equal` 3
                page.entites.elementAt(0).name `should equal` "Aanne"
            }
        }

        on("changing the page") {
            val q1 = Query<QueryEntity, String>(q.f, q.ordering, q.page.next)
            val tq = base.ds.query(q1)
            it("should initially work") {
                tq.isSuccess.`should be true`()
            }
            val qd = tq.getOrElse { throw it }
            val tp = runBlocking { qd.await() }.getOrElse { throw it }
            tp.entites.elementAt(0).name `should equal` "Danne"
        }
    }
})