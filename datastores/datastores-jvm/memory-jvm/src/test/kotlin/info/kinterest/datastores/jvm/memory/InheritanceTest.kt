package info.kinterest.datastores.jvm.memory

import info.kinterest.FilterError
import info.kinterest.KIEntity
import info.kinterest.cast
import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.memory.jvm.MemCustomerJvm
import info.kinterest.datastores.jvm.memory.jvm.MemEmployeeJvm
import info.kinterest.datastores.jvm.memory.jvm.MemPersonJvm
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.filter.filter
import info.kinterest.paging.Paging
import info.kinterest.query.EntityProjection
import info.kinterest.query.EntityProjectionResult
import info.kinterest.query.Query
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.runBlocking
import org.amshove.kluent.`should be instance of`
import org.amshove.kluent.`should equal`
import org.amshove.kluent.`should throw`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

@Entity
interface MemPerson : KIEntity<String> {
    override val id: String
    var first: String
    var last: String
}

@Entity
interface MemCustomer : MemPerson {
    val customerId: Long
}

@Entity
interface MemEmployee : MemPerson {
    val rank: String
}

class InheritanceTest : Spek({
    given("") {
        val base = BaseMemTest(object : DataStoreConfig {
            override val name: String
                get() = "test"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = emptyMap()
        })
        base.metaProvider.register(MemPersonJvm.meta)
        base.metaProvider.register(MemEmployeeJvm.meta)
        base.metaProvider.register(MemCustomerJvm.meta)
        repeat(10) {
            base.create<MemPerson, String>(MemPersonJvm.Companion.Transient(base.ds, "$it", "${'A' + it}", "${'z' - it}"))
        }
        repeat(10) {
            base.create<MemEmployee, String>(MemEmployeeJvm.Companion.Transient(base.ds, "${100 + it}", "E${'a' + it}", "${'Z' - it}", "${'A' + it}"))
        }
        on("querying for one type of entity") {
            val f = filter<MemEmployee, String>(MemEmployeeJvm.meta) {
                parse("last >= \"A\"", meta)
            }
            val q = Query<MemEmployee, String>(f.cast(), listOf(EntityProjection(Ordering.NATURAL.cast(), Paging(0, 100))))
            val res = runBlocking { base.ds.query(q).getOrElse { throw it }.await() }.getOrElse { throw it }
            it("should the right number of results") {
                res.projections["entities"] `should be instance of` EntityProjectionResult::class
                val proj = res.projections["entities"] as EntityProjectionResult
                proj.page.entities.size `should equal` 10
            }
        }

        on("should the right number of results") {
            val f = filter<MemPerson, String>(MemPersonJvm.meta) {
                parse("last >= \"A\"", meta)
            }
            val q = Query<MemPerson, String>(f.cast(), listOf(EntityProjection(Ordering.NATURAL.cast(), Paging(0, 100))))
            val res = runBlocking { base.ds.query(q).getOrElse { throw it }.await() }.getOrElse { throw it }
            it("") {
                res.projections["entities"] `should be instance of` EntityProjectionResult::class
                val proj = res.projections["entities"] as EntityProjectionResult
                proj.page.entities.size `should equal` 20
                proj.page.entities.count { it is MemEmployee } `should equal` 10

            }
        }

        on("querying all entities for a property which only exists in a subclass") {
            it("") {
                val f = {
                    filter<MemPerson, String>(MemPersonJvm.meta) {
                        parse("rank >= \"A\"", meta)
                    }
                }
                f `should throw` FilterError::class
            }
        }
    }
})