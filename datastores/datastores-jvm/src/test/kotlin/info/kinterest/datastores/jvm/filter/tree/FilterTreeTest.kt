package info.kinterest.datastores.jvm.filter.tree

import com.github.salomonbrys.kodein.instance
import info.kinterest.EntityEvent
import info.kinterest.core.jvm.filters.parse
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.filter.tree.jvm.mem.SomeEntityJvmMem
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.filter.filter
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging
import org.amshove.kluent.`should equal`
import org.amshove.kluent.`should have key`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class FilterTreeTest : Spek({
    given("") {
        val base = BaseDataStoreTest(object : DataStoreConfig {
            override val name: String
                get() = "name"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = mapOf()
        })
        base.metaProvider.register(SomeEntityJvmMem.meta)
        val filterTree: FilterTree = FilterTree(base.kodein.instance("entities"), 3)
        val f = filter<SomeEntity, Long>(base.ds, SomeEntityJvmMem.meta) {
            parse("name >= \"W\"", base.metaProvider, base.ds)
        }
        on("adding a filter") {
            filterTree += f
            it("should have a proper structure") {
                filterTree.root.entities `should have key` SomeEntityJvmMem.meta
                filterTree.root[SomeEntityJvmMem.meta].filters.size `should equal` 1
            }
        }

        on("adding a filter again") {
            filterTree += f
            it("nothing should happen") {
                filterTree.root.entities `should have key` SomeEntityJvmMem.meta
                filterTree.root[SomeEntityJvmMem.meta].filters.size `should equal` 1
            }
        }
        val f1 = filter<SomeEntity, Long>(base.ds, SomeEntityJvmMem.meta) {
            parse("name >= \"W\"", base.metaProvider, base.ds)
        }

        on("adding a second identical filter") {
            filterTree += f1
            it("it should change") {
                filterTree.root.entities `should have key` SomeEntityJvmMem.meta
                filterTree.root[SomeEntityJvmMem.meta].filters.size `should equal` 2
            }
        }

        on("removing a second identical filter") {
            filterTree -= f1
            it("it should change") {
                filterTree.root.entities `should have key` SomeEntityJvmMem.meta
                filterTree.root[SomeEntityJvmMem.meta].filters.size `should equal` 1
            }
        }
    }

    given("a filtertree") {
        val base = BaseDataStoreTest(object : DataStoreConfig {
            override val name: String
                get() = "name"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = mapOf()
        })
        base.metaProvider.register(SomeEntityJvmMem.meta)
        val filterTree = FilterTree(base.kodein.instance("entities"), 3)
        val f = filter<SomeEntity, Long>(base.ds, SomeEntityJvmMem.meta) {
            parse("name >= \"W\"", base.metaProvider, base.ds)
        }
        val listener = object {
            val ch = Channel<EntityEvent<*, *>>()
            var total = 0
            val job: Job

            init {
                job = launch {
                    for (ev in ch) {
                        logger.debug { "event: $ev" }
                        total++
                    }
                }
            }

            fun close() = runBlocking { f.listener = null; ch.close(); job.join() }
        }
        f.listener = listener.ch
        filterTree += f

        on("creating a non-matching entity") {
            logger.debug { "first" }
            val idA = base.create<SomeEntity, Long>(1, mapOf("name" to "A")).getOrElse { throw it }
            val e = base.retrieve<SomeEntity, Long>(listOf(idA)).getOrElse { throw it }.first()
            it("should not hit our filter") {
                runBlocking { delay(200) }
                listener.total `should equal` 0
            }
        }


        on("creating a matching entity") {
            logger.debug { "second" }
            val idX = base.create<SomeEntity, Long>(2, mapOf("name" to "X")).getOrElse { throw it }
            val e = base.retrieve<SomeEntity, Long>(listOf(idX)).getOrElse { throw it }.first()
            logger.debug { e.name }
            it("should hit our filter") {
                runBlocking { delay(200) }
                listener.total `should equal` 1
            }
        }


        on("changing a property") {
            logger.debug { "third" }
            val e = base.retrieve<SomeEntity, Long>(listOf(2)).getOrElse { throw it }.first()
            e.dob = LocalDate.now()
            logger.debug { e.dob }
            it("should not hit our filter") {
                runBlocking { delay(200) }
                listener.total `should equal` 1
            }
        }


        on("another filter which reacts to that property") {
            logger.debug { "fourth" }
            val f1 = filter<SomeEntity, Long>(base.ds, SomeEntityJvmMem.meta) {
                val ds = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))
                parse("""dob >= date("$ds", "dd.MM.yyyy")""", base.metaProvider, base.ds)
            }
            f1.listener = listener.ch
            filterTree += f1
            val e = base.retrieve<SomeEntity, Long>(listOf(2)).getOrElse { throw it }.first()
            e.dob = LocalDate.now().minusDays(2)
            it("should hit our filter") {
                e.dob `should equal` LocalDate.now().minusDays(2)
                runBlocking { delay(200) }
                listener.total `should equal` 2
            }
            f1.listener = null
        }
        afterGroup {
            logger.debug { "after" }
            listener.close()
        }
    }
}) {
    companion object : KLogging()
}