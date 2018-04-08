package info.kinterest.datastores.jvm.filter.tree

import com.github.salomonbrys.kodein.instance
import info.kinterest.EntityEvent
import info.kinterest.core.jvm.filters.parse
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.filter.tree.jvm.SomeEntityJvm
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.filter.filter
import info.kinterest.jvm.filter.tree.FilterTree
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
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
        base.metaProvider.register(SomeEntityJvm.meta)
        val filterTree = FilterTree(base.kodein.instance("entities"), 3)
        val f = filter<SomeEntity, Long>(SomeEntityJvm.meta) {
            parse("name >= \"W\"", SomeEntityJvm.meta)
        }
        on("adding a filter") {
            filterTree += f
            it("should have a proper structure") {
                filterTree.root.entities `should have key` SomeEntityJvm.meta
                filterTree.root[SomeEntityJvm.meta].filters.size `should equal` 1
            }
        }

        on("adding a filter again") {
            filterTree += f
            it("nothing should happen") {
                filterTree.root.entities `should have key` SomeEntityJvm.meta
                filterTree.root[SomeEntityJvm.meta].filters.size `should equal` 1
            }
        }
        val f1 = filter<SomeEntity, Long>(SomeEntityJvm.meta) {
            parse("name >= \"W\"", SomeEntityJvm.meta)
        }

        on("adding a second identical filter") {
            filterTree += f1
            it("it should change") {
                filterTree.root.entities `should have key` SomeEntityJvm.meta
                filterTree.root[SomeEntityJvm.meta].filters.size `should equal` 2
            }
        }

        on("removing a second identical filter") {
            filterTree -= f1
            it("it should change") {
                filterTree.root.entities `should have key` SomeEntityJvm.meta
                filterTree.root[SomeEntityJvm.meta].filters.size `should equal` 1
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
        base.metaProvider.register(SomeEntityJvm.meta)
        val filterTree = FilterTree(base.kodein.instance("entities"), 3)
        val f = filter<SomeEntity, Long>(SomeEntityJvm.meta) {
            parse("name >= \"W\"", SomeEntityJvm.meta)
        }
        val listener = object {
            val ch = Channel<EntityEvent<*, *>>()
            val ping = Channel<Int>()
            var total = 0
            val job: Job

            init {
                job = launch(base.context) {
                    for (ev in ch) {
                        logger.debug { "event: $ev" }
                        total++
                        ping.send(total)
                    }
                }
            }

            fun close() = runBlocking { f.listener = null; ch.close(); job.join() }
            fun wait(): Int = runBlocking(base.context) { withTimeout(1000) { ping.receive() } }
        }
        f.listener = listener.ch
        filterTree += f

        on("creating a non-matching entity") {
            logger.debug { "first" }
            val idA = base.create<SomeEntity, Long>(listOf(SomeEntityJvm.Companion.Transient(base.ds, 1, "AA", true, null))).getOrElse { throw it }
            base.retrieve<SomeEntity, Long>(listOf(idA.id)).getOrElse { throw it }.first()
            it("should not hit our filter") {
                runBlocking(base.context) { delay(200) }
                listener.total `should equal` 0
            }
        }


        on("creating a matching entity") {
            logger.debug { "second" }
            val idX = runBlocking {
                withTimeout(300) {
                    base.create<SomeEntity, Long>(listOf(SomeEntityJvm.Companion.Transient(base.ds, 2, "X", true, null))).getOrElse { throw it }
                }
            }
            logger.debug { "created" }
            val e = runBlocking { withTimeout(300) { base.retrieve<SomeEntity, Long>(listOf(idX.id)).getOrElse { throw it }.first() } }
            logger.debug { e.name }
            it("should hit our filter") {
                runBlocking(base.context) { delay(200) }
                listener.wait()
                listener.total `should equal` 1
            }
        }


        on("changing a property") {
            logger.debug { "third" }
            val e = base.retrieve<SomeEntity, Long>(listOf(2)).getOrElse { throw it }.first()
            e.dob = LocalDate.now()
            logger.debug { e.dob }
            it("should not hit our filter") {
                runBlocking(base.context) { delay(200) }
                listener.total `should equal` 1
            }
        }


        on("another filter which reacts to that property") {
            logger.debug { "fourth" }
            val f1 = filter<SomeEntity, Long>(SomeEntityJvm.meta) {
                val ds = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))
                parse("""dob >= date("$ds", "dd.MM.yyyy")""", SomeEntityJvm.meta)
            }
            f1.listener = listener.ch
            filterTree += f1
            val e = base.retrieve<SomeEntity, Long>(listOf(2)).getOrElse { throw it }.first()
            e.dob = LocalDate.now().minusDays(2)
            it("should hit our filter") {
                e.dob `should equal` LocalDate.now().minusDays(2)
                listener.wait()
                listener.total `should equal` 2
            }
        }
        afterGroup {
            logger.debug { "after ${Runtime.getRuntime().availableProcessors()}" }
            listener.close()

        }
    }
}) {
    companion object : KLogging()
}