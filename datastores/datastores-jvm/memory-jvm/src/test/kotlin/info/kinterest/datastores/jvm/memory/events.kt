package info.kinterest.datastores.jvm.memory

import info.kinterest.EntityCreateEvent
import info.kinterest.EntityEvent
import info.kinterest.KIEntity
import info.kinterest.UUID
import info.kinterest.datastores.jvm.memory.jvm.TestEventsEntityJvm
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.datastores.DataStoreConfig
import info.kinterest.jvm.events.Dispatcher
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should be true`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.kodein.di.erased.instance
import java.time.LocalDate

@Entity
interface TestEventsEntity : KIEntity<UUID> {
    override val id: UUID
    val name : String
    val dob : LocalDate
}
object TestEvents : Spek ({

    given("a dispatcher") {
        val cfg = object : DataStoreConfig {
            override val name: String
                get() = "test"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = emptyMap()
        }
        val base = BaseMemTest(cfg)
        val dispatcher: Dispatcher<EntityEvent<*, *>> by base.kodein.instance("entities")

        val listener = object {
            val ch : Channel<EntityEvent<*,*>> = Channel()
            var events = 0
            var created = 0
            val job : Job
            init {
                job = launch {
                    for(e in ch) {
                        log.debug { "listener received $e" }
                        if (e is EntityCreateEvent && e.entities.first()._meta == TestEventsEntityJvm.meta) {
                            events++
                            created++
                        }
                    }
                }
            }
        }
        runBlocking {
            @Suppress("UNCHECKED_CAST")
            dispatcher.subscribing.send(listener.ch)
        }
        base.metaProvider.register(TestEventsEntityJvm.meta)
        TestEventsEntityJvm.Transient(base.ds, UUID.randomUUID(), "svd", LocalDate.now())
        val e = base.create<TestEventsEntity, UUID>(TestEventsEntityJvm.Transient(base.ds, UUID.randomUUID(), "svd", LocalDate.now()))
        on("subscribing and creating an entity") {
            val k = e.getOrElse { log.debug(it) {} }
            log.debug { k }
            e.isSuccess.`should be true`()
            it("should receive the event") {
                listener.events `should be equal to` 1
                listener.created `should be equal to` 1
            }
            listener.ch.close()
            runBlocking { listener.job.join() }
        }

    }

})