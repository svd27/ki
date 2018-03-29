package info.kinterest.datastores.jvm.memory

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import info.kinterest.*
import info.kinterest.annotations.Entity
import info.kinterest.annotations.StorageTypes
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.datastores.jvm.datasourceKodein
import info.kinterest.jvm.coreKodein
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
import java.time.LocalDate

@Entity
@StorageTypes(["jvm.mem"])
interface TestEventsEntity : KIEntity<UUID> {
    override val id: UUID
    val name : String
    val dob : LocalDate
}
object TestEvents : Spek ({
    val cfg = object : DataStoreConfig {
        override val name: String
            get() = "test"
        override val type: String
            get() = "jvm.mem"
        override val config: Map<String, Any?>
            get() = emptyMap()
    }
    val base = BaseMemTest(cfg)
    val dispatcher : Dispatcher<EntityEvent<*,*>> = base.kodein.instance("entities")

    val mem = base.ds
    given("a dispatcher") {
        val listener = object {
            val ch : Channel<EntityEvent<*,*>> = Channel()
            var events = 0
            var created = 0
            val job : Job
            init {
                job = launch {
                    for(e in ch) {
                        log.debug { e }
                        events++
                        when (e) {
                            is EntityCreateEvent -> created++
                        }
                    }
                }
            }
        }
        runBlocking {
            @Suppress("UNCHECKED_CAST")
            dispatcher.subscribing.send(listener.ch)
        }
        val e = base.create<TestEventsEntity,UUID>(UUID.randomUUID(), mapOf("dob" to LocalDate.now()))
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