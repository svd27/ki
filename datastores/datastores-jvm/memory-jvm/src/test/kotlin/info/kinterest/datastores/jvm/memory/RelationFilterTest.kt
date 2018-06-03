package info.kinterest.datastores.jvm.memory

import info.kinterest.*
import info.kinterest.datastores.jvm.memory.jvm.RelPersonJvm
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.datastores.DataStoreConfig
import info.kinterest.jvm.filter.AnyRelationFilter
import info.kinterest.jvm.filter.EQFilter
import info.kinterest.jvm.filter.filter
import info.kinterest.jvm.filter.tree.FilterTree
import info.kinterest.jvm.util.EventWaiter
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.runBlocking
import org.amshove.kluent.`should be false`
import org.amshove.kluent.`should be instance of`
import org.amshove.kluent.`should be true`
import org.amshove.kluent.`should equal`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.kodein.di.erased.instance

class RelationFilterTest : Spek({
    val base = BaseMemTest(object : DataStoreConfig {
        override val name: String
            get() = "test"
        override val type: String
            get() = "jvm.mem"
        override val config: Map<String, Any?>
            get() = emptyMap()
    })
    base.metaProvider.register(RelPersonJvm.meta)
    val f = filter<RelPerson, Long>(RelPersonJvm.meta) {
        AnyRelationFilter<RelPerson, Long, RelPerson, Long>(meta, RelPersonJvm.meta.PROP_FRIENDS, EQFilter(RelPersonJvm.meta.PROP_ONLINE, meta, true))
    }
    val channelFilter = Channel<FilterEvent<RelPerson, Long>>()
    f.listener = channelFilter
    val filterWaiter = EventWaiter(channelFilter)
    repeat(5) {
        base.create<RelPerson, Long>(RelPersonJvm.Transient(base.ds, it.toLong(), "${'A' + it}", false, mutableSetOf())).getOrElse { throw it }
    }

    val ft by base.kodein.instance<FilterTree>()
    ft += f

    given("some entities") {
        val events = Channel<EntityEvent<*, *>>()
        runBlocking { base.dispatcher.subscribing.send(events) }
        val waiter = EventWaiter<EntityEvent<*, *>>(events.cast())
        val p1 = base.retrieve<RelPerson, Long>(listOf(1.toLong())).getOrElse { throw it }.first()
        val p2 = base.retrieve<RelPerson, Long>(listOf(2.toLong())).getOrElse { throw it }.first()
        on("checking the filter") {
            it("should never match") {
                f.matches(p1).`should be false`()
                f.matches(p2).`should be false`()
            }
        }
        on("adding a friend") {
            p1.friends.add(p2)
            waiter.waitFor { it is EntityRelationsAdded<*, *, *, *> && it.relation.source == p1 && it.relation.target == p2 }
            it("should still be false") {
                f.matches(p1).`should be false`()
            }
        }

        on("setting a friend online") {
            p2.online = true
            val fe = filterWaiter.waitFor { it is FilterRelationInScopeEvent && it.entity == p1 }
            waiter.waitFor { it is EntityUpdatedEvent && it.entity == p2 }
            it("should now accept the entity") {
                f.matches(p1).`should be true`()
                fe.`should be instance of`(FilterRelationInScopeEvent::class)
                val scope = fe as FilterRelationInScopeEvent
                scope.entity `should equal` p1
            }
        }
    }

})