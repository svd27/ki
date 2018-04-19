package info.kinterest.datastores.jvm.memory

import info.kinterest.EntityEvent
import info.kinterest.EntityRelationsAdded
import info.kinterest.EntityRelationsRemoved
import info.kinterest.KIEntity
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.memory.jvm.RelPersonJvm
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.annotations.Relation
import info.kinterest.jvm.getIncomingRelations
import info.kinterest.jvm.util.EventWaiter
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.runBlocking
import org.amshove.kluent.`should be instance of`
import org.amshove.kluent.`should be true`
import org.amshove.kluent.`should equal`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

@Entity
interface RelPerson : KIEntity<Long> {
    override val id: Long

    val name: String

    val friends: MutableSet<RelPerson>
        @Relation(target = RelPerson::class, targetId = Long::class) get() = TODO()
}

class RelationTest : Spek({
    given("an entity with relationsips") {
        val base = BaseMemTest(object : DataStoreConfig {
            override val name: String
                get() = "test"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = mapOf()
        })

        base.metaProvider.register(RelPersonJvm.meta)
        val subscription: Channel<EntityEvent<*, *>> = Channel()
        val waiter = EventWaiter<EntityEvent<*, *>>(subscription)
        runBlocking { base.dispatcher.subscribing.send(subscription) }


        repeat(2) {
            base.create<RelPerson, Long>(RelPersonJvm.Companion.Transient(base.ds, it.toLong(), "${'A' + it}", mutableSetOf()))
        }
        on("addding a relation") {
            val p1 = base.retrieve<RelPerson, Long>(listOf(0)).getOrElse { throw it }.first()
            @Suppress("UNCHECKED_CAST")
            val p2 = base.retrieve<RelPerson, Long>(listOf(1)).getOrElse { throw it }.first() as KIJvmEntity<RelPerson, Long>
            val addres = base.ds.addRelation(info.kinterest.meta.Relation(RelPersonJvm.Companion.Meta.PROP_FRIENDS, p1, p2))

            it("should be reflected in the property") {
                addres.isSuccess.`should be true`()
                val res = runBlocking { addres.getOrElse { throw it }.await().getOrElse { throw it } }
                res.`should be true`()
                @Suppress("UNCHECKED_CAST")
                val evt = waiter.waitFor {
                    val ev = it as EntityEvent<RelPerson, Long>
                    ev is EntityRelationsAdded<*, *, *, *> && ev.relations.count() == 1 && ev.relations.first().target == p2
                }
                evt `should be instance of` EntityRelationsAdded::class
                p1.friends.first() `should equal` p2
            }

            it("should also show up as incoming") {
                val incoming = p2.getIncomingRelations(RelPersonJvm.meta.PROP_FRIENDS, RelPersonJvm.meta).getOrElse { throw it }.toList()
                incoming.size `should equal` 1
                incoming.first().id `should equal` p1.id
                incoming.first().type `should equal` RelPersonJvm.meta.name
            }
        }

        on("adding a relation at creation time") {
            val p1 = base.retrieve<RelPerson, Long>(listOf(0)).getOrElse { throw it }.first()
            val p2 = base.retrieve<RelPerson, Long>(listOf(1)).getOrElse { throw it }.first()
            val cr = base.create<RelPerson, Long>(RelPersonJvm.Companion.Transient(base.ds, 2, "${'A' + 2}", mutableSetOf(p1, p2)))


            it("should be properly created") {
                cr.isSuccess.`should be true`()
                cr.getOrElse { throw it }.id `should equal` 2
            }

            val p3 = base.retrieve<RelPerson, Long>(listOf(2)).getOrElse { throw it }.first()
            it("should contain the proper number and type of relations") {
                p3.friends.size `should equal` 2
                p3.friends `should equal` setOf(p1, p2)
            }

            val incoming1 = (p1 as KIJvmEntity<*, *>).getIncomingRelations(RelPersonJvm.meta.PROP_FRIENDS, RelPersonJvm.meta).getOrElse { throw it }.toList()
            it("should be reflected in incomings") {
                incoming1.size `should equal` 1
                incoming1.map { it.id }.toSet() `should equal` setOf(p3.id)
            }

            val incoming2 = (p2 as KIJvmEntity<*, *>).getIncomingRelations(RelPersonJvm.meta.PROP_FRIENDS, RelPersonJvm.meta).getOrElse { throw it }.toList()
            it("should be reflected in incomings") {
                incoming2.size `should equal` 2
                incoming2.map { it.id }.toSet() `should equal` setOf(p1.id, p3.id)
            }
        }

        on("removing relations from an entity") {
            val p1 = base.retrieve<RelPerson, Long>(listOf(0)).getOrElse { throw it }.first()
            val p2 = base.retrieve<RelPerson, Long>(listOf(1)).getOrElse { throw it }.first()
            val p3 = base.retrieve<RelPerson, Long>(listOf(2)).getOrElse { throw it }.first()
            p3.friends.clear()
            waiter.waitFor { it is EntityRelationsRemoved<*, *, *, *> && it.relations.any { it.source == p3 && it.target == p1 } }
            waiter.waitFor { it is EntityRelationsRemoved<*, *, *, *> && it.relations.any { it.source == p3 && it.target == p2 } }

            it("should reflect that") {
                p3.friends.size `should equal` 0
            }
            val incoming2 = (p2 as KIJvmEntity<*, *>).getIncomingRelations(RelPersonJvm.meta.PROP_FRIENDS, RelPersonJvm.meta).getOrElse { throw it }.toList()
            it("should be reflected in incomings") {
                incoming2.size `should equal` 1
                incoming2.map { it.id }.toSet() `should equal` setOf(p1.id)
            }

            val incoming1 = (p1 as KIJvmEntity<*, *>).getIncomingRelations(RelPersonJvm.meta.PROP_FRIENDS, RelPersonJvm.meta).getOrElse { throw it }.toList()
            it("should be reflected in incomings") {
                incoming1.size `should equal` 0
            }
        }

        on("adding a relation twice to an entity") {

        }
    }
})