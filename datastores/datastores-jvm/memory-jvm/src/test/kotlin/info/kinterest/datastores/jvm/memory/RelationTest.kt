package info.kinterest.datastores.jvm.memory

import info.kinterest.EntityEvent
import info.kinterest.EntityRelationsAdded
import info.kinterest.EntityRelationsRemoved
import info.kinterest.KIEntity
import info.kinterest.datastores.jvm.memory.jvm.RelPersonJvm
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.annotations.Relation
import info.kinterest.jvm.datastores.DataStoreConfig
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.filter.filter
import info.kinterest.jvm.getIncomingRelations
import info.kinterest.jvm.tx.Transaction
import info.kinterest.jvm.tx.TransactionManager
import info.kinterest.jvm.tx.jvm.*
import info.kinterest.jvm.util.EventWaiter
import info.kinterest.paging.Paging
import info.kinterest.query.EntityProjection
import info.kinterest.query.Query
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.kodein.di.erased.instance

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
        val tm by base.kodein.instance<TransactionManager>()
        tm.txStore.name
//        Try.errorHandler = {
//            logger.debug(it) { }
//        }

        base.metaProvider.register(RelPersonJvm.meta)
        base.metaProvider.register(AddRelationTransactionJvm.meta)
        base.metaProvider.register(AddOutgoingRelationTransactionJvm.meta)
        base.metaProvider.register(BookRelationTransactionJvm.meta)
        base.metaProvider.register(RemoveRelationTransactionJvm.meta)
        base.metaProvider.register(RemoveOutgoingRelationTransactionJvm.meta)
        base.metaProvider.register(UnBookRelationTransactionJvm.meta)
        val subscription: Channel<EntityEvent<*, *>> = Channel()
        val waiter = EventWaiter(subscription)
        runBlocking { base.dispatcher.subscribing.send(subscription) }


        repeat(2) {
            base.create<RelPerson, Long>(RelPersonJvm.Transient(base.ds, it.toLong(), "${'A' + it}", mutableSetOf()))
        }
        on("addding a relation") {
            val p1 = base.retrieve<RelPerson, Long>(listOf(0)).getOrElse { throw it }.first()
            @Suppress("UNCHECKED_CAST")
            val p2 = base.retrieve<RelPerson, Long>(listOf(1)).getOrElse { throw it }.first() as KIJvmEntity<RelPerson, Long>
            val addres = base.ds.addRelation(info.kinterest.meta.Relation(RelPersonJvm.Meta.PROP_FRIENDS, p1, p2))

            it("should be reflected in the property") {
                addres.getOrElse { throw it }
                addres.isSuccess.`should be true`()
                val res = runBlocking { addres.getOrElse { throw it }.await().getOrElse { throw it } }
                res.`should be true`()
                @Suppress("UNCHECKED_CAST")
                val evt = waiter.waitFor {
                    val ev = it as EntityEvent<RelPerson, Long>
                    ev is EntityRelationsAdded<*, *, *, *> && ev.relation.target == p2
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
            val cr = base.create<RelPerson, Long>(RelPersonJvm.Transient(base.ds, 2, "${'A' + 2}", mutableSetOf(p1, p2)))


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


            it("should reflect that") {
                waiter.waitFor { it is EntityRelationsRemoved<*, *, *, *> && it.relation.let { it.source == p3 && it.target == p1 } }
                waiter.waitFor { it is EntityRelationsRemoved<*, *, *, *> && it.relation.let { it.source == p3 && it.target == p2 } }
                p3.friends.size `should equal` 0
            }

            it("should be reflected in incomings") {
                val incoming2 = (p2 as KIJvmEntity<*, *>).getIncomingRelations(RelPersonJvm.meta.PROP_FRIENDS, RelPersonJvm.meta).getOrElse { throw it }.toList()
                logger.debug { "incomings $incoming2" }
                incoming2.size `should equal` 1
                incoming2.map { it.id }.toSet() `should equal` setOf(p1.id)
            }


            it("should be reflected in incomings") {
                val incoming1 = (p1 as KIJvmEntity<*, *>).getIncomingRelations(RelPersonJvm.meta.PROP_FRIENDS, RelPersonJvm.meta).getOrElse { throw it }.toList()
                incoming1.size `should equal` 0
            }
        }

        on("adding a relation twice to an entity") {
            val p1 = base.retrieve<RelPerson, Long>(listOf(0)).getOrElse { throw it }.first()
            @Suppress("UNUSED_VARIABLE")
            val p2 = base.retrieve<RelPerson, Long>(listOf(1)).getOrElse { throw it }.first()
            val p3 = base.retrieve<RelPerson, Long>(listOf(2)).getOrElse { throw it }.first()
            p3.friends.add(p1)
            waiter.waitFor { it is EntityRelationsAdded<*, *, *, *> && it.relation.let { it.source == p3 && it.target == p1 } }
            p3.friends.add(p1)

            it("should only have one relation") {
                p3.friends.size `should equal` 1
                val incomingRelations = (p1 as KIJvmEntity<*, *>).getIncomingRelations(RelPersonJvm.meta.PROP_FRIENDS, RelPersonJvm.meta).getOrElse { throw it }
                logger.debug { "incoming $incomingRelations" }
                incomingRelations.count() `should equal` 1
            }
        }

        on("trying to delete an entity with incoming relations") {
            val p1 = base.retrieve<RelPerson, Long>(listOf(0)).getOrElse { throw it }.first()
            @Suppress("UNUSED_VARIABLE")
            val p2 = base.retrieve<RelPerson, Long>(listOf(1)).getOrElse { throw it }.first()
            val p3 = base.retrieve<RelPerson, Long>(listOf(2)).getOrElse { throw it }.first()
            val td = base.ds.delete(RelPersonJvm.Meta, listOf(p1)).getOrElse { throw it }.run { runBlocking { await() } }
            it("should fail") {
                td.isSuccess.`should be false`()
                val thr = { td.getOrElse { throw it } }
                thr.`should throw`(AnyException)
            }

            it("after removing the relation it should work") {
                val tf = filter<Transaction<*>, Long>(TransactionJvm.meta) {
                    EntityFilter.AllFilter<Transaction<*>, Long>(TransactionJvm.meta)
                }
                val qr = runBlocking { base.qm.query(Query(tf, listOf(EntityProjection<Transaction<*>, Long>(Ordering.natural(), Paging.ALL)), setOf(tm.txStore))).getOrElse { throw it }.await().getOrElse { throw it } }
                logger.debug { qr }
                p3.friends.remove(p1)
                waiter.waitFor { it is EntityRelationsRemoved<*, *, *, *> && it.relation.let { it.source == p3 && it.target == p1 } }
                p3.friends.add(p2)
                waiter.waitFor { it is EntityRelationsAdded<*, *, *, *> && it.relation.let { it.source == p3 && it.target == p2 } }

                val td1 = base.ds.delete(RelPersonJvm.Meta, listOf(p1)).getOrElse { throw it }.run { runBlocking { await() } }
                td1.getOrElse { throw it }
                td1.isSuccess.`should be true`()
            }

        }
    }
}) {
    companion object : KLogging()
}