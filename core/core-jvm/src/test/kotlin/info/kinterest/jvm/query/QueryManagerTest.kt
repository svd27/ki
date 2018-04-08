package info.kinterest.jvm.query

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import info.kinterest.StoreReady
import info.kinterest.cast
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.filter.StaticEntityFilter
import info.kinterest.jvm.filter.tree.FilterTree
import info.kinterest.jvm.interest.InterestEntity
import info.kinterest.jvm.interest.InterestEntityImpl
import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.query.Query
import info.kinterest.sorting.Ordering
import info.kinterest.sorting.asc
import kotlinx.coroutines.experimental.CompletableDeferred
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.yield
import mu.KLogging
import org.amshove.kluent.`should be true`
import org.amshove.kluent.`should equal`
import org.amshove.kluent.any
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.math.min

val logger = KLogging().logger

class QueryManagerTest : Spek({
    given("a query manager and three DataStores") {
        val pool = newFixedThreadPoolContext(4, "test")
        val ds1: DataStoreFacade = mock {
            on { name } doReturn "ds1"
        }
        val ds2: DataStoreFacade = mock {
            on { name } doReturn "ds2"
        }
        val ds3: DataStoreFacade = mock {
            on { name } doReturn "ds3"
        }

        val el1 = listOf(InterestEntityImpl(ds1, 1, "zc"), InterestEntityImpl(ds1, 2, "xa"))
        val el2 = listOf(InterestEntityImpl(ds1, 4, "zb"), InterestEntityImpl(ds1, 3, "za"))
        val el3 = listOf(InterestEntityImpl(ds1, 5, "wx"), InterestEntityImpl(ds1, 6, "fg"))

        whenever(ds1.retrieveLenient<InterestEntity, Long>(any(), any())).thenAnswer {
            Try {
                CompletableDeferred(Try { el1 })
            }
        }

        whenever(ds2.retrieveLenient<InterestEntity, Long>(any(), any())).thenAnswer {
            Try {
                CompletableDeferred(Try { el2 })
            }
        }

        whenever(ds3.retrieveLenient<InterestEntity, Long>(any(), any())).thenAnswer {
            Try {
                CompletableDeferred(Try { el3 })
            }
        }

        whenever(ds1.query<InterestEntity, Long>(any())).thenAnswer {
            @Suppress("UNCHECKED_CAST")
            val query = it.arguments[0] as Query<InterestEntity, Long>
            logger.debug { "ds1: $query" }
            Try {
                CompletableDeferred(Try {
                    @Suppress("UNCHECKED_CAST")
                    val drop = el1.sortedWith(query.ordering as Comparator<in InterestEntityImpl>).drop(query.page.offset)
                    val entites = drop.subList(0, min(query.page.size, drop.size))
                    Page(query.page, entites, if (entites.size >= query.page.size) 1 else 0)
                })
            }
        }

        whenever(ds2.query<InterestEntity, Long>(any())).thenAnswer {
            @Suppress("UNCHECKED_CAST")
            val query = it.arguments[0] as Query<InterestEntity, Long>
            logger.debug { "ds2: $query" }
            Try {
                CompletableDeferred(Try {
                    @Suppress("UNCHECKED_CAST")
                    val drop = el2.sortedWith(query.ordering as Comparator<in InterestEntityImpl>).drop(query.page.offset)
                    val entites = drop.subList(0, min(query.page.size, drop.size))
                    Page(query.page, entites, if (entites.size >= query.page.size) 1 else 0)
                })
            }
        }

        whenever(ds3.query<InterestEntity, Long>(any())).thenAnswer {
            @Suppress("UNCHECKED_CAST")
            val query = it.arguments[0] as Query<InterestEntity, Long>
            logger.debug { "ds3: $query" }
            Try {
                CompletableDeferred(Try {
                    @Suppress("UNCHECKED_CAST")
                    val drop = el3.sortedWith(query.ordering as Comparator<in InterestEntityImpl>).drop(query.page.offset)
                    val entites = drop.subList(0, min(query.page.size, drop.size))
                    Page(query.page, entites, if (entites.size >= query.page.size) 1 else 0)
                })
            }
        }

        val meta = InterestEntityImpl.Companion.Meta


        val qm = QueryManagerJvm(FilterTree(Dispatcher(), 2))
        runBlocking {
            qm.dataStores.send(StoreReady(ds1))
            qm.dataStores.send(StoreReady(ds2))
            qm.dataStores.send(StoreReady(ds3))
            yield()
        }

        on("sending the DataStores") {
            logger.debug { "checking DataStores" }
            it("should contain the proper stores") {
                qm.stores `should equal` setOf(ds1, ds2, ds3)
            }
        }



        on("retrieving from one DataStore") {
            logger.debug { "retrieving from one DataStores" }
            val tretrieve = qm.retrieve<InterestEntity, Long>(meta, setOf(1, 2, 3, 4, 5, 6), setOf(ds1)).getOrElse { throw it }
            val res = runBlocking { tretrieve.await() }
            it("should not fail") {
                res.isSuccess.`should be true`()
            }
            val es = res.getOrElse { throw it }
            it("should return the expected result") {
                es.toSet() `should equal` el1.toSet()
            }
        }

        on("retrieving from another DataStore") {
            logger.debug { "retrieving from another DataStores" }
            val tretrieve = qm.retrieve<InterestEntity, Long>(meta, setOf(1, 2, 3, 4, 5, 6), setOf(ds2)).getOrElse { throw it }
            val res = runBlocking { tretrieve.await() }
            it("should not fail") {
                res.isSuccess.`should be true`()
            }
            val es = res.getOrElse { throw it }
            it("should return the expected result") {
                es.toSet() `should equal` el2.toSet()
            }
        }

        on("retrieving from two DataStores") {
            logger.debug { "retrieving from two DataStores" }
            val tretrieve = qm.retrieve<InterestEntity, Long>(meta, setOf(1, 2, 3, 4, 5, 6), setOf(ds1, ds3)).getOrElse { throw it }
            val res = runBlocking { tretrieve.await() }
            it("should not fail") {
                res.isSuccess.`should be true`()
            }
            val es = res.getOrElse { throw it }
            it("should return the expected result") {
                es.toSet() `should equal` (el1 + el3).toSet()
            }
        }

        on("querying one DataStore") {
            logger.debug { "querying from one DataStores" }
            val res = qm.query(Query<InterestEntity, Long>(EntityFilter.FilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6), InterestEntityImpl.Companion.Meta)).cast(), Ordering.NATURAL.cast(), Paging(0, 100), setOf(ds1)))
            val td = runBlocking { res.getOrElse { throw it }.await() }
            it("should be successfull") {
                res.isSuccess.`should be true`()
                td.isSuccess.`should be true`()
            }

            val page = td.getOrElse { throw it }

            it("should contain the proper result") {
                page.entites.toSet() `should equal` el1.toSet()
            }
        }

        on("querying two DataStores") {
            logger.debug { "querying from two DataStores" }
            val res = qm.query(Query<InterestEntity, Long>(EntityFilter.FilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6), InterestEntityImpl.Companion.Meta)).cast(), Ordering.NATURAL.cast(), Paging(0, 100), setOf(ds1, ds3)))
            val td = runBlocking { res.getOrElse { throw it }.await() }
            it("should be successful") {
                res.isSuccess.`should be true`()
                td.isSuccess.`should be true`()
            }

            val page = td.getOrElse { throw it }

            it("should contain the proper result") {
                logger.debug { page }
                page.entites.toSet() `should equal` (el1 + el3).toSet()
            }
        }

        on("querying two DataStores with a page") {
            logger.debug { "querying from two DataStores with page" }
            val res = qm.query(Query<InterestEntity, Long>(EntityFilter.FilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6), InterestEntityImpl.Companion.Meta)).cast(), Ordering.NATURAL.cast(), Paging(0, 2), setOf(ds1, ds3)))
            val td = runBlocking(pool) { res.getOrElse { throw it }.await() }
            it("should be successful") {
                res.isSuccess.`should be true`()
                td.isSuccess.`should be true`()
            }

            val page = td.getOrElse { throw it }

            it("should contain the proper result") {
                logger.debug { page }
                page.entites.toSet() `should equal` (el1).toSet()
            }
        }

        on("querying two DataStores with the next page") {
            logger.debug { "querying from two DataStores with next page" }
            val res = qm.query(Query<InterestEntity, Long>(EntityFilter.FilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6), InterestEntityImpl.Companion.Meta)).cast(), Ordering.NATURAL.cast(), Paging(0, 2).next, setOf(ds1, ds3)))
            val td = runBlocking { res.getOrElse { throw it }.await() }
            it("should be successfull") {
                res.isSuccess.`should be true`()
                td.isSuccess.`should be true`()
            }

            val page = td.getOrElse { throw it }

            it("should contain the proper result") {
                logger.debug { page }
                page.entites.toSet() `should equal` (el3).toSet()
            }
        }

        on("querying three DataStores with a page and an ordering") {
            logger.debug { "querying from two DataStores with page and ordering" }
            val res = qm.query(Query<InterestEntity, Long>(EntityFilter.FilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6), InterestEntityImpl.Companion.Meta)).cast(), Ordering(listOf(InterestEntityImpl.Companion.Meta.props["name"]!!.asc())), Paging(0, 2), setOf(ds1, ds3, ds2)))
            val td = runBlocking(pool) { res.getOrElse { throw it }.await() }
            it("should be successfull") {
                res.isSuccess.`should be true`()
                td.isSuccess.`should be true`()
            }

            val page = td.getOrElse { throw it }

            it("should contain the proper result") {
                logger.debug { page }
                page.entites.toSet() `should equal` (el3).toSet()
                page.entites.first().name `should equal` "fg"
                page.entites[1].name `should equal` "wx"
            }

            val resNext = qm.query(Query<InterestEntity, Long>(EntityFilter.FilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6), InterestEntityImpl.Companion.Meta)).cast(), Ordering(listOf(InterestEntityImpl.Companion.Meta.props["name"]!!.asc())), Paging(0, 2).next, setOf(ds1, ds3, ds2)))
            val dn = runBlocking(pool) { resNext.getOrElse { throw it }.await() }
            val pNext = dn.getOrElse { throw it }

            it("the next page should contain the proper result") {
                logger.debug { "pNext: $pNext" }
                pNext.entites.map { it.name }.toSet() `should equal` setOf("xa", "za")
            }

            val paging = (Paging(0, 2).next).next
            val resLast = qm.query(Query<InterestEntity, Long>(EntityFilter.FilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6), InterestEntityImpl.Companion.Meta)).cast(), Ordering(listOf(InterestEntityImpl.Companion.Meta.props["name"]!!.asc())), paging, setOf(ds1, ds3, ds2)))
            val dl = runBlocking(pool) { resLast.getOrElse { throw it }.await() }
            val pLast = dl.getOrElse { throw it }

            it("the last page should contain the proper result") {
                logger.debug { "pLast: $pLast" }
                pLast.entites.map { it.name } `should equal` listOf("zb", "zc")
            }
        }


    }
}) {
    companion object : KLogging()
}