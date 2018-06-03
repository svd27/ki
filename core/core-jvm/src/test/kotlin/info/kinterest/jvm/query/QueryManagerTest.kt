package info.kinterest.jvm.query

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import info.kinterest.EntityEvent
import info.kinterest.MetaProvider
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
import info.kinterest.query.*
import info.kinterest.sorting.Ordering
import info.kinterest.sorting.asc
import kotlinx.coroutines.experimental.CompletableDeferred
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.yield
import mu.KLogging
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.kodein.di.Kodein
import org.kodein.di.erased.bind
import org.kodein.di.erased.instance
import org.kodein.di.erased.singleton
import kotlin.math.min

val meta1 = InterestEntityImpl.Meta

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
                    val ps = query.projections.map {
                        val page = if (it is EntityProjection) {
                            @Suppress("UNCHECKED_CAST")
                            val drop = el1.sortedWith(it.ordering as Comparator<in InterestEntityImpl>).drop(it.paging.offset)
                            val entites = drop.subList(0, min(it.paging.size, drop.size))
                            Page(it.paging, entites, if (entites.size >= it.paging.size) 1 else 0)
                        } else throw Exception()
                        EntityProjectionResult(it, page)
                    }
                    @Suppress("UNCHECKED_CAST")
                    QueryResult(query, ps.associateBy { it.projection } as Map<Projection<InterestEntity, Long>, ProjectionResult<InterestEntity, Long>>)

                })
            }
        }

        whenever(ds2.query<InterestEntity, Long>(any())).thenAnswer {
            @Suppress("UNCHECKED_CAST")
            val query = it.arguments[0] as Query<InterestEntity, Long>
            logger.debug { "ds2: $query" }
            Try {
                CompletableDeferred(Try {
                    val ps = query.projections.map {
                        val page = if (it is EntityProjection) {
                            @Suppress("UNCHECKED_CAST")
                            val drop = el2.sortedWith(it.ordering as Comparator<in InterestEntityImpl>).drop(it.paging.offset)
                            val entites = drop.subList(0, min(it.paging.size, drop.size))
                            Page(it.paging, entites, if (entites.size >= it.paging.size) 1 else 0)
                        } else throw Exception()
                        EntityProjectionResult(it, page)
                    }
                    @Suppress("UNCHECKED_CAST")
                    QueryResult(query, ps.associateBy { it.projection } as Map<Projection<InterestEntity, Long>, ProjectionResult<InterestEntity, Long>>)
                })
            }
        }

        whenever(ds3.query<InterestEntity, Long>(any())).thenAnswer {
            @Suppress("UNCHECKED_CAST")
            val query = it.arguments[0] as Query<InterestEntity, Long>
            logger.debug { "ds3: $query" }
            Try {
                CompletableDeferred(Try {
                    val ps = query.projections.map {
                        val page = if (it is EntityProjection) {
                            @Suppress("UNCHECKED_CAST")
                            val drop = el3.sortedWith(it.ordering as Comparator<in InterestEntityImpl>).drop(it.paging.offset)
                            val entites = drop.subList(0, min(it.paging.size, drop.size))
                            Page(it.paging, entites, if (entites.size >= it.paging.size) 1 else 0)
                        } else throw Exception()
                        EntityProjectionResult(it, page)
                    }
                    @Suppress("UNCHECKED_CAST")
                    QueryResult(query, ps.associateBy { it.projection } as Map<Projection<InterestEntity, Long>, ProjectionResult<InterestEntity, Long>>)
                })
            }
        }

        val meta = InterestEntityImpl.Meta

        val kodein = Kodein {
            bind<Dispatcher<EntityEvent<*, *>>>("entities") with singleton { Dispatcher<EntityEvent<*, *>>() }
            bind<FilterTree>() with singleton { FilterTree(kodein, 2) }
        }
        val ft: FilterTree by kodein.instance()
        val qm = QueryManagerJvm(ft, MetaProvider())
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
                logger.debug { es }
                es.toSet() `should equal` (el1 + el3).toSet()
            }
        }

        val projection1 = EntityProjection<InterestEntity, Long>(Ordering.NATURAL.cast(), Paging(0, 100))
        on("querying one DataStore") {
            logger.debug { "querying from one DataStores" }
            val res = qm.query(Query<InterestEntity, Long>(EntityFilter.LiveFilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6),
                    InterestEntityImpl.Meta)).cast(),
                    listOf(projection1), setOf(ds1)))
            val td = runBlocking { res.getOrElse { throw it }.await() }
            it("should be successfull") {
                res.isSuccess.`should be true`()
                td.isSuccess.`should be true`()
            }

            val queryResult = td.getOrElse { throw it }

            it("should contain the proper result") {
                queryResult.projections.size `should equal` 1
                queryResult.projections[projection1].`should not be null`()
                queryResult.projections[projection1] `should be instance of` EntityProjectionResult::class
                val proj = queryResult.projections[projection1] as EntityProjectionResult
                proj.page.entities.toSet() `should equal` el1.toSet()
            }
        }

        on("querying two DataStores") {
            logger.debug { "querying from two DataStores" }
            val res = qm.query(Query<InterestEntity, Long>(
                    EntityFilter.LiveFilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6),
                            InterestEntityImpl.Meta)).cast(),
                    listOf(projection1), setOf(ds1, ds3)))
            val td = runBlocking { res.getOrElse { throw it }.await() }
            it("should be successful") {
                res.isSuccess.`should be true`()
                td.isSuccess.`should be true`()
            }

            val queryResult = td.getOrElse { throw it }

            it("should contain the proper result") {
                logger.debug { queryResult }
                queryResult.projections[projection1] `should be instance of` EntityProjectionResult::class
                val proj = queryResult.projections[projection1] as EntityProjectionResult
                proj.page.entities.toSet() `should equal` (el1 + el3).toSet()
            }
        }

        on("querying two DataStores with a page") {
            logger.debug { "querying from two DataStores with page" }
            val projection = EntityProjection<InterestEntity, Long>(Ordering.NATURAL.cast(), Paging(0, 2))
            val res = qm.query(Query<InterestEntity, Long>(EntityFilter.LiveFilterWrapper(
                    StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6), InterestEntityImpl.Meta)),
                    listOf(projection), setOf(ds1, ds3)))
            val td = runBlocking(pool) { res.getOrElse { throw it }.await() }
            it("should be successful") {
                res.isSuccess.`should be true`()
                td.isSuccess.`should be true`()
            }

            val queryResult = td.getOrElse { throw it }

            it("should contain the proper result") {
                logger.debug { queryResult }
                queryResult.projections[projection] `should be instance of` EntityProjectionResult::class
                val proj = queryResult.projections[projection] as EntityProjectionResult
                proj.page.entities.toSet() `should equal` (el1).toSet()
            }
        }

        on("querying two DataStores with the next page") {
            logger.debug { "querying from two DataStores with next page" }
            val projection = EntityProjection<InterestEntity, Long>(Ordering.NATURAL.cast(), Paging(0, 2).next)
            val res = qm.query(Query<InterestEntity, Long>(
                    EntityFilter.LiveFilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6), InterestEntityImpl.Meta)),
                    listOf(projection), setOf(ds1, ds3)))
            val td = runBlocking { res.getOrElse { throw it }.await() }
            it("should be successfull") {
                res.isSuccess.`should be true`()
                td.isSuccess.`should be true`()
            }

            val queryResult = td.getOrElse { throw it }

            it("should contain the proper result") {
                logger.debug { queryResult }
                queryResult.projections[projection] `should be instance of` EntityProjectionResult::class
                val proj = queryResult.projections[projection] as EntityProjectionResult
                proj.page.entities.toSet() `should equal` (el3).toSet()
            }
        }

        on("querying three DataStores with a page and an ordering") {
            logger.debug { "querying from two DataStores with page and ordering" }
            val projection = EntityProjection<InterestEntity, Long>(Ordering(listOf(meta1.props["name"]!!.asc())), Paging(0, 2))
            val query = Query(
                    EntityFilter.LiveFilterWrapper(StaticEntityFilter<InterestEntity, Long>(setOf(1, 2, 3, 4, 5, 6), InterestEntityImpl.Meta)),
                    listOf(projection), setOf(ds1, ds3, ds2))
            val res = qm.query(query)
            val td = runBlocking(pool) { res.getOrElse { throw it }.await() }
            it("should be successfull") {
                res.isSuccess.`should be true`()
                td.isSuccess.`should be true`()
            }

            val queryResult = td.getOrElse { throw it }

            it("should contain the proper result") {
                logger.debug { queryResult }
                queryResult.projections[projection] `should be instance of` EntityProjectionResult::class
                val proj = queryResult.projections[projection] as EntityProjectionResult
                proj.page.entities.toSet() `should equal` (el3).toSet()
                proj.page.entities.first().name `should equal` "fg"
                proj.page.entities[1].name `should equal` "wx"
            }

            val entityProjection1 = EntityProjection(projection.ordering, projection.paging.next)
            val resNext = qm.query(query.copy(projections = listOf(entityProjection1)))
            val dn = runBlocking(pool) { resNext.getOrElse { throw it }.await() }
            val pNext = dn.getOrElse { throw it }

            it("the next page should contain the proper result") {
                logger.debug { "pNext: $pNext" }
                pNext.projections[entityProjection1] `should be instance of` EntityProjectionResult::class
                val proj = pNext.projections[entityProjection1] as EntityProjectionResult
                proj.page.entities.map { it.name } `should equal` listOf("xa", "za")
            }

            val paging = (Paging(0, 2).next).next
            val entityProjection = EntityProjection<InterestEntity, Long>(
                    Ordering(listOf(meta1.props["name"]!!.asc())),
                    paging)
            val resLast = qm.query(
                    query.copy(projections = listOf(
                            entityProjection))
            )
            val dl = runBlocking(pool) { resLast.getOrElse { throw it }.await() }
            val pLast = dl.getOrElse { throw it }

            it("the last page should contain the proper result") {
                logger.debug { "pLast: $pLast" }
                pLast.projections[entityProjection] `should be instance of` EntityProjectionResult::class
                val proj = pLast.projections[entityProjection] as EntityProjectionResult
                proj.page.entities.map { it.name } `should equal` listOf("zb", "zc")
            }
        }
    }
    logger.debug { "Done." }
}) {
    companion object : KLogging()
}