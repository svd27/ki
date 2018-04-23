package info.kinterest.datastores.jvm.memory

import info.kinterest.InterestCreated
import info.kinterest.InterestProjectionEvent
import info.kinterest.ProjectionChanged
import info.kinterest.ProjectionLoaded
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.memory.jvm.RelPersonJvm
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.filter.LiveFilterWrapper
import info.kinterest.jvm.interest.InterestManager
import info.kinterest.jvm.query.DistinctDiscriminators
import info.kinterest.jvm.query.QueryManagerJvm
import info.kinterest.jvm.util.EventWaiter
import info.kinterest.paging.Paging
import info.kinterest.query.*
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging
import norswap.utils.cast
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

class ProjectionTest : Spek({
    val base = BaseMemTest(object : DataStoreConfig {
        override val name: String
            get() = "test"
        override val type: String
            get() = "jvm.mem"
        override val config: Map<String, Any?> = mapOf()
    })
    base.metaProvider.register(RelPersonJvm.meta)
    given("a datastore with some entities") {
        repeat(5) {
            base.create<RelPerson, Long>(RelPersonJvm.Companion.Transient(base.ds, it.toLong(), "AA", mutableSetOf()))
        }
        repeat(7) {
            base.create<RelPerson, Long>(RelPersonJvm.Companion.Transient(base.ds, (it + 5).toLong(), "BB", mutableSetOf()))
        }
        on("querying a BucketProjection on them") {
            val filter = EntityFilter.AllFilter<RelPerson, Long>(RelPersonJvm.meta)
            val discriminators = DistinctDiscriminators<RelPerson, Long, String>(RelPersonJvm.meta, RelPersonJvm.meta.PROP_NAME)
            val en = EntityProjection<RelPerson, Long>(Ordering.NATURAL.cast(), Paging(0, 10))
            val count = CountProjection<RelPerson, Long>(RelPersonJvm.meta.PROP_NAME, null)
            val bucketProjection = BucketProjection<RelPerson, Long, String>(null, listOf(en, count), discriminators, RelPersonJvm.meta.PROP_NAME)
            val q = Query(LiveFilterWrapper(filter), setOf(bucketProjection))
            val rd = base.ds.query(q).getOrElse { throw it }
            val res = runBlocking { rd.await().getOrElse { throw it } }
            val bucketsPath = Path(discriminators.name, null)
            fun pathFor(n: String) = Path("${bucketsPath.name} = $n", bucketsPath)
            it("should have a proper result") {
                res.projections.size `should equal` 1
                res.projections.values.first() `should be instance of` BucketProjectionResult::class
                @Suppress("UNCHECKED_CAST")
                val bucket = res.projections.values.first() as BucketProjectionResult<RelPerson, Long, String>
                bucket.buckets.size `should equal` 2


                bucket.buckets.values.map { it.projection.path }.toSet() `should equal` setOf(pathFor("AA"), pathFor("BB"))
            }
            @Suppress("UNCHECKED_CAST")
            val bucket = res.projections.values.first() as BucketProjectionResult<RelPerson, Long, String>
            val ret = runBlocking { bucket.retrieve(pathFor("AA"), q, base.ds.qm).getOrElse { throw it }.await() }.getOrElse { throw it }
            it("should have a proper bucket") {
                ret `should be instance of` ProjectionBucketResult::class
                @Suppress("UNCHECKED_CAST")
                val bucketResult = ret as ProjectionBucketResult<RelPerson, Long, String>
                bucketResult.result.size `should equal` 2
                val er = runBlocking { bucketResult.retrieve(Path(en.name, pathFor("AA")), q, base.ds.qm).getOrElse { throw it }.await() }.getOrElse { throw it }
                er `should be instance of` EntityProjectionResult::class
                val ep = er as EntityProjectionResult<RelPerson, Long>
                ep.page.size `should equal` 5
            }

            it("should have a proper count") {
                ret `should be instance of` ProjectionBucketResult::class
                @Suppress("UNCHECKED_CAST")
                val bucketResult = ret as ProjectionBucketResult<RelPerson, Long, String>
                val cr = runBlocking { bucketResult.retrieve(Path(count.name, pathFor("AA")), q, base.ds.qm).getOrElse { throw it }.await() }.getOrElse { throw it }
                cr `should be instance of` CountProjectionResult::class
                val cp = cr as CountProjectionResult
                cp.count `should equal` 5
            }
        }



        on("creating a new entity") {
            val filter = EntityFilter.AllFilter<RelPerson, Long>(RelPersonJvm.meta)
            val discriminators = DistinctDiscriminators<RelPerson, Long, String>(RelPersonJvm.meta, RelPersonJvm.meta.PROP_NAME)
            val en = EntityProjection<RelPerson, Long>(Ordering.NATURAL.cast(), Paging(0, 10))
            val count = CountProjection<RelPerson, Long>(RelPersonJvm.meta.PROP_NAME, null)
            val bucketProjection = BucketProjection(null, listOf(en, count), discriminators, RelPersonJvm.meta.PROP_NAME)
            val q = Query(LiveFilterWrapper(filter), setOf(bucketProjection))
            val mgr = InterestManager(base.ds.qm as QueryManagerJvm)
            val waiterInterest = EventWaiter(mgr.events)
            val interest = mgr + q
            it("should have a proper interest") {
                val wait = { waiterInterest.waitFor { it is InterestCreated } }
                wait `should not throw` AnyException
            }
            val bucketsPath = Path(discriminators.name, null)
            fun pathFor(n: String) = Path("${bucketsPath.name} = $n", bucketsPath)
            waiterInterest.waitFor { it is InterestProjectionEvent && it.evts.any { it is ProjectionLoaded && it.projection.path == bucketProjection.path } }
            val entity = base.create<RelPerson, Long>(RelPersonJvm.Companion.Transient(base.ds, 100, "AA", mutableSetOf()))
            it("should reflect the change") {
                val wait = {
                    waiterInterest.waitFor {
                        logger.debug { it }
                        it is InterestProjectionEvent && it.evts.any { it is ProjectionChanged && it.projection.path == pathFor("AA") }
                    }
                }
                wait `should not throw` AnyException
                val bres = interest.result.projections[bucketProjection]
                bres `should be instance of` BucketProjectionResult::class
                @Suppress("UNCHECKED_CAST")
                val br = bres as BucketProjectionResult<RelPerson, Long, String>
                br.buckets.values.any { it is ReloadProjectionResult }.`should be true`()
                val res = runBlocking { interest.result.retrieve(pathFor("AA"), base.ds.qm).getOrElse { throw it }.await() }.getOrElse { throw it }
                res `should be instance of` ProjectionBucketResult::class
                val rb = res as ProjectionBucketResult<RelPerson, Long, String>
                rb.result.values.any { it is CountProjectionResult }.`should be true`()
                val cr = rb.result.values.first { it is CountProjectionResult } as CountProjectionResult<RelPerson, Long>
                cr.count `should equal` 6
            }

            it("should create a new bucket") {
                base.create<RelPerson, Long>(RelPersonJvm.Companion.Transient(base.ds, 101, "CC", mutableSetOf()))
                val wait = {
                    waiterInterest.waitFor {
                        logger.debug { it }
                        it is InterestProjectionEvent && it.evts.any { it is ProjectionChanged && it.projection.path == bucketsPath }
                    }
                }
                wait `should not throw` AnyException
                val bres = interest.result.projections[bucketProjection]
                bres `should be instance of` ReloadProjectionResult::class


                val res = runBlocking { interest.result.retrieve(pathFor("CC"), base.ds.qm).getOrElse { throw it }.await() }.getOrElse { throw it }
                res `should be instance of` ProjectionBucketResult::class
                val rb = res as ProjectionBucketResult<RelPerson, Long, String>
                rb.result.values.any { it is CountProjectionResult }.`should be true`()
                val cr = rb.result.values.first { it is CountProjectionResult } as CountProjectionResult<RelPerson, Long>
                cr.count `should equal` 1
            }
        }

        on("deleting an entity") {
            val filter = EntityFilter.AllFilter<RelPerson, Long>(RelPersonJvm.meta)
            val discriminators = DistinctDiscriminators<RelPerson, Long, String>(RelPersonJvm.meta, RelPersonJvm.meta.PROP_NAME)
            val en = EntityProjection<RelPerson, Long>(Ordering.NATURAL.cast(), Paging(0, 10))
            val count = CountProjection<RelPerson, Long>(RelPersonJvm.meta.PROP_NAME, null)
            val bucketProjection = BucketProjection(null, listOf(en, count), discriminators, RelPersonJvm.meta.PROP_NAME)
            val q = Query(LiveFilterWrapper(filter), setOf(bucketProjection))
            val mgr = InterestManager(base.ds.qm as QueryManagerJvm)
            val waiterInterest = EventWaiter(mgr.events)
            val interest = mgr + q
            it("should have a proper interest") {
                val wait = { waiterInterest.waitFor { it is InterestCreated } }
                wait `should not throw` AnyException
            }
            val bucketsPath = Path(discriminators.name, null)
            fun pathFor(n: String) = Path("${bucketsPath.name} = $n", bucketsPath)
            waiterInterest.waitFor { it is InterestProjectionEvent && it.evts.any { it is ProjectionLoaded && it.projection.path == bucketProjection.path } }
            val pathCount = Path(count.name, pathFor("BB"))
            val bbres = interest.result.retrieve(pathCount, base.ds.qm)
            val bb = runBlocking { bbres.getOrElse { throw it }.await() }.getOrElse { throw it }
            it("shold have a proper projection") {
                bb `should be instance of` CountProjectionResult::class
                val cr = bb as CountProjectionResult
                cr.result `should equal` 7
            }
            val tryRetrieve = base.retrieve<RelPerson, Long>(listOf(9.toLong()))
            tryRetrieve.isSuccess.`should be true`()
            base.ds.delete(RelPersonJvm.meta, tryRetrieve.getOrElse { throw it })
            it("should change our projection") {
                val wait = { waiterInterest.waitFor { it is InterestProjectionEvent && it.evts.any { it is ProjectionChanged && it.projection.name.contains("BB") } } }
                wait `should not throw` AnyException
            }
            val bbres1 = interest.result.retrieve(pathCount, base.ds.qm)
            it("should retrieve") {
                bbres1.isSuccess.`should be true`()
            }
            val bb1 = runBlocking { bbres1.getOrElse { throw it }.await().getOrElse { throw it } }
            it("shold have a changed projection") {
                bb1 `should be instance of` CountProjectionResult::class
                val cr = bb1 as CountProjectionResult
                cr.result `should equal` 6
            }

        }
    }
}) {
    companion object : KLogging()
}