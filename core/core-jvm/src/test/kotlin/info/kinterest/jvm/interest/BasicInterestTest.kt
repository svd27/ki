package info.kinterest.jvm.interest

import com.nhaarman.mockito_kotlin.whenever
import info.kinterest.*
import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.jvm.filter.LiveFilterWrapper
import info.kinterest.jvm.filter.filter
import info.kinterest.jvm.filter.tree.FilterTree
import info.kinterest.jvm.query.QueryManagerJvm
import info.kinterest.jvm.util.EventWaiter
import info.kinterest.meta.IdInfo
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.query.*
import info.kinterest.sorting.Ordering
import info.kinterest.sorting.asc
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.CompletableDeferred
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging
import norswap.utils.cast
import org.amshove.kluent.`should be instance of`
import org.amshove.kluent.`should equal`
import org.amshove.kluent.any
import org.amshove.kluent.mock
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.kodein.di.Kodein
import org.kodein.di.erased.bind
import org.kodein.di.erased.instance
import org.kodein.di.erased.singleton
import kotlin.reflect.KClass


interface InterestEntity : KIEntity<Long> {
    var name: String
}

open class InterestEntityImpl(override val _store: DataStore, override val id: Long, name: String) : InterestEntity {
    @Suppress("unused")
    constructor(ds: DataStore, id: Long) : this(ds, id, "")

    override var name: String = name
        set(value) {
            val old = field
            field = value
            setValue(_meta.props["name"]!!, old)
        }
    @Suppress("PropertyName")
    override val _meta: KIEntityMeta
        get() = Meta

    override fun asTransient(): KITransientEntity<Long> = Transient(this)

    @Suppress("UNCHECKED_CAST")
    override fun <V, P : KIProperty<V>> getValue(prop: P): V? = when (prop.name) {
        "name" -> name
        else -> DONTDOTHIS()
    } as V?

    override fun <V, P : KIProperty<V>> setValue(prop: P, v: V?) {
        (_store as DataStoreFacade).setValues(_meta, id, mapOf(prop to v))
    }

    override fun <V, P : KIProperty<V>> setValue(prop: P, version: Any, v: V?) = DONTDOTHIS()

    override fun toString(): String = "${this::class.simpleName}($id)"

    override fun equals(other: Any?): Boolean = if (other is InterestEntity) {
        id == other.id
    } else false

    override fun hashCode(): Int = InterestEntity::class.hashCode() + id.hashCode()

    object Meta : KIJvmEntityMeta(InterestEntityImpl::class, InterestEntity::class) {
        override val root: KClass<*>
            get() = InterestEntity::class
        override val parent: KClass<*>?
            get() = null
        override val versioned: Boolean
            get() = false

        override val hierarchy: List<KIEntityMeta> = listOf()
        override val idInfo: IdInfo = IdInfo(Long::class, false, null, null, true)
    }

    class Transient(override val _store: DataStore, override val id: Long, override var name: String) : KITransientEntity<Long>, InterestEntity {
        constructor(e: InterestEntity) : this(e._store, e.id, e.name)

        @Suppress("PropertyName")
        override val _meta: KIEntityMeta
            get() = Meta

        override fun asTransient(): KITransientEntity<Long> = Transient(this)
        @Suppress("UNCHECKED_CAST")
        override fun <V, P : KIProperty<V>> getValue(prop: P): V? = when (prop.name) {
            "name" -> name as V
            "id" -> id as V
            else -> throw IllegalArgumentException("unknown property $prop")
        }


        override fun <V, P : KIProperty<V>> setValue(prop: P, v: V?) {
            throw IllegalArgumentException()
        }

        override fun <V, P : KIProperty<V>> setValue(prop: P, version: Any, v: V?) {
            throw IllegalArgumentException()
        }
    }
}

class BasicInterestTest : Spek({
    given("a datastore") {
        val ds: DataStoreFacade = mock()
        val meta = InterestEntityImpl.Meta
        val entities = listOf(
                InterestEntityImpl(ds, 0, "a"),
                InterestEntityImpl(ds, 1, "w"),
                InterestEntityImpl(ds, 2, "d"))
        val mapped = entities.associateBy { it.id }
        whenever(ds.query(any<Query<InterestEntity, Long>>())).thenAnswer {
            @Suppress("UNCHECKED_CAST")
            val q: Query<InterestEntity, Long> = it.arguments[0] as Query<InterestEntity, Long>
            Try {
                CompletableDeferred(Try {
                    val projections = q.projections.filterIsInstance<EntityProjection<InterestEntity, Long>>()
                    val pmap = projections.map { projection ->
                        @Suppress("UNCHECKED_CAST")
                        val page = Page(Paging(0, 10),
                                entities.filter { q.f.matches(it) }.sortedWith(projection.ordering as Comparator<in InterestEntityImpl>), 0)
                        EntityProjectionResult(projection, page)
                    }.associateBy { it.projection }
                    @Suppress("UNCHECKED_CAST")
                    QueryResult(q, pmap as Map<Projection<InterestEntity, Long>, ProjectionResult<InterestEntity, Long>>)
                })
            }
        }
        val f = filter<InterestEntity, Long>(InterestEntityImpl.Meta) {
            parse("name > \"c\"", meta)
        }

        whenever(ds.setValues(any(), any(), any())).thenAnswer {
            val t = Try {
                val id = it.arguments[1] as Long
                @Suppress("UNCHECKED_CAST")
                val map = it.arguments[2] as Map<KIProperty<*>, Any?>
                val e = mapped[id]
                e?.let { entity ->
                    val upds = map.map {
                        val old = it.value
                        logger.debug { "setting ${it.key.name} value  ${it.value} on $e" }
                        EntityUpdated(it.key.cast(), old, e.name)
                    }
                    logger.debug { "sending $upds to $f" }
                    (f as? LiveFilterWrapper)?.digest(EntityUpdatedEvent(entity, upds))
                }
            }
            t.getOrElse { logger.debug(it) { } }
            CompletableDeferred(t)
        }

        val kodein = Kodein {
            bind<Dispatcher<EntityEvent<*, *>>>("entities") with singleton { Dispatcher<EntityEvent<*, *>>(CommonPool) }
            bind<FilterTree>() with singleton { FilterTree(kodein, 2) }
        }

        val ft: FilterTree by kodein.instance()

        val qm = QueryManagerJvm(ft, MetaProvider())
        runBlocking { qm.dataStores.send(StoreReady(ds)) }

        on("creating the interest") {
            val im = InterestManager(qm)
            @Suppress("UNCHECKED_CAST")
            val waiter = EventWaiter(im.events as Channel<InterestEvent<Interest<InterestEntity, Long>, InterestEntity, Long>>)
            val projection = EntityProjection<InterestEntity, Long>(Ordering.NATURAL.cast(), Paging(0, 10))
            val interest = im + Query<InterestEntity, Long>(f.cast(), listOf(projection))

            it("should be registered") {
                waiter.waitFor {
                    if (it is InterestCreated) logger.debug { "got created" }
                    it is InterestCreated
                }
                runBlocking { delay(100) }
                im.interests.size `should equal` 1
            }
            it("should have the proper length") {
                waiter.waitFor {
                    logger.debug { "wait for entities $it" }
                    it is InterestProjectionEvent && it.evts.count() > 0 && it.evts.any {
                        it is ProjectionLoaded && run {
                            val res = it.result
                            if (res is EntityProjectionResult) {
                                res.page.entities.size > 0
                            } else {
                                logger.debug { "nope" }
                                false
                            }
                        }
                    }
                }

                logger.debug { interest.result.projections }
                val res = interest.result.projections[projection] as EntityProjectionResult
                res.page.entities.size `should equal` 2
                res.page.entities.first().name `should equal` "w"
            }
        }

        on("changing the ordering") {
            val im = InterestManager(qm)
            @Suppress("UNCHECKED_CAST")
            val waiter = EventWaiter(im.events as Channel<InterestEvent<Interest<InterestEntity, Long>, InterestEntity, Long>>)
            val projection = EntityProjection<InterestEntity, Long>(Ordering(listOf(InterestEntityImpl.Meta.props["name"]!!.asc())), Paging(0, 100))
            val interest = im + Query(f.cast(), listOf(projection))

            it("should return according to orderin") {
                waiter.waitFor {
                    it is InterestProjectionEvent && it.evts.any {
                        it is ProjectionLoaded && it.projection is EntityProjection
                    }
                }
                runBlocking { delay(100) }
                interest.result.projections[projection] `should be instance of` EntityProjectionResult::class
                val proj = interest.result.projections[projection] as EntityProjectionResult
                proj.page.entities.size `should equal` 2
                proj.page.entities.first().name `should equal` "d"
            }
        }

        on("changing the value of an excluded entity") {
            val im = InterestManager(qm)
            @Suppress("UNCHECKED_CAST")
            val waiter = EventWaiter(im.events as Channel<InterestEvent<Interest<InterestEntity, Long>, InterestEntity, Long>>)
            val projection = EntityProjection(Ordering<InterestEntity, Long>(listOf(InterestEntityImpl.Meta.props["name"]!!.asc())), Paging(0, 100))
            val interest = im + Query(f.cast(), listOf(projection))
            entities.firstOrNull { it.name == "a" }?.let { it.name = "e" }

            it("after updating an entity") {
                waiter.waitFor {
                    it is InterestProjectionEvent && it.evts.any {
                        it is ProjectionLoaded && it.projection.let {
                            it is EntityProjection
                        }
                    }
                }
                logger.debug { "interest entities ${interest.result.projections[projection]}" }
                interest.result.projections[projection] `should be instance of` EntityProjectionResult::class
                val proj = interest.result.projections[projection] as EntityProjectionResult
                proj.page.entities.size `should equal` 3
                proj.page.entities.first().name `should equal` "d"
                proj.page.entities[1].name `should equal` "e"
            }
        }
        runBlocking { delay(100) }
    }
}) {
    companion object : KLogging()
}