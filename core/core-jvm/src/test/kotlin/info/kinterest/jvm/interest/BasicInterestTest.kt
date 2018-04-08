package info.kinterest.jvm.interest

import com.nhaarman.mockito_kotlin.whenever
import info.kinterest.*
import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.jvm.filter.filter
import info.kinterest.jvm.filter.tree.FilterTree
import info.kinterest.jvm.query.QueryManagerJvm
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.query.Query
import info.kinterest.sorting.Ordering
import info.kinterest.sorting.asc
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import mu.KLogging
import norswap.utils.cast
import org.amshove.kluent.`should equal`
import org.amshove.kluent.any
import org.amshove.kluent.mock
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
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
        get() = Companion.Meta

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

    companion object {
        object Meta : KIJvmEntityMeta(InterestEntityImpl::class, InterestEntity::class) {
            override val root: KClass<*>
                get() = InterestEntity::class
            override val parent: KClass<*>?
                get() = null
            override val versioned: Boolean
                get() = false
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
}

class BasicInterestTest : Spek({
    given("a datastore") {
        val ds: DataStoreFacade = mock()
        val meta = InterestEntityImpl.Companion.Meta
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
                    @Suppress("UNCHECKED_CAST")
                    Page(Paging(0, 10), entities.filter { q.f.matches(it) }.sortedWith(q.ordering as Comparator<in InterestEntityImpl>), 0)
                })
            }
        }
        val f = filter<InterestEntity, Long>(InterestEntityImpl.Companion.Meta) {
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
                    f.digest(EntityUpdatedEvent(entity, upds))
                }
            }
            t.getOrElse { logger.debug(it) { } }
            CompletableDeferred(t)
        }

        val qm = QueryManagerJvm(FilterTree(Dispatcher(CommonPool), 2))
        runBlocking { qm.dataStores.send(StoreReady(ds)) }
        val im = InterestManager(qm)
        var evts: List<InterestEvent<Interest<InterestEntity, Long>, InterestEntity, Long>> = listOf()
        val pool: CoroutineDispatcher = newFixedThreadPoolContext(2, "test")
        val channel = Channel<Pair<Any, Int>>()
        launch(pool) {
            for (ev in im.events) {
                logger.debug { ev }
                @Suppress("UNCHECKED_CAST")
                evts += (ev as InterestEvent<Interest<InterestEntity, Long>, InterestEntity, Long>)
                if (ev is InterestContainedEvent<*, *, *>) {
                    logger.debug { "sending $ev as ${ev.interest.id}" }
                    channel.send(ev.interest.id to 1)
                }
            }
        }

        var read: Map<Any, Int> = mapOf()

        fun wait(id: Any, n: Int) {
            if (id in read && read[id]!! >= n) return
            runBlocking(pool) {
                for (i in channel) {
                    logger.debug { "increment ${read[i.first]} by ${i.second}" }
                    read += (i.first to ((read[i.first] ?: 0) + i.second))
                    if (read[i.first] ?: 0 >= n) break
                }
            }
        }


        on("creating the interest") {
            val interest = im + Query<InterestEntity, Long>(f.cast())
            wait(interest.id, 3)
            it("should be registered") {
                im.interests.size `should equal` 1
            }
            it("should have the proper length") {
                interest.entities.entites.size `should equal` 2
                interest.entities.entites.first().name `should equal` "w"
            }
        }

        on("changing the ordering") {
            val interest = im + Query<InterestEntity, Long>(f.cast())
            interest.ordering = Ordering(listOf(InterestEntityImpl.Companion.Meta.props["name"]!!.asc()))
            wait(interest.id, 3)
            interest.entities.entites.size `should equal` 2
            interest.entities.entites.first().name `should equal` "d"
        }

        on("changing the value of an excluded entity") {
            val interest = im + Query<InterestEntity, Long>(f.cast(), Ordering(listOf(InterestEntityImpl.Companion.Meta.props["name"]!!.asc())))
            entities.firstOrNull { it.name == "a" }?.let { it.name = "e" }
            wait(interest.id, 5)
            it("after updating an entity") {
                logger.debug { "interest entities ${interest.entities}" }
                interest.entities.entites.size `should equal` 3
                interest.entities.entites.first().name `should equal` "d"
                interest.entities.entites[1].name `should equal` "e"
            }
        }
    }
}) {
    companion object : KLogging()
}