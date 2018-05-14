package info.kinterest.jvm.events

import info.kinterest.*
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.filter.TestFilter
import info.kinterest.meta.IdInfo
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import org.amshove.kluent.`should equal`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.reflect.KClass

@Suppress("unused", "PropertyName")
class AnEntity(override val id: Long, var name: String, var score: Int) : KIEntity<Long> {
    @Suppress("UNUSED_PARAMETER")
    constructor(ds: DataStore, id: Long) : this(id, "", 0)

    override val _store: DataStore
        get() = DONTDOTHIS("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val _meta: KIEntityMeta
        get() = TestFilter.Meta

    companion object {
        val Meta = object : KIJvmEntityMeta(AnEntity::class, AnEntity::class) {
            override val root: KClass<*>
                get() = AnEntity::class
            override val parent: KClass<*>?
                get() = null
            override val versioned: Boolean
                get() = false
            override val hierarchy: List<KIEntityMeta> = listOf()
            override val idInfo: IdInfo = IdInfo(Long::class, false, null, null, true)
        }
    }

    override fun asTransient(): KITransientEntity<Long> {
        DONTDOTHIS("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V, P : KIProperty<V>> getValue(prop: P): V? {
        DONTDOTHIS("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V, P : KIProperty<V>> setValue(prop: P, v: V?) {
        DONTDOTHIS("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V, P : KIProperty<V>> setValue(prop: P, version: Any, v: V?) {
        DONTDOTHIS("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class UpdateEventTest : Spek({
    given("an entity") {
        val propName = AnEntity.Meta["name"]!!
        val propScore = AnEntity.Meta["score"]!!
        on("a simple update event") {
            val updates = listOf(EntityUpdated(propName.cast(), "a", "b"))
            val upd = EntityUpdatedEvent(AnEntity(0, "", 1), updates)
            val history = upd.history<String>(propName.cast())
            it("history should be correct") {
                history `should equal` listOf("a", "b")
            }
        }
        on("an update event with mixed properties") {
            val updates = listOf(
                    EntityUpdated(propName.cast(), "a", "b"),
                    EntityUpdated(propScore.cast(), 1, 2),
                    EntityUpdated(propName.cast(), "b", "c"))
            val upd = EntityUpdatedEvent(AnEntity(0, "", 1), updates)
            val history = upd.history<String>(propName.cast())
            it("history should be correct") {
                history `should equal` listOf("a", "b", "c")
            }
        }
    }
})