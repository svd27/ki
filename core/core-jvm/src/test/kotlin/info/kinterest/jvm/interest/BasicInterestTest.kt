package info.kinterest.jvm.interest

import com.nhaarman.mockito_kotlin.whenever
import info.kinterest.DONTDOTHIS
import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.KITransientEntity
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.datastores.DataStoreFacade
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.query.Query
import org.amshove.kluent.any
import org.amshove.kluent.mock
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import kotlin.reflect.KClass


interface InterestEntity : KIEntity<Long> {
    val name: String
}

open class InterestEntityImpl(override val _store: DataStore, override val id: Long, override val name: String) : InterestEntity {
    override val _meta: KIEntityMeta
        get() = Companion.Meta

    override fun asTransient(): KITransientEntity<Long> = Transient(this)

    override fun <V, P : KIProperty<V>> getValue(prop: P): V? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V, P : KIProperty<V>> setValue(prop: P, v: V?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V, P : KIProperty<V>> setValue(prop: P, version: Any, v: V?) = DONTDOTHIS()

    companion object {
        object Meta : KIJvmEntityMeta(InterestEntity::class, InterestEntity::class) {
            override val root: KClass<*>
                get() = InterestEntity::class
            override val parent: KClass<*>?
                get() = null
            override val versioned: Boolean
                get() = false
        }

        class Transient(override val _store: DataStore, override val id: Long, override val name: String) : KITransientEntity<Long>, InterestEntity {
            constructor(e: InterestEntity) : this(e._store, e.id, e.name)

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
        whenever(ds.query(any<Query<InterestEntity, Long>>())).thenAnswer {

        }
    }
})