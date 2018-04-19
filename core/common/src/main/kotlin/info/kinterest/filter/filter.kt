package info.kinterest.filter

import info.kinterest.*
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import kotlin.reflect.KClass

@Suppress("AddVarianceModifier")
interface Filter<E : KIEntity<K>, K : Any> {
    val meta: KIEntityMeta
    fun matches(e: E): Boolean
    fun wants(upd: EntityUpdatedEvent<E, K>): Boolean
    fun wants(rel: EntityRelationEvent<E, K, *, *>): Boolean
    fun inverse(): Filter<E, K>
}

open class FilterWrapper<E : KIEntity<K>, K : Any>(val f: Filter<E, K>, override val meta: KIEntityMeta) : Filter<E, K> {
    override fun matches(e: E): Boolean = f.matches(e)

    override fun wants(upd: EntityUpdatedEvent<E, K>): Boolean = f.wants(upd)
    override fun wants(rel: EntityRelationEvent<E, K, *, *>): Boolean = f.wants(rel)

    override fun inverse(): Filter<E, K> = FilterWrapper(f.inverse(), meta)
}

expect class IdFilter<E : KIEntity<K>, K : Any>(ids: Set<K>, meta: KIEntityMeta) : Filter<E, K>

val NOFILTER = object : FilterWrapper<Nothing, Nothing>(object : Filter<Nothing, Nothing> {
    override val meta: KIEntityMeta
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override fun matches(e: Nothing): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun wants(upd: EntityUpdatedEvent<Nothing, Nothing>): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun inverse(): Filter<Nothing, Nothing> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun wants(rel: EntityRelationEvent<Nothing, Nothing, *, *>): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}, object : KIEntityMeta() {
    override val root: KClass<*>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val impl: KClass<*>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val me: KClass<*>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val parent: KClass<*>?
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val name: String
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val versioned: Boolean
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val idProperty: KIProperty<*>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val props: Map<String, KIProperty<*>>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val hierarchy: List<KIEntityMeta>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val types: List<KIEntityMeta>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override fun <K : Any> new(ds: DataStore, id: K): KIEntity<K> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}) {

    override val meta: KIEntityMeta
        get() = DONTDOTHIS()

    override fun matches(e: Nothing): Boolean {
        DONTDOTHIS()
    }

    override fun wants(upd: EntityUpdatedEvent<Nothing, Nothing>): Boolean {
        DONTDOTHIS()
    }

    override fun inverse(): Filter<Nothing, Nothing> {
        DONTDOTHIS()
    }
}