package info.kinterest.filter

import info.kinterest.DONTDOTHIS
import info.kinterest.EntityUpdatedEvent
import info.kinterest.KIEntity
import info.kinterest.meta.KIEntityMeta

@Suppress("AddVarianceModifier")
expect interface Filter<E : KIEntity<K>, K : Any> {
    val f: Filter<E, K>
    val meta: KIEntityMeta
    fun matches(e: E): Boolean
    fun wants(upd: EntityUpdatedEvent<E, K>): Boolean
    fun inverse(): Filter<E, K>
}

val NOFILTER = object : Filter<Nothing, Nothing> {
    override val f: Filter<Nothing, Nothing>
        get() = this
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