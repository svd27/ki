package info.kinterest.filter

import info.kinterest.KIEntity
import info.kinterest.meta.KIEntityMeta

expect class Filter<E : KIEntity<K>, K : Any> {
    val meta: KIEntityMeta
    fun matches(e: E): Boolean
}