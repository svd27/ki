package info.kinterest.paging

import info.kinterest.KIEntity

data class Paging(val offset: Int, val size: Int) {
    val next: Paging get() = Paging(offset + size, size)
    val prev: Paging get() = Paging(maxOf(0, offset - size), size)
}

data class Page<out E : KIEntity<K>, out K : Any>(val paging: Paging, val entites: List<E>, val more: Int = 0) {
    operator fun get(idx: Int): E? = if (idx < entites.size) entites[idx] else null
}