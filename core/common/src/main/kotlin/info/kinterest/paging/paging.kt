package info.kinterest.paging

import info.kinterest.KIEntity

class Paging(val offset: Int, val size: Int) {
    val next: Paging get() = Paging(offset + size, size)
    val prev: Paging get() = Paging(maxOf(0, offset - size), size)
}

class Page<E : KIEntity<K>, K : Any>(val paging: Paging, val entites: List<E>, val more: Int = 0)