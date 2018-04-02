package info.kinterest

import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.sorting.Ordering

interface Interest<E : KIEntity<K>, K : Any> {
    val id: Any
    val entities: Page<E, K>
    var ordering: Ordering<E, K>
    var paging: Paging
    /**
     * retrieves the entity with key K if it is within the filter of the interest or null
     * will throw EntityNotFound
     */
    operator fun get(k: K): E?

    /**
     * get entity at idx in the current page
     */
    operator fun get(idx: Int): E?

    fun next() {
        paging = paging.next
    }

    fun prev() {
        paging = paging.prev
    }

    fun addSubscriber(s: suspend (Iterable<InterestEvent<Interest<E, K>, E, K>>) -> Unit)
    fun removeSubscriber(s: suspend (Iterable<InterestEvent<Interest<E, K>, E, K>>) -> Unit)
}

