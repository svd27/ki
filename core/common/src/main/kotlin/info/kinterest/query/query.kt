package info.kinterest.query

import info.kinterest.KIEntity
import info.kinterest.cast
import info.kinterest.filter.Filter
import info.kinterest.filter.NOFILTER
import info.kinterest.paging.Paging
import info.kinterest.sorting.Ordering

class Query<E : KIEntity<K>, K : Any>(
        val f: Filter<E, K>, val ordering: Ordering<E, K> = Ordering.NATURAL.cast(), val page: Paging = Paging(0, 10)
) {
    companion object {
        val NOQUERY = Query(NOFILTER)
    }
}