package info.kinterest.query

import info.kinterest.KIEntity
import info.kinterest.filter.Filter
import info.kinterest.paging.Paging
import info.kinterest.sorting.Ordering

class Query<E : KIEntity<K>, K : Any>(val f: Filter<E, K>, val ordering: Ordering<E, K>, val page: Paging)