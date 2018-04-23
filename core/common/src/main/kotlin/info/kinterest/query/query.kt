package info.kinterest.query

import info.kinterest.*
import info.kinterest.filter.AbstractFilterWrapper
import info.kinterest.filter.Filter
import info.kinterest.filter.FilterEvent
import info.kinterest.filter.FilterWrapper
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.meta.KIEntityMeta
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.async

data class Query<E : KIEntity<K>, K : Any>(
        val f: AbstractFilterWrapper<E, K>,
        val projections: Iterable<Projection<E, K>>,
        val ds: Set<DataStore> = ALL
) {
    override fun equals(other: Any?): Boolean = other === this
    override fun hashCode(): Int = f.hashCode()

    suspend fun query(qm: QueryManager): Try<Deferred<Try<QueryResult<E, K>>>> = Try {
        val storesFor = qm.storesFor(f.meta)
        val dss = if (ds == Query.ALL) storesFor else storesFor.filter { it.name in ds.map { it.name }.toSet() }
        when {
            dss.isEmpty() -> throw QueryManagerRetrieveError(qm, "no DataStores found to query")
            dss.size == 1 -> dss.first().query(this).getOrElse { throw it }
            else -> {
                val proj = projections.map { it.adapt(dss) }
                val q = copy(projections = proj)
                val deferreds: List<Deferred<Try<QueryResult<E, K>>>> = dss.map { it.query(q) }.map { it.getOrElse { throw it } }
                async {
                    val res = deferreds.map { it.await() }
                    val qrs = res.map { it.getOrElse { throw it } }
                    Try { QueryResult.combine(this@Query, qrs) }

                }

            }
        }

    }

    companion object {
        val ALL: Set<DataStore> = setOf(object : DataStore("") {
            override fun equals(other: Any?): Boolean = other === this
        })
    }
}

data class QueryResult<E : KIEntity<K>, K : Any>(val q: Query<E, K>, var projections: Map<Projection<E, K>, ProjectionResult<E, K>>) {
    fun <I : Interest<E, K>> digest(i: I, evts: Iterable<FilterEvent<E, K>>, events: (Iterable<ProjectionEvent<E, K>>) -> Unit) {
        println("${QueryResult::class.simpleName}: digest $evts with $projections")
        projections.toMap().forEach {
            val digest = it.value.digest(i, evts, events)
            println("${it.key} ${it.value} -> ${digest}")
            projections += it.key to digest
        }
    }

    fun retrieve(path: Path, qm: QueryManager): Try<Deferred<Try<ProjectionResult<E, K>>>> = Try {
        projections.values.firstOrNull {
            println("${it.projection.path} subpath of $path says ${it.projection.path.isSubPath(path)}")
            path.isSubPath(it.projection.path)
        }?.retrieve(path, q, qm)?.getOrElse { throw it } ?: throw QueryError(q, qm, "$path not found in $this")
    }

    companion object {
        fun <E : KIEntity<K>, K : Any> combine(q: Query<E, K>, results: Iterable<QueryResult<E, K>>): QueryResult<E, K> =
                QueryResult(q, q.projections.map { p -> p.combine(results.map { it.projections[p]!! }) }.associateBy { it.projection })

        fun <E : KIEntity<K>, K : Any> empty(meta: KIEntityMeta): QueryResult<E, K> = QueryResult(Query(FilterWrapper(Filter.nofilter(meta)), emptyList()), emptyMap())
    }


}