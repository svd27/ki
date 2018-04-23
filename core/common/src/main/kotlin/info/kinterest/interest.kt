package info.kinterest

import info.kinterest.functional.Try
import info.kinterest.query.Query
import info.kinterest.query.QueryResult

interface Interest<E : KIEntity<K>, K : Any> {
    val query: Query<E, K>
    val id: Any
    var result: QueryResult<E, K>

    fun query(query: Query<E, K>): Try<QueryResult<E, K>>

}

interface StaticInterest<E : KIEntity<K>, K : Any> {
    operator fun plus(e: E)
    operator fun minus(e: E)
}
