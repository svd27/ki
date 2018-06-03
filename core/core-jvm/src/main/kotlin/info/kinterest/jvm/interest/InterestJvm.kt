package info.kinterest.jvm.interest

import info.kinterest.*
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.query.Query
import info.kinterest.query.QueryResult
import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging

open class InterestJvm<E : KIEntity<K>, K : Any>(override val id: Any, q: Query<E, K>, protected val manager: InterestManager, private val subscriber: suspend (Iterable<InterestContainedEvent<Interest<E, K>, E, K>>) -> Unit) : Interest<E, K> {
    override val query: Query<E, K>
        get() = _query
    protected var _query: Query<E, K> = q
        set(value) {
            runBlocking(pool) {
                result = query(value).getOrElse {
                    throw InterestError.InterestQueryError(this@InterestJvm, it.message ?: "", it)
                }
                fire(listOf(InterestProjectionEvent(this@InterestJvm, result.projections.map { ProjectionLoaded(it.value) })))
            }
            field = value
        }

    override var result: QueryResult<E, K> = QueryResult.empty(q.f.meta)

    private val events: Channel<FilterEvent<E, K>> = Channel()
    @Suppress("UNCHECKED_CAST")
    private val filter = (q.f as EntityFilter.LiveFilterWrapper<E, K>)

    init {
        launch(pool) {
            for (ev in events) {
                logger.debug { "digesting $ev in $result" }
                var pevts: List<ProjectionEvent<E, K>> = listOf()
                result.digest(this@InterestJvm, listOf(ev), { evts: Iterable<ProjectionEvent<E, K>> -> pevts += evts })
                if (pevts.size > 0) fire(listOf(InterestProjectionEvent(this@InterestJvm, pevts)))
            }
        }

        filter.listener = events
        manager.qm.filterTree += filter
        manager.created(this@InterestJvm)

        launch(pool) {
            _query = q
        }
    }

    fun close() {
        manager.qm.filterTree -= filter
        filter.listener = null
        events.close()
        manager.qm.removeFilter(query.f as EntityFilter.LiveFilterWrapper<*, *>)
    }


    override fun query(query: Query<E, K>): Try<QueryResult<E, K>> = runBlocking {
        manager.qm.query(query).getOrElse {
            throw InterestError.InterestQueryError(this@InterestJvm, it.message ?: "", it)
        }.await()
    }


    private fun fire(evts: Iterable<InterestContainedEvent<Interest<E, K>, E, K>>) {
        val list = evts.toList()
        if (list.isNotEmpty()) launch(pool) {
            subscriber(list)
        }
    }

    override fun toString(): String = "Interrest($id, $query)"

    companion object : KLogging() {
        val pool: CoroutineDispatcher = newFixedThreadPoolContext(8, "interests")
    }
}