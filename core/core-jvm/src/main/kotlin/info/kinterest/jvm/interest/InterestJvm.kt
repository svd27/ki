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

    override var result: QueryResult<E, K> = QueryResult.empty()

    private val events: Channel<EntityEvent<E, K>> = Channel()
    @Suppress("UNCHECKED_CAST")
    private val filter = (q.f as EntityFilter.FilterWrapper<E, K>)

    init {
        launch(pool) {
            for (ev in events) {
                result.digest(this@InterestJvm, listOf(ev), { evts: Iterable<ProjectionEvent<E, K>> -> fire(listOf(InterestProjectionEvent(this@InterestJvm, evts))) })
            }
        }

        filter.listener = events

        launch(pool) {
            _query = q
            manager.qm.addFilter(query.f as EntityFilter.FilterWrapper<*, *>)
            manager.created(this@InterestJvm)
        }
    }

    fun close() {
        filter.listener = null
        events.close()
        manager.qm.removeFilter(query.f as EntityFilter.FilterWrapper<*, *>)
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

    companion object {
        val pool: CoroutineDispatcher = newFixedThreadPoolContext(8, "interests")
    }
}