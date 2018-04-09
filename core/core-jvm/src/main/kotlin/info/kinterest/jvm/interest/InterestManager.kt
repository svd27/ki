package info.kinterest.jvm.interest

import info.kinterest.*
import info.kinterest.jvm.query.QueryManagerJvm
import info.kinterest.query.Query
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.runBlocking

class InterestManager(val qm: QueryManagerJvm) {
    @Volatile
    private var _interests: List<InterestJvm<*, *>> = listOf()
    val interests: List<Interest<*, *>> get() = _interests
    val events = Channel<InterestEvent<Interest<KIEntity<Any>, Any>, KIEntity<Any>, Any>>(100)

    suspend fun subscriber(evts: Iterable<InterestContainedEvent<Interest<KIEntity<Any>, Any>, KIEntity<Any>, Any>>) {
        for (e in evts) events.send(e.cast())
    }

    operator fun <E : KIEntity<K>, K : Any> plus(q: Query<E, K>): Interest<E, K> = InterestJvm(UUID.randomUUID(), q, this, {
        @Suppress("UNCHECKED_CAST")
        subscriber(it as Iterable<InterestContainedEvent<Interest<KIEntity<Any>, Any>, KIEntity<Any>, Any>>)
    })

    operator fun <E : KIEntity<K>, K : Any> minus(i: Interest<E, K>): Any? =
            _interests.filter { it.id == i.id }.firstOrNull()?.let {
                it.close()
                _interests -= it
                runBlocking {
                    @Suppress("UNCHECKED_CAST")
                    events.send(InterestDeleted<Interest<E, K>, E, K>(i.id) as InterestEvent<Interest<KIEntity<Any>, Any>, KIEntity<Any>, Any>)
                }
                it.id
            }

    fun <E : KIEntity<K>, K : Any> created(i: InterestJvm<E, K>) {
        runBlocking {
            _interests += i
            @Suppress("UNCHECKED_CAST")
            events.send(InterestCreated<Interest<E, K>, E, K>(i) as InterestEvent<Interest<KIEntity<Any>, Any>, KIEntity<Any>, Any>)
        }
    }
}