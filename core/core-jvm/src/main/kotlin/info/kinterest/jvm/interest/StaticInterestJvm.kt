package info.kinterest.jvm.interest

import info.kinterest.*
import info.kinterest.jvm.filter.FilterWrapper
import info.kinterest.jvm.filter.StaticEntityFilter
import info.kinterest.query.Query

class StaticInterestJvm<E : KIEntity<K>, K : Any>(id: Any, q: Query<E, K>, manager: InterestManager,
                                                  subscriber: suspend (Iterable<InterestContainedEvent<Interest<E, K>, E, K>>) -> Unit) :
        InterestJvm<E, K>(id, q, manager, subscriber), StaticInterest<E, K> {

    init {
        require(q.f.f is StaticEntityFilter<*, *>)
    }


    override fun plus(e: E) {
        @Suppress("UNCHECKED_CAST")
        val old = query.f.f as StaticEntityFilter<E, K>
        val f = StaticEntityFilter<E, K>(old.ids + e.id, old.meta)
        val filterWrapper = FilterWrapper(f)
        manager.qm.filterTree -= query.f.cast()
        manager.qm.filterTree += filterWrapper
        _query = Query(filterWrapper.cast(), query.projections, query.ds)
    }

    override fun minus(e: E) {
        @Suppress("UNCHECKED_CAST")
        val old = query.f.f as StaticEntityFilter<E, K>
        val f = StaticEntityFilter<E, K>(old.ids - e.id, old.meta)
        val filterWrapper = FilterWrapper(f)
        manager.qm.filterTree -= query.f.cast()
        manager.qm.filterTree += filterWrapper
        _query = Query(filterWrapper.cast(), query.projections, query.ds)
    }
}