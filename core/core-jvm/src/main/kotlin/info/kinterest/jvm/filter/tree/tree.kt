@file:Suppress("unused")

package info.kinterest.jvm.filter.tree

import info.kinterest.*
import info.kinterest.filter.Filter

import info.kinterest.jvm.events.Dispatcher
import info.kinterest.jvm.filter.*
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging

class FilterTree(events: Dispatcher<EntityEvent<*, *>>, load: Int) {
    var root: Node.Root = Node.Root(load)
    private fun collect(ev: EntityEvent<*, *>) = root.collect(ev)

    init {
        val listener = Channel<EntityEvent<*, *>>()
        val context: CoroutineDispatcher = newFixedThreadPoolContext(4, "filter.tree")
        runBlocking {
            launch(context) {
                for (ev in listener) {
                    val collect = collect(ev)
                    logger.debug { "$ev collected $collect" }
                    for (dest in collect) {
                        logger.debug { "$dest digest" }
                        dest.digest(ev.cast())
                    }
                }
            }
            logger.debug { "subscribing" }
            events.subscribing.send(listener)
        }
    }

    sealed class Node(val load: Int) {
        abstract operator fun plus(f: LiveFilterWrapper<*, *>): Node
        abstract operator fun minus(f: LiveFilterWrapper<*, *>): Node
        sealed class EntityBasedNode(load: Int, val meta: KIEntityMeta) : Node(load) {
            abstract val filters: Set<LiveFilterWrapper<*, *>>
            abstract override fun plus(f: LiveFilterWrapper<*, *>): EntityBasedNode

            abstract override fun minus(f: LiveFilterWrapper<*, *>): EntityBasedNode
            abstract fun collect(ev: EntityEvent<*, *>): Set<LiveFilterWrapper<*, *>>

            class EntityNode(load: Int, meta: KIEntityMeta, override val filters: Set<LiveFilterWrapper<*, *>> = setOf()) : EntityBasedNode(load, meta) {
                override operator fun plus(f: LiveFilterWrapper<*, *>): EntityBasedNode = if (f !in filters) {
                    if (filters.size >= load) EntitySplitNode(load, meta, filters) else EntityNode(load, meta, filters + f)
                } else this

                override fun minus(f: LiveFilterWrapper<*, *>): EntityNode = EntityNode(load, meta, filters - f)
                override fun collect(ev: EntityEvent<*, *>) = filters
            }

            class EntitySplitNode(load: Int, meta: KIEntityMeta, override val filters: Set<LiveFilterWrapper<*, *>>) : EntityBasedNode(load, meta) {
                private var allFilter: Set<LiveFilterWrapper<*, *>> = setOf()
                private var idNode: IDNode = IDNode(load, meta, setOf())
                private var propertyNodes: Map<KIProperty<*>, PropertyNode> = mapOf()

                init {
                    val fits = filters.map { it to bestFit(it.f) }
                    for (fit in fits) {
                        val bests = fit.second.findBest()
                        for (sf in bests) {
                            when (sf) {
                                is AllFit -> allFilter += fit.first
                                is IdFit -> idNode += fit.first
                                is PropertyFit -> {
                                    val props = if (sf.f.prop in propertyNodes) {
                                        propertyNodes[sf.f.prop]!! + fit.first
                                    } else PropertyNode(load, meta, sf.f.prop, setOf(fit.first))
                                    propertyNodes += sf.f.prop to props
                                }
                            }
                        }
                    }
                }

                override fun plus(f: LiveFilterWrapper<*, *>): EntityBasedNode = EntitySplitNode(load, meta, filters + f)

                override fun minus(f: LiveFilterWrapper<*, *>): EntityBasedNode = if (filters.size <= load / 2) {
                    EntityNode(load, meta, filters - f)
                } else {
                    EntitySplitNode(load, meta, filters - f)
                }

                override fun collect(ev: EntityEvent<*, *>) = idNode.collect(ev) + propertyNodes.map { it.value }.flatMap { it.collect(ev) } + allFilter
            }

            class IDNode(load: Int, meta: KIEntityMeta, override val filters: Set<LiveFilterWrapper<*, *>>) : EntityBasedNode(load, meta) {
                override fun plus(f: LiveFilterWrapper<*, *>): IDNode = IDNode(load, meta, filters + f)

                override fun minus(f: LiveFilterWrapper<*, *>): IDNode = IDNode(load, meta, filters - f)

                override fun collect(ev: EntityEvent<*, *>): Set<LiveFilterWrapper<*, *>> = when (ev) {
                    is EntityCreateEvent<*, *> -> filters
                    is EntityDeleteEvent<*, *> -> filters
                    is EntityRelationEvent<*, *, *, *> -> emptySet()
                    is EntityUpdatedEvent<*, *> -> emptySet()
                }
            }

            class PropertyNode(load: Int, meta: KIEntityMeta, private val property: KIProperty<*>, override val filters: Set<LiveFilterWrapper<*, *>>) : EntityBasedNode(load, meta) {

                override fun plus(f: LiveFilterWrapper<*, *>): PropertyNode = PropertyNode(load, meta, property, filters + f)

                override fun minus(f: LiveFilterWrapper<*, *>): PropertyNode = PropertyNode(load, meta, property, filters - f)

                override fun collect(ev: EntityEvent<*, *>): Set<LiveFilterWrapper<*, *>> = when (ev) {
                    is EntityCreateEvent, is EntityDeleteEvent -> filters
                    is EntityUpdatedEvent ->
                        if (ev.updates.any { it.prop == property })
                            filters
                        else emptySet()
                    is EntityRelationEvent<*, *, *, *> -> if (ev.relations.first().rel == property) filters else emptySet()
                }
            }
        }

        class Root(load: Int, val entities: Map<KIEntityMeta, EntityBasedNode> = mapOf()) : Node(load) {
            operator fun get(meta: KIEntityMeta): EntityBasedNode = entities.getOrElse(meta) { EntityBasedNode.EntityNode(load, meta) }
            override operator fun plus(f: LiveFilterWrapper<*, *>): Root = run {
                val ne = entities + (f.meta to this[f.meta] + f)
                Root(load, ne)
            }

            override fun minus(f: LiveFilterWrapper<*, *>): Root = if (f !in this[f.meta].filters) this else {
                val ne = entities + (f.meta to this[f.meta] - f)
                Root(load, ne)
            }


            fun collect(ev: EntityEvent<*, *>): Set<LiveFilterWrapper<*, *>> = when (ev) {
                is EntityCreateEvent -> ev.entities.firstOrNull()?.let { e -> this[e._meta].collect(ev) } ?: setOf()
                is EntityDeleteEvent -> ev.entities.firstOrNull()?.let { e -> this[e._meta].collect(ev) } ?: setOf()
                is EntityUpdatedEvent -> this[ev.entity._meta].collect(ev)
                is EntityRelationEvent<*, *, *, *> -> ev.relations.firstOrNull()?.let {
                    val meta = it.source._meta
                    this[meta].collect(ev)
                } ?: emptySet()
            }


        }
    }


    operator fun plusAssign(filter: LiveFilterWrapper<*, *>) {
        root += filter
    }

    operator fun minusAssign(filter: Filter<*, *>) {
        if (filter is LiveFilterWrapper)
            root -= filter
    }

    companion object : KLogging()

}

object PropertyComparator : Comparator<KIProperty<*>> {
    override fun compare(o1: KIProperty<*>, o2: KIProperty<*>): Int = o1.order.compareTo(o2.order)
}

sealed class Fit(open val f: EntityFilter<*, *>) {
    class OrFit(f: OrFilter<*, *>, val fits: Set<Fit>) : Fit(f)
    class AndFit(f: AndFilter<*, *>, val fit: Fit, val rest: Iterable<Fit>) : Fit(f)
    sealed class SimpleFit(f: EntityFilter<*, *>) : Fit(f) {
        class IdFit(override val f: IdFilter<*, *>) : SimpleFit(f)
        class PropertyFit(override val f: PropertyFilter<*, *, *>) : SimpleFit(f)
        class AllFit(override val f: EntityFilter.AllFilter<*, *>) : SimpleFit(f)
    }

    fun findBest(): Set<SimpleFit> = when (this) {
        is SimpleFit -> setOf(this)
        is AndFit -> fit.findBest()
        is OrFit -> fits.flatMap { it.findBest() }.toSet()
    }
}

typealias OrFit = Fit.OrFit
typealias AndFit = Fit.AndFit
typealias IdFit = Fit.SimpleFit.IdFit
typealias AllFit = Fit.SimpleFit.AllFit
typealias PropertyFit = Fit.SimpleFit.PropertyFit

fun bestFit(f: Filter<*, *>): Fit =
        when (f) {
            is EntityFilter.Empty -> DONTDOTHIS()
            is LiveFilterWrapper -> bestFit(f.f)
            is CombinationFilter -> {
                val operandsFit = f.operands.map { bestFit(it) }
                when (f) {
                    is AndFilter -> {
                        val orderedFits = operandsFit.sortedWith(FitComparator)
                        val best = orderedFits.first()
                        val rest = orderedFits.drop(1)
                        Fit.AndFit(f, best, rest)
                    }
                    is OrFilter -> Fit.OrFit(f, operandsFit.toSet())
                }
            }
            is IdFilter -> IdFit(f)
            is PropertyFilter<*, *, *> -> PropertyFit(f)
            is EntityFilter.AllFilter<*, *> -> Fit.SimpleFit.AllFit(f)
            else -> throw Exception("bad filter $f ${f::class}")
        }

object FitComparator : Comparator<Fit> {
    override fun compare(f1: Fit, f2: Fit): Int = when (f1) {
        is AllFit -> 1
        is IdFit -> when (f2) {
            is IdFit -> when (f1.f) {
                is StaticEntityFilter -> -1
                else -> if (f2.f is StaticEntityFilter) 1 else -1
            }
            else -> -1
        }
        is PropertyFit -> if (f2 is IdFit) 1 else {
            val filter1 = f1.f
            when (filter1) {
                is PropertyNullFilter<*, *, *> -> -1
                is PropertyNotNullFilter<*, *, *> -> -1
                is PropertyValueFilter<*, *, *> -> {
                    val filter2 = f2.f
                    when (filter2) {
                        is PropertyNullFilter<*, *, *>, is PropertyNotNullFilter<*, *, *> -> 1
                        is PropertyValueFilter<*, *, *> -> PropertyComparator.compare(filter1.prop, filter2.prop)
                        else -> -1
                    }
                }
                is PropertyInFilter<*, *, *>, is PropertyNotInFilter<*, *, *> -> 1
            }
        }
        is Fit.AndFit, is Fit.OrFit -> if (f2 is Fit.OrFit) -1 else 1
    }
}