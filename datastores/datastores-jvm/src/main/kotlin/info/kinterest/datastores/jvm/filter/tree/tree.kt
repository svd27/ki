package info.kinterest.datastores.jvm.filter.tree

import info.kinterest.*
import info.kinterest.datastores.jvm.DataStoreJvm
import info.kinterest.jvm.filter.*
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty

class FilterTree(val ds: DataStoreJvm, val load: Int) {
    internal var root: Node.Root = Node.Root(load)
    fun collect(ev: EntityEvent<*, *>) = root.collect(ev)

    internal sealed class Node(val load: Int) {
        abstract operator fun plus(f: FilterWrapper<*, *>): Node
        abstract operator fun minus(f: FilterWrapper<*, *>): Node
        sealed class EntityBasedNode(load: Int, val meta: KIEntityMeta) : Node(load) {
            abstract val filters: Set<FilterWrapper<*, *>>
            abstract override fun plus(f: FilterWrapper<*, *>): EntityBasedNode

            abstract override fun minus(f: FilterWrapper<*, *>): EntityBasedNode
            abstract fun collect(ev: EntityEvent<*, *>): Set<FilterWrapper<*, *>>

            class EntityNode(load: Int, meta: KIEntityMeta, override val filters: Set<FilterWrapper<*, *>> = setOf()) : EntityBasedNode(load, meta) {
                override operator fun plus(f: FilterWrapper<*, *>): EntityBasedNode = if (f !in filters) {
                    if (filters.size >= load) EntitySplitNode(load, meta, filters) else EntityNode(load, meta, filters + f)
                } else this

                override fun minus(f: FilterWrapper<*, *>): EntityNode = EntityNode(load, meta, filters - f)
                override fun collect(ev: EntityEvent<*, *>) = filters
            }

            class EntitySplitNode(load: Int, meta: KIEntityMeta, override val filters: Set<FilterWrapper<*, *>>) : EntityBasedNode(load, meta) {
                var idNode: IDNode = IDNode(load, meta, setOf())
                var propertyNodes: Map<KIProperty<*>, PropertyNode> = mapOf()

                init {
                    val fits = filters.map { it to bestFit(it) }
                    for (fit in fits) {
                        val bests = fit.second.findBest()
                        for (sf in bests) {
                            when (sf) {
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

                override fun plus(f: FilterWrapper<*, *>): EntityBasedNode = EntitySplitNode(load, meta, filters + f)

                override fun minus(f: FilterWrapper<*, *>): EntityBasedNode = if (filters.size <= load / 2) {
                    EntityNode(load, meta, filters - f)
                } else {
                    EntitySplitNode(load, meta, filters - f)
                }

                override fun collect(ev: EntityEvent<*, *>) = idNode.collect(ev) + propertyNodes.map { it.value }.flatMap { it.collect(ev) }
            }

            class IDNode(load: Int, meta: KIEntityMeta, override val filters: Set<FilterWrapper<*, *>>) : EntityBasedNode(load, meta) {
                override fun plus(f: FilterWrapper<*, *>): IDNode = IDNode(load, meta, filters + f)

                override fun minus(f: FilterWrapper<*, *>): IDNode = IDNode(load, meta, filters - f)

                override fun collect(ev: EntityEvent<*, *>): Set<FilterWrapper<*, *>> = when (ev) {
                    is EntityCreateEvent<*, *> -> filters
                    is EntityDeleteEvent<*, *> -> filters
                    is EntityUpdatedEvent<*, *> -> setOf()
                }
            }

            class PropertyNode(load: Int, meta: KIEntityMeta, val property: KIProperty<*>, override val filters: Set<FilterWrapper<*, *>>) : EntityBasedNode(load, meta) {

                override fun plus(f: FilterWrapper<*, *>): PropertyNode = PropertyNode(load, meta, property, filters + f)

                override fun minus(f: FilterWrapper<*, *>): PropertyNode = PropertyNode(load, meta, property, filters - f)

                override fun collect(ev: EntityEvent<*, *>): Set<FilterWrapper<*, *>> = when (ev) {
                    is EntityCreateEvent, is EntityDeleteEvent -> filters
                    is EntityUpdatedEvent ->
                        if (ev.updates.filter { it.prop == property }.isNotEmpty())
                            filters
                        else setOf()
                }
            }

            sealed class PropertyRangeNode(load: Int, meta: KIEntityMeta, val property: KIProperty<*>, val min: Any, val max: Any) {
                class LTRange(load: Int, meta: KIEntityMeta, property: KIProperty<*>, min: Any, max: Any) : PropertyRangeNode(load, meta, property, min, max)
                class GTERange(load: Int, meta: KIEntityMeta, property: KIProperty<*>, min: Any, max: Any) : PropertyRangeNode(load, meta, property, min, max)
            }
        }

        class Root(load: Int, val entities: Map<KIEntityMeta, EntityBasedNode> = mapOf()) : Node(load) {
            operator fun get(meta: KIEntityMeta): EntityBasedNode = entities.getOrElse(meta) { EntityBasedNode.EntityNode(load, meta) }
            override operator fun plus(f: FilterWrapper<*, *>): Root = run {
                val ne = entities + (f.meta to this[f.meta] + f)
                Root(load, ne)
            }

            override fun minus(f: FilterWrapper<*, *>): Root = if (f !in this[f.meta].filters) this else {
                val ne = entities + (f.meta to this[f.meta] - f)
                Root(load, ne)
            }


            fun collect(ev: EntityEvent<*, *>): Set<FilterWrapper<*, *>> = this[ev.entity._meta].collect(ev)

        }
    }


    operator fun plus(filter: FilterWrapper<*, *>): FilterTree = this.apply {
        root += filter
    }


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
typealias PropertyFit = Fit.SimpleFit.PropertyFit

fun bestFit(f: EntityFilter<*, *>): Fit =
        when (f) {
            is FilterWrapper -> bestFit(f.f)
            is EntityFilter.Empty -> DONTDOTHIS()
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
        }

object FitComparator : Comparator<Fit> {
    override fun compare(f1: Fit, f2: Fit): Int = when (f1) {
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
            }
        }
        is Fit.AndFit, is Fit.OrFit -> if (f2 is Fit.OrFit) -1 else 1
    }
}