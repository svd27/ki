package info.kinterest.jvm.query

import info.kinterest.KIEntity
import info.kinterest.filter.Filter
import info.kinterest.jvm.filter.EQFilter
import info.kinterest.jvm.filter.PropertyNullFilter
import info.kinterest.jvm.filter.filter
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.query.Discriminator
import info.kinterest.query.Discriminators

sealed class DiscriminatorsJvm<E : KIEntity<K>, K : Any, V : Any>() : Discriminators<E, K, V>

data class DistinctDiscriminators<E : KIEntity<K>, K : Any, V : Any>(
        val meta: KIEntityMeta, val property: KIProperty<V>,
        override val name: String = "Distinct(${property.name})") : DiscriminatorsJvm<E, K, V>() {
    override fun discriminatorFor(v: V?): Discriminator<E, K, V> = DistinctDriscriminator(this, v)
}

sealed class DiscriminatorJvm<E : KIEntity<K>, K : Any, V : Any>(override val name: String) : Discriminator<E, K, V>
data class DistinctDriscriminator<E : KIEntity<K>, K : Any, V : Any>(val parent: DistinctDiscriminators<E, K, V>, val value: V?) : DiscriminatorJvm<E, K, V>("${parent.name} = $value") {
    override fun inside(v: V?): Boolean = value == v

    override fun asFilter(): Filter<E, K> =
            filter<E, K>(parent.meta) {
                if (value == null)
                    PropertyNullFilter<E, K, V>(parent.property, parent.meta)
                else
                    @Suppress("UNCHECKED_CAST")
                    EQFilter(parent.property as KIProperty<Comparable<Any>>, parent.meta, value as Comparable<Any>)
            }
}