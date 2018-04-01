package info.kinterest.sorting

import info.kinterest.KIEntity
import info.kinterest.meta.KIProperty

fun KIProperty<*>.asc(): OrderDef = OrderDef(this, OrderDirection.ASCENDING)
fun KIProperty<*>.desc(): OrderDef = OrderDef(this, OrderDirection.DESCENDING)

enum class OrderDirection {
    ASCENDING,
    DESCENDING
}

enum class NullPlacement {
    NULLLASY,
    NULLFIRST
}

class OrderDef(val prop: KIProperty<*>, val direction: OrderDirection = OrderDirection.ASCENDING)

class Ordering<E : KIEntity<K>, K : Any>(val order: Iterable<OrderDef>, val nullPlacement: NullPlacement = NullPlacement.NULLLASY) : Comparator<E> {
    @Suppress("UNCHECKED_CAST")
    override fun compare(a: E, b: E): Int {
        for (o in order) {
            val va = a.getValue(o.prop) as Comparable<Any>?
            val vb = b.getValue(o.prop) as Comparable<Any>?
            if (va == null) return if (nullPlacement == NullPlacement.NULLLASY) 1 else -1
            if (vb == null) return if (nullPlacement == NullPlacement.NULLLASY) -1 else 1
            var res = va.compareTo(vb)
            if (o.direction == OrderDirection.DESCENDING) res *= -1
            if (res != 0) return res
        }
        return 0
    }
}
