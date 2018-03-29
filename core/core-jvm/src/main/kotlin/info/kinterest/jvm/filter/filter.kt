package info.kinterest.jvm.filter

import info.kinterest.DataStore
import info.kinterest.FilterError
import info.kinterest.KIEntity
import info.kinterest.cast
import info.kinterest.jvm.MetaProvider
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty

sealed class KIFilter<T> {
    abstract fun matches(e:T) : Boolean
}

inline fun<reified E:KIEntity<K>,K:Any> filter(ds:DataStore, provider: MetaProvider, crossinline cb:EntityFilter<E,K>.()-> EntityFilter<E, K>) : EntityFilter<E,K> = run {
    val meta = provider.meta(E::class) as KIEntityMeta
    filter(ds, meta, cb)
}

inline fun<E:KIEntity<K>,K:Any> filter(ds:DataStore, meta: KIEntityMeta, crossinline cb:EntityFilter<E,K>.()-> EntityFilter<E, K>) : EntityFilter<E,K> = EntityFilter.Empty<E,K>(ds, meta).run{
    EntityFilter.WrapperFilter<E,K>(ds, meta).apply {
        f = this.cb()
    }
}

@Suppress("EqualsOrHashCode")
sealed class EntityFilter<E:KIEntity<K>, K:Any>(val meta:KIEntityMeta) : KIFilter<E>() {
    abstract val parent : EntityFilter<E,K>
    open val ds :DataStore
      get() = parent.ds
    abstract fun matches(values:Map<String,Any?>) : Boolean
    class Empty<E:KIEntity<K>,K:Any>(ds: DataStore, meta:KIEntityMeta) : EntityFilter<E,K>(meta) {
        override lateinit var parent:EntityFilter<E,K>
        override fun matches(e: E): Boolean = TODO("not implemented")


        override fun matches(values: Map<String, Any?>): Boolean = TODO("not implemented")

        override fun inverse(): EntityFilter<E, K> = TODO("not implemented")
        override fun contentEquals(f: EntityFilter<*, *>): Boolean = TODO("not implemented")
    }
    class WrapperFilter<E:KIEntity<K>,K:Any>(override val ds:DataStore, meta:KIEntityMeta) : EntityFilter<E,K>(meta) {
        lateinit var f:EntityFilter<E,K>
        override val parent: EntityFilter<E, K>
            get() = TODO("not implemented")

        override fun matches(e: E): Boolean = f.matches(e)

        override fun matches(values: Map<String, Any?>): Boolean = f.matches(values)

        override fun inverse(): EntityFilter<E, K> = WrapperFilter<E,K>(ds, meta).also{ it.f = f.inverse()}

        override fun equals(other: Any?): Boolean = this === other

        override fun hashCode(): Int = System.identityHashCode(this)

        override fun contentEquals(f: EntityFilter<*, *>): Boolean = equals(f)
    }

    fun ids(vararg ids:K) : StaticEntityFilter<E, K> = StaticEntityFilter(ids.toSet(), meta, this)
    infix fun<P:Comparable<P>> String.eq(p:P) : EQFilter<E,K,P> = meta.props[this]?.let {
        EQFilter<E, K, P>(it.cast(), meta,this@EntityFilter,  p)
    }?:throw FilterError("property $this not found in ${meta.me}")
    infix fun<P:Comparable<P>> String.neq(value:P) : NEQFilter<E,K,P> = (this eq value).inverse()
    infix fun<P:Comparable<P>> String.gt(value:P) : GTFilter<E,K,P> = withProp(this,value) {GTFilter(it, meta, this@EntityFilter, value)}
    infix fun<P:Comparable<P>> String.gte(value:P) : GTEFilter<E,K,P> = withProp(this,value) {GTEFilter(it, meta, this@EntityFilter, value)}

    fun or(f:Iterable<EntityFilter<E,K>>) : EntityFilter<E,K> = when(this) {
        is CombinationFilter -> when(this) {
            is OrFilter -> OrFilter(this.operands+f,  meta, parent)
            else -> OrFilter(listOf(this)+f, meta, parent)
        }
        else -> OrFilter(listOf(this)+f, meta, parent)
    }

    fun and(f:Iterable<EntityFilter<E,K>>) : EntityFilter<E,K> = when(this) {
        is CombinationFilter -> when(this) {
            is AndFilter -> AndFilter(this.operands+f, meta, parent)
            else -> AndFilter(listOf(this)+f, meta, parent)
        }
        else -> AndFilter(listOf(this)+f, meta, parent)
    }

    @Suppress("UNUSED_PARAMETER")
    private fun<R,P> withProp(name:String, value:P, cb:(KIProperty<P>)->R) = meta.props[name]?.let{cb(it.cast())}?:throw FilterError("property $this not found in ${meta.me}")

    abstract fun inverse() : EntityFilter<E, K>

    override fun equals(other: Any?): Boolean = if(other==this) true else {
        if (other is EntityFilter<*, *>) {
            if (other.meta == meta && other.ds == ds) contentEquals(other)
            else false
        } else false
    }

    protected abstract fun contentEquals(f:EntityFilter<*,*>) : Boolean

}

abstract class IdFilter<E:KIEntity<K>,K:Any>(meta: KIEntityMeta, override val parent:EntityFilter<E,K>) : EntityFilter<E, K>(meta)
@Suppress("EqualsOrHashCode")
class StaticEntityFilter<E:KIEntity<K>, K:Any>(private val ids:Set<K>, meta: KIEntityMeta, parent:EntityFilter<E,K>) : IdFilter<E, K>(meta, parent) {
    override fun matches(e: E): Boolean = ids.any { e.id == it }
    override fun matches(values: Map<String, Any?>): Boolean = values["_id"]?.let { ids.any(it::equals) }?:false

    inner class Inverse : IdFilter<E, K>(meta, parent) {
        private val origin = this@StaticEntityFilter
        override fun matches(e: E): Boolean = !origin.matches(e)
        override fun matches(values: Map<String, Any?>): Boolean = !origin.matches(values)

        override fun inverse(): IdFilter<E, K> = origin
        override fun contentEquals(f: EntityFilter<*, *>): Boolean = if(f is Inverse) ids == f.origin.ids else false
    }
    override fun inverse(): IdFilter<E, K> = Inverse()

    override fun contentEquals(f: EntityFilter<*, *>): Boolean = if(f is StaticEntityFilter<*,*>) ids == f.ids else false
    override fun hashCode(): Int = ids.hashCode()
}

sealed class PropertyFilter<E:KIEntity<K>,K:Any,P>(val prop:KIProperty<P>, meta: KIEntityMeta, override val parent: EntityFilter<E, K>) : EntityFilter<E, K>(meta)
sealed class PropertyValueFilter<E:KIEntity<K>,K:Any,P>(prop:KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, val value: P) : PropertyFilter<E, K,P>(prop,meta,parent) {
    fun match(p:P?) : Boolean = relate(p,value)
    abstract fun relate(value:P?, test:P) : Boolean
    override fun matches(e: E): Boolean = match(meta.get(e, prop))
    @Suppress("UNCHECKED_CAST")
    override fun matches(values: Map<String, Any?>): Boolean = match(values[prop.name] as P?)

    override fun contentEquals(f: EntityFilter<*, *>): Boolean = if(this::class==f::class && f is PropertyValueFilter<*,*,*>)
        prop == f.prop && value==f.value else false
}

class EQFilter<E:KIEntity<K>,K:Any,P:Comparable<P>>(prop:KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value == test
    override fun inverse(): NEQFilter<E, K, P> = NEQFilter(prop, meta, parent, value)
}

class NEQFilter<E:KIEntity<K>,K:Any,P:Comparable<P>>(prop:KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value != test
    override fun inverse(): EQFilter<E, K, P> = EQFilter(prop, meta, parent, value)
}

class GTFilter<E:KIEntity<K>,K:Any,P:Comparable<P>> (prop:KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->v>test }?:false
    override fun inverse(): LTEFilter<E, K, P> = LTEFilter(prop, meta, parent, value)
}
class GTEFilter<E:KIEntity<K>,K:Any,P:Comparable<P>> (prop:KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->v>=test }?:false
    override fun inverse(): LTFilter<E, K, P> = LTFilter(prop, meta, parent, value)
}

class LTFilter<E:KIEntity<K>,K:Any,P:Comparable<P>> (prop:KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->
        v < test
    } ?: true

    override fun inverse(): GTEFilter<E, K, P> = GTEFilter(prop, meta, parent, value)
}

class LTEFilter<E:KIEntity<K>,K:Any,P:Comparable<P>> (prop:KIProperty<P>, meta: KIEntityMeta, parent: EntityFilter<E, K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, parent, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->
        v <= test
    } ?: true

    override fun inverse(): GTFilter<E, K, P> = GTFilter(prop, meta, parent, value)
}

sealed class CombinationFilter<E:KIEntity<K>,K:Any>(val operands:Iterable<EntityFilter<E,K>>, meta: KIEntityMeta, override val parent: EntityFilter<E, K>) : EntityFilter<E,K>(meta) {
    override fun contentEquals(f: EntityFilter<*, *>): Boolean = if(this::class == f::class && f is CombinationFilter) operands.all {
        source -> f.operands.any { source == it } } else false
}
class AndFilter<E:KIEntity<K>,K:Any>(operands:Iterable<EntityFilter<E,K>>, meta: KIEntityMeta, parent: EntityFilter<E, K>) : CombinationFilter<E,K>(operands, meta, parent) {
    override fun matches(e: E): Boolean = operands.all { it.matches(e) }


    override fun matches(values: Map<String, Any?>): Boolean = operands.all { it.matches(values) }

    override fun inverse(): EntityFilter<E, K> = OrFilter(operands.map(EntityFilter<E,K>::inverse), meta, parent)
}

class OrFilter<E:KIEntity<K>,K:Any>(operands:Iterable<EntityFilter<E,K>>, meta: KIEntityMeta, parent: EntityFilter<E, K>) : CombinationFilter<E,K>(operands, meta, parent) {
    override fun matches(e: E): Boolean = operands.any {it.matches(e)}

    override fun matches(values: Map<String, Any?>): Boolean = operands.any { it.matches(values) }

    override fun inverse(): EntityFilter<E, K> = AndFilter(operands.map { it.inverse() }, meta, parent)
}