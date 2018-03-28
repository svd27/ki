package info.kinterest.jvm.filter

import info.kinterest.KIEntity
import info.kinterest.cast
import info.kinterest.FilterError
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty

sealed class KIFilter<T> {
    abstract fun matches(e:T) : Boolean
    inline fun<E:KIEntity<K>,K:Comparable<K>> entity(meta:KIEntityMeta<K>,cb: KIFilter<T>.()-> EntityFilter<E, K>) : EntityFilter<E, K> = EntityFilter.WrapperFilter<E,K>(meta,null).run {
        cb()
    }
}

inline fun<reified E:KIEntity<K>,K:Comparable<K>> filter(provider: MetaProvider,cb:EntityFilter<E,K>.()-> EntityFilter<E, K>) : EntityFilter<E,K> = run {
    val meta = provider.meta(E::class) as KIEntityMeta<K>
    filter<E,K>(meta, cb)
}

inline fun<E:KIEntity<K>,K:Comparable<K>> filter(meta: KIEntityMeta<K>,cb:EntityFilter<E,K>.()-> EntityFilter<E, K>) : EntityFilter<E,K> = EntityFilter.WrapperFilter<E,K>(meta,null).run{
    this.cb()
}

sealed class EntityFilter<E:KIEntity<K>,K:Comparable<K>>(val meta:KIEntityMeta<K>) : KIFilter<E>() {
    abstract fun matches(values:Map<String,Any?>) : Boolean
    class WrapperFilter<E:KIEntity<K>,K:Comparable<K>>(meta:KIEntityMeta<K>, var f:EntityFilter<E,K>?) : EntityFilter<E,K>(meta) {
        override fun matches(e: E): Boolean = f!!.matches(e)

        override fun matches(values: Map<String, Any?>): Boolean = f!!.matches(values)

        override fun inverse(): EntityFilter<E, K> = f!!.inverse()
    }

    fun ids(vararg ids:K) : StaticEntityFilter<E, K> = StaticEntityFilter(ids.toList(), meta)
    infix fun String.ids(ids:Iterable<K>) : StaticEntityFilter<E,K> = StaticEntityFilter(ids,meta)
    infix fun<P:Comparable<P>> String.eq(p:P) : EQFilter<E,K,P> = meta.props[this]?.let {
        EQFilter<E, K, P>(it.cast(), meta, p)
    }?:throw FilterError("property $this not found in ${meta.me}")
    infix fun<P:Comparable<P>> String.neq(value:P) : NEQFilter<E,K,P> = (this eq value).inverse()
    infix fun<P:Comparable<P>> String.gt(value:P) : GTFilter<E,K,P> = withProp(this,value) {GTFilter(it, meta, value)}
    infix fun<P:Comparable<P>> String.gte(value:P) : GTEFilter<E,K,P> = withProp(this,value) {GTEFilter(it, meta, value)}

    fun or(f:Iterable<EntityFilter<E,K>>) : EntityFilter<E,K> = when(this) {
        is CombinationFilter -> when(this) {
            is OrFilter -> OrFilter(this.operands+f, meta)
            else -> OrFilter(listOf(this)+f, meta)
        }
        else -> OrFilter(listOf(this)+f, meta)
    }

    fun and(f:Iterable<EntityFilter<E,K>>) : EntityFilter<E,K> = when(this) {
        is CombinationFilter -> when(this) {
            is AndFilter -> AndFilter(this.operands+f, meta)
            else -> AndFilter(listOf(this)+f, meta)
        }
        else -> AndFilter(listOf(this)+f, meta)
    }

    fun<R,P> withProp(name:String, value:P, cb:(KIProperty<P>)->R) = meta.props[name]?.let{cb(it.cast())}?:throw FilterError("property $this not found in ${meta.me}")

    abstract fun inverse() : EntityFilter<E, K>
}

abstract class IdFilter<E:KIEntity<K>,K:Comparable<K>>(meta: KIEntityMeta<K>) : EntityFilter<E, K>(meta)
class StaticEntityFilter<E:KIEntity<K>, K:Comparable<K>>(val ids:Iterable<K>, meta: KIEntityMeta<K>) : IdFilter<E, K>(meta) {
    override fun matches(e: E): Boolean = ids.any { e.id == it }
    override fun matches(values: Map<String, Any?>): Boolean = values["_id"]?.let { ids.any(it::equals) }?:false

    override fun inverse(): IdFilter<E, K> = object : IdFilter<E, K>(meta) {
        val origin = this@StaticEntityFilter
        override fun matches(e: E): Boolean = !origin.matches(e)
        override fun matches(values: Map<String, Any?>): Boolean = !origin.matches(values)

        override fun inverse(): IdFilter<E, K> = origin
    }
}

sealed class PropertyFilter<E:KIEntity<K>,K:Comparable<K>,P>(val prop:KIProperty<P>, meta: KIEntityMeta<K>) : EntityFilter<E, K>(meta)
sealed class PropertyValueFilter<E:KIEntity<K>,K:Comparable<K>,P>(prop:KIProperty<P>, meta: KIEntityMeta<K>, val value: P) : PropertyFilter<E, K,P>(prop,meta) {
    private val _meta = meta as KIJvmEntityMeta<K>
    fun match(p:P?) : Boolean = relate(p,value)
    abstract fun relate(value:P?, test:P) : Boolean
    override fun matches(e: E): Boolean = match(_meta.get<P>(e, prop))
    override fun matches(values: Map<String, Any?>): Boolean = match(values[prop.name] as P?)
}
class EQFilter<E:KIEntity<K>,K:Comparable<K>,P:Comparable<P>>(prop:KIProperty<P>, meta: KIEntityMeta<K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value == test
    override fun inverse(): NEQFilter<E, K, P> = NEQFilter(prop, meta, value)
}
class NEQFilter<E:KIEntity<K>,K:Comparable<K>,P:Comparable<P>>(prop:KIProperty<P>, meta: KIEntityMeta<K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value != test
    override fun inverse(): EQFilter<E, K, P> = EQFilter(prop, meta, value)
}

fun<A,B,R> B.let2(a:A,b:B, cb : (a:A,b:B) -> R) : R? = if(b!=null) cb(a,b) else null
class GTFilter<E:KIEntity<K>,K:Comparable<K>,P:Comparable<P>> (prop:KIProperty<P>, meta: KIEntityMeta<K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->v>test }?:false
    override fun inverse(): LTEFilter<E, K, P> = LTEFilter(prop, meta, value)
}
class GTEFilter<E:KIEntity<K>,K:Comparable<K>,P:Comparable<P>> (prop:KIProperty<P>, meta: KIEntityMeta<K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->v>=test }?:false
    override fun inverse(): LTFilter<E, K, P> = LTFilter(prop, meta, value)
}

class LTFilter<E:KIEntity<K>,K:Comparable<K>,P:Comparable<P>> (prop:KIProperty<P>, meta: KIEntityMeta<K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->
        v < test
    } ?: true

    override fun inverse(): GTEFilter<E, K, P> = GTEFilter(prop, meta, value)
}

class LTEFilter<E:KIEntity<K>,K:Comparable<K>,P:Comparable<P>> (prop:KIProperty<P>, meta: KIEntityMeta<K>, value:P) : PropertyValueFilter<E, K, P>(prop, meta, value) {
    override fun relate(value: P?, test: P): Boolean = value?.let { v ->
        v <= test
    } ?: true

    override fun inverse(): GTFilter<E, K, P> = GTFilter(prop, meta, value)
}

sealed class CombinationFilter<E:KIEntity<K>,K:Comparable<K>>(val operands:Iterable<EntityFilter<E,K>>, meta: KIEntityMeta<K>) : EntityFilter<E,K>(meta)
class NotFilter<E:KIEntity<K>,K:Comparable<K>>(operands:Iterable<EntityFilter<E,K>>, meta: KIEntityMeta<K>) : CombinationFilter<E,K>(operands, meta) {
    override fun matches(e: E): Boolean = !operands.any {it.matches(e)}

    override fun matches(values: Map<String, Any?>): Boolean = !operands.any { it.matches(values) }

    override fun inverse(): EntityFilter<E, K> = AndFilter(operands, meta)
}
class AndFilter<E:KIEntity<K>,K:Comparable<K>>(operands:Iterable<EntityFilter<E,K>>, meta: KIEntityMeta<K>) : CombinationFilter<E,K>(operands, meta) {
    override fun matches(e: E): Boolean = operands.all { it.matches(e) }


    override fun matches(values: Map<String, Any?>): Boolean = operands.all { it.matches(values) }

    override fun inverse(): EntityFilter<E, K> = OrFilter(operands.map(EntityFilter<E,K>::inverse), meta)
}

class OrFilter<E:KIEntity<K>,K:Comparable<K>>(operands:Iterable<EntityFilter<E,K>>, meta: KIEntityMeta<K>) : CombinationFilter<E,K>(operands, meta) {
    override fun matches(e: E): Boolean = operands.any {it.matches(e)}

    override fun matches(values: Map<String, Any?>): Boolean = operands.any { it.matches(values) }

    override fun inverse(): EntityFilter<E, K> = AndFilter(operands.map { it.inverse() }, meta)
}
