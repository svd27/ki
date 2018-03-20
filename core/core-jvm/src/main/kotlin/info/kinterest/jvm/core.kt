package info.kinterest.jvm

import info.kinterest.EntitySupport
import info.kinterest.KIEntity
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty0
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties


abstract class KIJvmEntity<K:Comparable<K>> : KIEntity<K> {
    abstract val _meta : KIJvmEntityMeta<KIEntity<*>>
    abstract val _me : KClass<*>
    fun<V:Any> get(p:String) : V? = _meta.get(this, p)

    fun<V:Any> set(p:String, v:V?) = _meta.set(this, p, v)
}

interface KIJvmEntitySupport<E:KIEntity<K>,K:Comparable<K>> : EntitySupport<E,K> {
    val meta : KIJvmEntityMeta<KIEntity<*>>
}

abstract class KIJvmEntityMeta<E:KIEntity<*>>(kc:KClass<*>) {
    abstract val root : KClass<*>
    abstract val me: KClass<*>
    abstract val parent: KClass<*>?

    private val props : Map<String,KProperty1<E,*>> = kc.memberProperties.associate { it.name to it as KProperty1<E,*> }

    @Suppress("UNCHECKED_CAST")
    fun<V:Any> get(e:E, p:String) : V? = props[p]?.get(e) as V?
    @Suppress("UNCHECKED_CAST")
    fun<V:Any> set(e:E, p:String,v:V?) = props[p]?.let {
        require(it is KMutableProperty1)
        (it as KMutableProperty1<E,V?>).set(e, v)
    }
}