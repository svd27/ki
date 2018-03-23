package info.kinterest.jvm

import info.kinterest.DataStore
import info.kinterest.EntitySupport
import info.kinterest.KIEntity
import info.kinterest.cast
import org.jetbrains.annotations.Nullable
import kotlin.reflect.*
import kotlin.reflect.full.memberProperties


@Suppress("UNCHECKED_CAST")
abstract class KIJvmEntity<E:KIEntity<K>,K:Comparable<K>> : KIEntity<K> {
    abstract val _meta : KIJvmEntityMeta<E,K>
    abstract val _me : KClass<*>
    inline fun<reified V:Any> get(p:String) : V? = _meta.get<V>(this as E, p)
    inline fun<reified V:Any> set(p:String, v:V?) = _meta.set<V>(this  as E, p, v)
}

interface KIJvmEntitySupport<E:KIEntity<K>,K:Comparable<K>> : EntitySupport<E,K> {
    val meta : KIJvmEntityMeta<E,K>
}


abstract class KIJvmEntityMeta<E:KIEntity<K>,K:Comparable<K>>(val impl:KClass<*>, val me: KClass<*>) {
    val name = me.simpleName!!
    abstract val root : KClass<*>
    abstract val parent: KClass<*>?

    private val props : Map<String,Property> = me.memberProperties.associate { it.name to Property(it.cast()) }
    operator fun get(n:String) : Property? = props[n]
    inner class Property(val kProperty: KProperty1<E,*>) {
        val name:String = kProperty.name
        val type:KClass<*> = kProperty.returnType.classifier!!.cast()
        val readOnly:Boolean = kProperty !is KMutableProperty1
        val nullable:Boolean = kProperty.annotations.any { it is Nullable }
        val transient:Boolean = kProperty.annotations.any { it is Transient }

        inline fun<reified V:Any> get(e:E) : V? = kProperty.get(e)?.cast()
        inline fun<reified V:Any> set(e:E, v:V?) = run {
            require(!readOnly)
            require(nullable || v!=null)
            kProperty.cast<KMutableProperty1<E,V?>>().set(e, v)
        }
    }

    @Suppress("UNCHECKED_CAST")
    inline fun<reified V:Any> get(e:E, p:String) : V? = this[p]?.get(e) as V?
    @Suppress("UNCHECKED_CAST")
    inline fun<reified V:Any> set(e:E, p:String,v:V?) = this[p]?.let {
        require(!it.readOnly)
        it.set(e, v)
    }

    private val ctor = findCtor()
    private fun findCtor() = run {
        val ctor = impl.constructors.first()
        println("ctor $ctor ${ctor.parameters.size}")
        assert(ctor.parameters.size==2)
        ctor
    }

    fun new(ds: DataStore, id:Any) : KIEntity<K> = ctor.call(ds, id).cast()

}