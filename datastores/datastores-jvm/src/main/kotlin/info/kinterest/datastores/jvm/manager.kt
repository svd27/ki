package info.kinterest.datastores.jvm

import info.kinterest.DataStore
import kotlin.concurrent.*
import info.kinterest.DataStoreManager
import info.kinterest.KIEntity
import info.kinterest.jvm.KIJvmEntity
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.superclasses


object DataStores : info.kinterest.DataStores {
    private val rw  = ReentrantReadWriteLock()
    override val types: Map<String, DataStoreManager>
      get() = rw.read { _types.toMap() }
    private val _types : MutableMap<String,DataStoreManager> = mutableMapOf()
    override fun add(type: String, m: DataStoreManager) = rw.write {
        _types[type] = m
    }
}

class JvmEntityMeta<E:KIJvmEntity<K>, K:Comparable<K>>(val klass:KClass<E>) {
    val name : String = klass.simpleName!!
    val root : KClass<KIEntity<K>> by lazy {
        @Suppress("UNCHECKED_CAST")
        klass as KClass<KIEntity<K>>
    }

    private fun findRoot() : KClass<KIEntity<K>> = kotlin.run {
        fun findSuper(k:KClass<KIEntity<K>>) : KClass<KIEntity<K>>? = k.superclasses.firstOrNull{it.isSubclassOf(KIEntity::class)} as? KClass<KIEntity<K>>
        var ck = klass as KClass<KIEntity<K>>
        var k = ck
        while (ck!=null) {ck = findRoot(); if(ck!=null) k =ck}
        k
    }
}

interface DataStoreJvm : DataStore {

}