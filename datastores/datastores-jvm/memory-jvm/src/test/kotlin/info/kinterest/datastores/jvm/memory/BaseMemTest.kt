package info.kinterest.datastores.jvm.memory

import info.kinterest.*
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.meta.KIEntityMeta
import kotlinx.coroutines.experimental.runBlocking
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.Spec

class BaseMemTest(cfg:DataStoreConfig)  {
    val provider = DataStoreFactoryProvider()
    val fac = provider.factories[cfg.type]
    val ds = fac!!.create(cfg) as JvmMemoryDataStore
    inline fun<reified E:KIEntity<K>,K:Comparable<K>> create(id:K, values:Map<String,Any?>) : Try<K> = run {
        val tryC = ds.create<K>(E::class, id, values)
        val await = tryC.map { runBlocking { it.await() } }
        await.flatten()
    }

    inline fun<reified E:KIEntity<K>, K:Comparable<K>> retrieve(ids:Iterable<K>) : Try<Iterable<E>> = run {
        val meta = ds[E::class] as KIEntityMeta<K>
        ds.retrieve<E,K>(meta, ids).map { runBlocking { it.await() } }.getOrElse { throw it }
    }
}