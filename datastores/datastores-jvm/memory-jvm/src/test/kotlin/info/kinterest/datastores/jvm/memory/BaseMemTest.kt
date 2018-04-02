package info.kinterest.datastores.jvm.memory

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import info.kinterest.KIEntity
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.datastores.jvm.datasourceKodein
import info.kinterest.functional.Try
import info.kinterest.functional.flatten
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.coreKodein
import kotlinx.coroutines.experimental.runBlocking

class BaseMemTest(cfg:DataStoreConfig)  {
    val kodein = Kodein {
        import(coreKodein)
        import(datasourceKodein)
    }
    init {
        kodein.instance<DataStoreFactoryProvider>().inject(kodein)
    }


    val provider = kodein.instance<DataStoreFactoryProvider>()
    val fac = provider.factories[cfg.type]
    val ds = fac!!.create(cfg) as JvmMemoryDataStore
    val metaProvider = kodein.instance<MetaProvider>()
    inline fun<reified E:KIEntity<K>,K:Comparable<K>> create(id:K, values:Map<String,Any?>) : Try<K> = run {
        val meta = metaProvider.meta(E::class)
        val tryC = ds.create(meta!!, listOf(id to values))
        val await = tryC.map { runBlocking { it.await() } }
        await.flatten().map { it.first() }
    }

    inline fun<reified E:KIEntity<K>, K:Any> retrieve(ids:Iterable<K>) : Try<Iterable<E>> = run {
        val meta = ds[E::class]
        ds.retrieve<E,K>(meta, ids).map { runBlocking { it.await() } }.getOrElse { throw it }
    }
}