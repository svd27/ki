package info.kinterest.datastores.jvm.memory

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import info.kinterest.*
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.datastores.jvm.datasourceKodein
import info.kinterest.jvm.coreKodein
import info.kinterest.meta.KIEntityMeta
import kotlinx.coroutines.experimental.runBlocking
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.Spec

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
    inline fun<reified E:KIEntity<K>,K:Comparable<K>> create(id:K, values:Map<String,Any?>) : Try<K> = run {
        val tryC = ds.create<K>(E::class, id, values)
        val await = tryC.map { runBlocking { it.await() } }
        await.flatten()
    }

    inline fun<reified E:KIEntity<K>, K:Any> retrieve(ids:Iterable<K>) : Try<Iterable<E>> = run {
        val meta = ds[E::class]
        ds.retrieve<E,K>(meta, ids).map { runBlocking { it.await() } }.getOrElse { throw it }
    }
}