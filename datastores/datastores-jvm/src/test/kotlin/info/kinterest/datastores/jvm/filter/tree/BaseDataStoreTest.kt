package info.kinterest.datastores.jvm.filter.tree

import info.kinterest.KIEntity
import info.kinterest.MetaProvider
import info.kinterest.datastores.jvm.datasourceKodein
import info.kinterest.datastores.jvm.memory.JvmMemoryDataStore
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.coreKodein
import info.kinterest.jvm.datastores.DataStoreConfig
import info.kinterest.jvm.datastores.IDataStoreFactoryProvider
import info.kinterest.jvm.tx.TransactionManager
import info.kinterest.jvm.tx.jvm.CreateTransactionJvm
import info.kinterest.jvm.tx.jvm.TransactionJvm
import kotlinx.coroutines.experimental.*
import mu.KLogging
import org.kodein.di.Kodein
import org.kodein.di.erased.bind
import org.kodein.di.erased.instance
import org.kodein.di.erased.singleton

class BaseDataStoreTest(cfg: DataStoreConfig) {
    val kodein = Kodein {
        import(coreKodein)
        import(datasourceKodein)
        bind<DataStoreConfig>("tx-store") with singleton {
            object : DataStoreConfig {
                override val name: String
                    get() = "tx-store"
                override val type: String
                    get() = "jvm.mem"
                override val config: Map<String, Any?>
                    get() = emptyMap()
            }
        }
    }


    val context: CoroutineDispatcher = newFixedThreadPoolContext(4, "base.test")

    val provider by kodein.instance<IDataStoreFactoryProvider>()

    val ds = provider.create(cfg).getOrElse { throw it } as JvmMemoryDataStore

    init {
        val tm: TransactionManager by kodein.instance()
        tm.txStore.name
    }
    val metaProvider by kodein.instance<MetaProvider>()

    init {
        metaProvider.register(TransactionJvm.Meta)
        metaProvider.register(CreateTransactionJvm.Meta)
    }

    inline fun <reified E : KIEntity<K>, K : Comparable<K>> create(entity: E): Try<E> = run {

        val tryC = ds.create(metaProvider.meta(E::class)!!, entity)
        val await = tryC.map { runBlocking { withTimeout(300) { it.await() } } }
        await.getOrElse { throw it }
    }

    inline fun <reified E : KIEntity<K>, K : Any> retrieve(ids: Iterable<K>): Try<Iterable<E>> {
        return run {
            val meta = ds[E::class]
            ds.retrieve<E, K>(meta, ids).map { runBlocking(context) { yield(); delay(200); withTimeout(300) { it.await() } } }.getOrElse { throw it }
        }
    }

    companion object : KLogging()
}