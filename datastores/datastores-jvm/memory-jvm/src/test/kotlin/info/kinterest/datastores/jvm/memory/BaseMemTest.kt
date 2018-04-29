package info.kinterest.datastores.jvm.memory

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.bind
import com.github.salomonbrys.kodein.instance
import com.github.salomonbrys.kodein.singleton
import info.kinterest.EntityEvent
import info.kinterest.KIEntity
import info.kinterest.MetaProvider
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.datastores.jvm.datasourceKodein
import info.kinterest.functional.Try
import info.kinterest.functional.flatten
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.coreKodein
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.query.QueryManager
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.runBlocking

class BaseMemTest(vararg cfg: DataStoreConfig) {
    val kodein = Kodein {
        import(coreKodein)
        import(datasourceKodein)
        bind<DataStoreConfig>("tx-store") with singleton {
            object : DataStoreConfig {
                override val name: String = "tx-store"
                override val type: String = "jvm.mem"
                override val config: Map<String, Any?> = emptyMap()
            }
        }
    }
    val dss: Iterable<DataStoreFacade>
    val ds: DataStoreFacade
    val qm: QueryManager
    init {
        kodein.instance<DataStoreFactoryProvider>().inject(kodein)
        val provider = kodein.instance<DataStoreFactoryProvider>()
        dss = cfg.map { provider.create(it).getOrElse { throw it } }
        ds = dss.first()
        qm = kodein.instance()
    }


    val metaProvider = kodein.instance<MetaProvider>()
    val dispatcher: Dispatcher<EntityEvent<*, *>> = kodein.instance("entities")
    val events: Channel<EntityEvent<*, *>> = Channel()

    fun <E : KIEntity<K>, K : Comparable<K>> create(e: E): Try<E> = run {
        val tryC = ds.create(e._meta, listOf(e))
        val await = tryC.map { runBlocking { it.await() } }
        await.flatten().map { it.first() }
    }

    inline fun<reified E:KIEntity<K>, K:Any> retrieve(ids:Iterable<K>) : Try<Iterable<E>> = run {
        val meta = metaProvider.meta(E::class)!!
        ds.retrieve<E,K>(meta, ids).map { runBlocking { it.await() } }.getOrElse { throw it }
    }
}