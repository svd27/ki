package info.kinterest.datastores.jvm

import com.github.salomonbrys.kodein.*
import info.kinterest.DataStoreEvent
import info.kinterest.EntityEvent
import info.kinterest.MetaProvider
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.datastores.IRelationTrace
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.jvm.tx.TransactionManager
import info.kinterest.jvm.tx.TransactionManagerJvm
import info.kinterest.query.QueryManager
import kotlinx.coroutines.experimental.channels.Channel
import mu.KLogging
import java.io.Serializable
import java.util.*


interface DataStoreFactory : KodeinInjected {
    val events: Channel<DataStoreEvent>

    fun create(cfg: DataStoreConfig): DataStoreJvm
}

class DataStoreFactoryProvider : KodeinInjected {
    override val injector: KodeinInjector = KodeinInjector()

    val factories = mutableMapOf<String,DataStoreFactory>()
    init {
        onInjected {
            kodein ->
            logger.debug { "loading factories" }
            this.javaClass.classLoader.getResources("datastore-factory.properties").iterator().forEach {
                val props = Properties()
                it.openStream().use {
                    props.load(it)
                }
                props.forEach { n, v ->
                    logger.debug { "loading $n = $v" }
                    factories[n.toString()] = (Class.forName(v.toString()).newInstance() as DataStoreFactory).apply { inject(kodein) }
                }
            }
        }
    }

    fun create(cfg: DataStoreConfig): Try<DataStoreFacade> = Try {
        factories[cfg.type]!!.create(cfg)
    }

    companion object : KLogging()
}


abstract class DataStoreJvm(name: String) : KodeinInjected, DataStoreFacade(name) {
    override val injector: KodeinInjector = KodeinInjector()

    protected val events: Dispatcher<EntityEvent<*, *>> by instance("entities")
    override val metaProvider by instance<MetaProvider>()
    override val qm by instance<QueryManager>()

}


data class RelationTrace(override val type: String, override val id: Any, override val ds: String) : Serializable, IRelationTrace

val datasourceKodein = Kodein.Module {
    bind<DataStoreFactoryProvider>() with singleton { DataStoreFactoryProvider() }
    bind<DataStoreFacade>("tx-store") with singleton {
        val cfg: DataStoreConfig = instance("tx-store")
        val fp: DataStoreFactoryProvider = instance()
        fp.create(cfg).getOrElse { throw it }
    }
    bind<TransactionManager>() with singleton {
        TransactionManagerJvm().apply { inject(kodein) }
    }

}