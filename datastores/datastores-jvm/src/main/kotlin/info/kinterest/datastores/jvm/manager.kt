package info.kinterest.datastores.jvm

import info.kinterest.DataStoreEvent
import info.kinterest.EntityEvent
import info.kinterest.MetaProvider
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.datastores.IEntityTrace
import info.kinterest.functional.Try
import info.kinterest.jvm.datastores.DataStoreConfig
import info.kinterest.jvm.datastores.IDataStoreFactoryProvider
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.jvm.tx.TransactionManager
import info.kinterest.jvm.tx.TransactionManagerJvm
import info.kinterest.query.QueryManager
import kotlinx.coroutines.experimental.channels.Channel
import mu.KLogging
import org.kodein.di.Kodein
import org.kodein.di.KodeinAware
import org.kodein.di.erased.bind
import org.kodein.di.erased.instance
import org.kodein.di.erased.singleton
import java.io.Serializable
import java.util.*


interface DataStoreFactory : KodeinAware {
    val events: Channel<DataStoreEvent>

    fun create(cfg: DataStoreConfig): DataStoreJvm
}


class DataStoreFactoryProvider(override val kodein: Kodein) : KodeinAware, IDataStoreFactoryProvider {
    val factories: Map<String, DataStoreFactory>
    init {
        logger.debug { "loading factories" }
        factories = this.javaClass.classLoader.getResources("datastore-factory.properties").toList().flatMap {
            val props = Properties()
            it.openStream().use {
                props.load(it)
            }
            props.map { (n, v) ->
                logger.debug { "loading $n = $v" }
                //factories[n.toString()] = (Class.forName(v.toString()).newInstance() as DataStoreFactory)
                n.toString() to Class.forName(v.toString()).kotlin.constructors.first {
                    logger.debug { "$it pars: ${it.parameters}" }
                    it.parameters.size == 1 && it.parameters.any {
                        logger.debug { "par: $it type: ${it.type} ${it.type.classifier == Kodein::class}" }
                        it.type.classifier == Kodein::class
                    }
                }.call(kodein) as DataStoreFactory
            }
        }.toMap()

    }

    override fun create(cfg: DataStoreConfig): Try<DataStoreFacade> = Try {
        factories[cfg.type]!!.create(cfg)
    }

    companion object : KLogging()
}


abstract class DataStoreJvm(name: String, override val kodein: Kodein) : KodeinAware, DataStoreFacade(name) {
    protected val events: Dispatcher<EntityEvent<*, *>> by instance("entities")
    override val metaProvider by instance<MetaProvider>()
    override val qm by instance<QueryManager>()
    protected val tm: TransactionManager by lazy {
        val t by kodein.instance<TransactionManager>()
        t
    }
}


data class EntityTrace(override val type: String, override val id: Any, override val ds: String) : Serializable, IEntityTrace {
    override fun equals(other: Any?): Boolean = _equals(other)
}

val datasourceKodein = Kodein.Module {
    bind<IDataStoreFactoryProvider>() with singleton { DataStoreFactoryProvider(kodein) }

    bind<TransactionManager>() with singleton {
        TransactionManagerJvm(kodein)
    }
}
