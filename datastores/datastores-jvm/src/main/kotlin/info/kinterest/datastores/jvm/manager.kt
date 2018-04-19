package info.kinterest.datastores.jvm

import com.github.salomonbrys.kodein.*
import info.kinterest.DataStore
import info.kinterest.DataStoreEvent
import info.kinterest.EntityEvent
import info.kinterest.MetaProvider
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.datastores.IRelationTrace
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.query.QueryManager
import kotlinx.coroutines.experimental.channels.Channel
import mu.KLogging
import java.io.Serializable
import java.util.*


interface DataStoreFactory : KodeinInjected {
    var kodein : Kodein
    val events: Channel<DataStoreEvent>

    fun create(cfg: DataStoreConfig) : DataStore
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
}