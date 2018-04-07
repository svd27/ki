package info.kinterest.datastores.jvm

import com.github.salomonbrys.kodein.*
import info.kinterest.DataStore
import info.kinterest.EntityEvent
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.events.Dispatcher
import java.util.*


interface DataStoreFactory : KodeinInjected {
    var kodein : Kodein

    fun create(cfg: DataStoreConfig) : DataStore
}

class DataStoreFactoryProvider : KodeinInjected {
    override val injector: KodeinInjector = KodeinInjector()

    val factories = mutableMapOf<String,DataStoreFactory>()
    init {
        onInjected {
            kodein ->
            this.javaClass.classLoader.getResources("datasource-factory.properties").iterator().forEach {
                val props = Properties()
                it.openStream().use {
                    props.load(it)
                }
                props.forEach { n, v ->
                    factories[n.toString()] = (Class.forName(v.toString()).newInstance() as DataStoreFactory).apply { inject(kodein) }
                }
            }
        }

    }
}


abstract class DataStoreJvm(override val name: String) : KodeinInjected, DataStoreFacade {
    override val injector: KodeinInjector = KodeinInjector()

    protected val events: Dispatcher<EntityEvent<*, *>> by instance("entities")
    val metaProvider by instance<MetaProvider>()

}

val datasourceKodein = Kodein.Module {
    bind<DataStoreFactoryProvider>() with singleton { DataStoreFactoryProvider() }
}