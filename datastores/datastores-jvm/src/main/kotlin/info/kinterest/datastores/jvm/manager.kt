package info.kinterest.datastores.jvm

import com.github.salomonbrys.kodein.*
import info.kinterest.DataStore
import info.kinterest.DataStoreError
import info.kinterest.EntityEvent
import info.kinterest.KIEntity
import info.kinterest.functional.Either
import info.kinterest.functional.Try
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.events.Dispatcher
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page
import info.kinterest.query.Query
import kotlinx.coroutines.experimental.Deferred
import java.util.*


interface DataStoreFactory : KodeinInjected {
    var kodein : Kodein

    fun create(cfg: DataStoreConfig) : DataStore
}

class DataStoreFactoryProvider() : KodeinInjected {
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


abstract class DataStoreJvm(override val name : String) : DataStore, KodeinInjected {
    override val injector: KodeinInjector = KodeinInjector()

    protected val events: Dispatcher<EntityEvent<*, *>> by instance("entities")
    val metaProvider by instance<MetaProvider>()

    abstract fun <E : KIEntity<K>, K : Any> query(type: KIEntityMeta, f: EntityFilter<E, K>): Try<Deferred<Try<Iterable<K>>>>
    abstract fun <E : KIEntity<K>, K : Any> query(query: Query<E, K>): Try<Deferred<Try<Page<E, K>>>>
    abstract fun<E:KIEntity<K>,K:Any> retrieve(type:KIEntityMeta,ids:Iterable<K>) : Try<Deferred<Try<Iterable<E>>>>
    abstract fun<K:Any> create(type:KIEntityMeta, entities:Iterable<Pair<K,Map<String, Any?>>>) : Try<Deferred<Try<Iterable<K>>>>
    abstract fun <K : Any> delete(type: KIEntityMeta, entities: Iterable<K>): Try<Deferred<Either<DataStoreError, Iterable<K>>>>
    abstract fun getValues(type: KIEntityMeta, id: Any): Deferred<Try<Map<String, Any?>?>>
    abstract fun getValues(type: KIEntityMeta, id: Any, vararg props: KIProperty<*>): Deferred<Try<Map<String, Any?>?>>
    abstract fun getValues(type: KIEntityMeta, id: Any, props: Iterable<KIProperty<*>>): Deferred<Try<Map<String, Any?>?>>
    abstract fun setValues(type: KIEntityMeta, id: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>>
    abstract fun setValues(type: KIEntityMeta, id: Any, version: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>>

}

val datasourceKodein = Kodein.Module {
    bind<DataStoreFactoryProvider>() with singleton { DataStoreFactoryProvider() }
}