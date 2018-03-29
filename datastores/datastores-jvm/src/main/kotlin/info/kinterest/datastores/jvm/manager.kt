package info.kinterest.datastores.jvm

import com.github.salomonbrys.kodein.*
import info.kinterest.*
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import kotlinx.coroutines.experimental.Deferred
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.reflect.KClass


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

    abstract fun<E:KIEntity<K>,K:Any> query(type:KIEntityMeta, f:EntityFilter<E,K>) : Try<Deferred<Try<Iterable<K>>>>
    abstract fun<E:KIEntity<K>,K:Any> retrieve(type:KIEntityMeta,ids:Iterable<K>) : Try<Deferred<Try<Iterable<E>>>>
    abstract fun<K:Any> create(type:KIEntityMeta, entities:Iterable<Pair<K,Map<String, Any?>>>) : Try<Deferred<Try<Iterable<K>>>>
    abstract fun<K:Any> delete(type:KIEntityMeta, entities:Iterable<K>) : Try<Deferred<Either<DataStoreError,Iterable<K>>>>
    abstract fun getValues(type: KIEntityMeta, id:Any) : Map<String,Any?>?
    abstract fun getValues(type: KIEntityMeta, id:Any, vararg props:KIProperty<*>) : Map<String,Any?>?
    abstract fun getValues(type: KIEntityMeta, id:Any, props:Iterable<KIProperty<*>>) : Map<String,Any?>?
    abstract fun setValues(type: KIEntityMeta, id:Any, values:Map<KIProperty<*>, Any?>)
    val metaProvider by instance<MetaProvider>()
}

val datasourceKodein = Kodein.Module {
    bind<DataStoreFactoryProvider>() with singleton { DataStoreFactoryProvider() }
}