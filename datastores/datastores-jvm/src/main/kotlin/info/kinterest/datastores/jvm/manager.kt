package info.kinterest.datastores.jvm

import com.github.salomonbrys.kodein.*
import info.kinterest.*
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.meta.KIEntityMeta
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

    abstract fun<E:KIEntity<K>,K:Comparable<K>> query(type:KIEntityMeta<K>, f:EntityFilter<E,K>) : Try<Deferred<Try<Iterable<K>>>>
    abstract fun<E:KIEntity<K>,K:Comparable<K>> retrieve(type:KIEntityMeta<K>,ids:Iterable<K>) : Try<Deferred<Try<Iterable<E>>>>
    abstract fun<K:Comparable<K>> create(type:KIEntityMeta<K>, entities:Iterable<Pair<K,Map<String, Any?>>>) : Try<Deferred<Try<Iterable<K>>>>
    abstract fun<K:Comparable<K>> delete(type:KIEntityMeta<K>, entities:Iterable<K>) : Try<Deferred<Either<DataStoreError,Iterable<K>>>>
    val metaProvider by instance<MetaProvider>()
}

val datasourceKodein = Kodein.Module {
    bind<DataStoreFactoryProvider>() with singleton { DataStoreFactoryProvider() }
}