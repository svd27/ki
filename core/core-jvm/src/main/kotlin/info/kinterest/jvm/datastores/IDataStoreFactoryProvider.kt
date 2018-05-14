package info.kinterest.jvm.datastores

import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Try

interface DataStoreConfig {
    val name: String
    val type: String
    val config: Map<String, Any?>
}


interface IDataStoreFactoryProvider {
    fun create(cfg: DataStoreConfig): Try<DataStoreFacade>
}