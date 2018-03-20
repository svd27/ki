package info.kinterest.datastores.jvm

interface DataStoreConfig {
    val name: String
    val type: String
    val config: Map<String, Any?>
}

class DataStoreConfigImpl(override val name : String, override val type : String, override val config : Map<String,Any?>) : DataStoreConfig