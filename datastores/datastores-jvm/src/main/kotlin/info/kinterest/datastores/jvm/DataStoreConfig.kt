package info.kinterest.datastores.jvm

import info.kinterest.jvm.datastores.DataStoreConfig


class DataStoreConfigImpl(override val name : String, override val type : String, override val config : Map<String,Any?>) : DataStoreConfig