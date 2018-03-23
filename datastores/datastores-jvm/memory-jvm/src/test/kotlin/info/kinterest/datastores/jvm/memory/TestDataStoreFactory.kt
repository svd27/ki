package info.kinterest.datastores.jvm.memory

import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactory
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import io.kotlintest.matchers.*
import io.kotlintest.specs.WordSpec

class TestDataStoreFactory : WordSpec( {
    "factory should have at least on entry" should {
        val fac = DataStoreFactoryProvider()
        fac.factories.size shouldBe beGreaterThan(0)
    }

    "given a cfg" should {
        val cfg = object : DataStoreConfig {
            override val name: String
                get() = "test"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = emptyMap()
        }
        val fac = DataStoreFactoryProvider()
        val ds = fac.factories[cfg.type]!!.create(cfg)
        println(ds)
        ds shouldNotBe null
    }
})