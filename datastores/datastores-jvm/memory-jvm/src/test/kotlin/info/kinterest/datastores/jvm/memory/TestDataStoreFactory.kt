package info.kinterest.datastores.jvm.memory

import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

object TestDataStoreFactory : Spek( {
    given("a DataStoreFactoryProvider") {
        val fac = DataStoreFactoryProvider()
        on("loading") {
            it("should have at least on entry") {
                fac.factories.size `should be greater or equal to` 1
            }
        }
        val cfg = object : DataStoreConfig {
            override val name: String
                get() = "test"
            override val type: String
                get() = "jvm.mem"
            override val config: Map<String, Any?>
                get() = emptyMap()
        }
        val ds = fac.factories[cfg.type]!!.create(cfg)
        on("creating a ds") {
            it("should be created") {
                ds `should not be` null
            }
        }
    }
})