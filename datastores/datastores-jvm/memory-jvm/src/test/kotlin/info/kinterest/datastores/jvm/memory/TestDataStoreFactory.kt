package info.kinterest.datastores.jvm.memory

import com.github.salomonbrys.kodein.Kodein
import com.github.salomonbrys.kodein.instance
import info.kinterest.datastores.jvm.DataStoreConfig
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.datastores.jvm.datasourceKodein
import info.kinterest.jvm.coreKodein
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

object TestDataStoreFactory : Spek( {
    val kodein = Kodein {
        import(coreKodein)
        import(datasourceKodein)
    }
    kodein.instance<DataStoreFactoryProvider>().inject(kodein)

    given("a DataStoreFactoryProvider") {
        val fac = kodein.instance<DataStoreFactoryProvider>()
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
                ds `should be instance of` JvmMemoryDataStore::class
            }
        }
    }
})