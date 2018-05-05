package info.kinterest.datastores.jvm.memory

import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import info.kinterest.datastores.jvm.datasourceKodein
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.coreKodein
import info.kinterest.jvm.datastores.DataStoreConfig
import info.kinterest.jvm.datastores.IDataStoreFactoryProvider
import org.amshove.kluent.`should be greater or equal to`
import org.amshove.kluent.`should be instance of`
import org.amshove.kluent.`should be true`
import org.amshove.kluent.`should not be`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.kodein.di.Kodein
import org.kodein.di.erased.instance

object TestDataStoreFactory : Spek( {
    given("a DataStoreFactoryProvider") {
        val kodein = Kodein {
            import(coreKodein)
            import(datasourceKodein)
        }

        val fac by kodein.instance<IDataStoreFactoryProvider>()
        on("loading") {
            it("should have at least on entry") {
                (fac as? DataStoreFactoryProvider)?.factories?.size ?: 0 `should be greater or equal to` 1
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
        val dst = fac.create(cfg)
        on("creating a ds") {
            it("should be created") {
                dst.isSuccess.`should be true`()
                val ds = dst.getOrElse { throw it }
                ds `should not be` null
                ds `should be instance of` JvmMemoryDataStore::class
            }
        }
    }
})