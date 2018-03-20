package info.kinterest.datastores.jvm.memory

import info.kinterest.datastores.jvm.DataStoreFactory
import info.kinterest.datastores.jvm.DataStoreFactoryProvider
import io.kotlintest.matchers.haveLength
import io.kotlintest.matchers.haveSize
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec

class TestDataStoreFactory : WordSpec( {
    "factory should have at least on entry" should {
        val fac = DataStoreFactoryProvider()
        println("!!! ${fac.factories}")
        fac.factories.size shouldBe 1
    }
})