package info.kinterest.jvm.interest

import info.kinterest.DataStore
import org.amshove.kluent.mock
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given

class BasicInterestTest : Spek({
    given("a datastore") {
        val ds: DataStore = mock()
        //whenever(ds.q)
    }
})