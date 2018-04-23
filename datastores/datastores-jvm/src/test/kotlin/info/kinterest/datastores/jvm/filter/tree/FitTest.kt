package info.kinterest.datastores.jvm.filter.tree

import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.datastores.jvm.filter.tree.jvm.SomeEntityJvm
import info.kinterest.jvm.filter.EQFilter
import info.kinterest.jvm.filter.GTFilter
import info.kinterest.jvm.filter.LTFilter
import info.kinterest.jvm.filter.filter
import info.kinterest.jvm.filter.tree.AndFit
import info.kinterest.jvm.filter.tree.bestFit
import org.amshove.kluent.`should be instance of`
import org.amshove.kluent.`should equal`
import org.amshove.kluent.`should not be null`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on


class FitTest : Spek({
    given("filter tree") {
        on("creating s simple && filter") {
            val f = filter<SomeEntity, Long>(SomeEntityJvm.meta) {
                parse("""name > "s"  && online = true""", SomeEntityJvm.meta)
            }
            val fit = bestFit(f)
            it("the fit should") {
                fit.`should not be null`()
                fit `should be instance of` AndFit::class
                val andFit = fit as AndFit
                andFit.fit.f `should be instance of` EQFilter::class
                andFit.rest.count() `should equal` 1
                andFit.rest.first().f `should be instance of` GTFilter::class
            }
        }

        on("creating s deeper && filter") {
            val f = filter<SomeEntity, Long>(SomeEntityJvm.meta) {
                parse("""name > "s"  &&  (dob < date("12.10.2001", "dd.M.yyyy") && (online = true || id > 5))""", SomeEntityJvm.meta)
            }
            val fit = bestFit(f)
            it("should create a proper filter") {
                fit.`should not be null`()
                fit `should be instance of` AndFit::class
                val andFit = fit as AndFit
                andFit.fit.f `should be instance of` GTFilter::class
                andFit.rest.count() `should equal` 2
                andFit.rest.first().f `should be instance of` LTFilter::class
            }
        }

    }
})