package info.kinterest.jvm.filter

import info.kinterest.DataStore
import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.jvm.MetaProvider
import org.amshove.kluent.`should equal`
import org.amshove.kluent.mock
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

object TestEqualsToString : Spek({
    val metaProvider = MetaProvider()
    metaProvider.register(TestFilter.Companion.Meta)
    val ds = mock<DataStore>()

    given("two filters") {
        val f1 = filter<TestFilter,String>(ds, TestFilter.Companion.Meta) {
            parse("(number > 5 && number <= 10) || (number > 10 && number <= 20)", metaProvider)
        } as EntityFilter.FilterWrapper
        val f2 = filter<TestFilter,String>(ds, TestFilter.Companion.Meta) {
            parse("(number > 10 && number <= 20) || (number > 5 && number <= 10)", metaProvider)
        } as EntityFilter.FilterWrapper
        on("parsing the toString result") {
            val pf1 = filter<TestFilter,String>(ds, TestFilter.Companion.Meta) {
                parse(f1.toString(), metaProvider)
            } as EntityFilter.FilterWrapper
            it("should be equal to itself") {
                pf1.f `should equal`  f1.f
            }
            it("should equal the other filter") {
                f1.f `should equal` f2.f
            }
            val pf2 = filter<TestFilter,String>(ds, TestFilter.Companion.Meta) {
                parse(f2.toString(), metaProvider)
            } as EntityFilter.FilterWrapper

            it("should equal the other parsed" ) {
                f1.f `should equal` pf2.f
            }
        }
    }

    given("two date filters") {
        val f1 = filter<TestFilter,String>(ds, TestFilter.Companion.Meta) {
            parse("""date > date("20170511", "yyyyMMdd") && date < date("20170512", "yyyyMMdd")""", metaProvider)
        } as EntityFilter.FilterWrapper
        val f2 = filter<TestFilter,String>(ds, TestFilter.Companion.Meta) {
            parse("""date < date("12.05.2017", "dd.MM.yyyy") && date > date("20170511", "yyyyMMdd") """, metaProvider)
        } as EntityFilter.FilterWrapper
        on("comparing them") {
            it("they should equal each other") {
                f1.f `should equal` f2.f
            }
        }
    }
})