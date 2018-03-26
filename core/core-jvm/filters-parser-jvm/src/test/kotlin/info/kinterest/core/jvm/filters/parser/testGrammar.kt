package info.kinterest.core.jvm.filters.parser

import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.Klass
import info.kinterest.TransientEntity
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.filter.GTFilter
import info.kinterest.jvm.filter.KIFilter
import info.kinterest.jvm.filter.LTFilter
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import org.amshove.kluent.`should be instance of`
import org.amshove.kluent.`should not be instance of`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.time.LocalDate

class TestFilter(override val id : String, val top:Int?, val date:LocalDate) : KIEntity<String> {
    override val _store: DataStore
        get() = TODO("not implemented")

    override val _meta: KIEntityMeta<String>
        get() = Meta

    override fun asTransient(): TransientEntity<String> {
        TODO("not implemented")
    }

    companion object {
        object Meta : KIJvmEntityMeta<TestFilter,String>(TestFilter::class,TestFilter::class) {
            override val root: Klass<*>
                get() = TestFilter::class
            override val parent: Klass<*>?
                get() = null
        }
        init {
            println("register $Meta")
            Metas.register(Meta)
        }
    }
}

val Metas = MetaProvider()

object SimpleTest : Spek({
    Metas.register(TestFilter.Companion.Meta)
    given("a string") {
        val f = parse<TestFilter,String>("TestFilter{top > 5}", Metas)
        on("parsing it") {
            it("should be parsed as a Filter") {
                f `should be instance of` KIFilter::class
                f `should be instance of` GTFilter::class
            }
        }
        val f1 = parse<TestFilter,String>("TestFilter{(top < 2&&top>=0) || top<20}", Metas)
    }

    given("a string with a date") {
        on("parsing") {
            val f = parse<TestFilter,String>("TestFilter{date < date(\"12.3.2014\",\"d.M.yyyy\")}", Metas)
            it("should not fail") {}
            it("should be a proper filter") {
                f `should be instance of` LTFilter::class
            }
        }


    }
})