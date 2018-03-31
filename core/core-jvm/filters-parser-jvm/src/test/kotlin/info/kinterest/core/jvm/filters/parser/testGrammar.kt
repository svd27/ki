package info.kinterest.core.jvm.filters.parser

import info.kinterest.DONTDOTHIS
import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.TransientEntity
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.filter.GTFilter
import info.kinterest.jvm.filter.KIFilter
import info.kinterest.jvm.filter.filter
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import org.amshove.kluent.`should be instance of`
import org.amshove.kluent.mock
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.time.LocalDate
import kotlin.reflect.KClass

class TestFilter(override val id: String, val top: Int?, val date: LocalDate) : KIEntity<String> {
    @Suppress("PropertyName")
    override val _store: DataStore
        get() = DONTDOTHIS("not implemented")

    @Suppress("PropertyName")
    override val _meta: KIEntityMeta
        get() = Meta

    @Suppress("UNCHECKED_CAST", "IMPLICIT_CAST_TO_ANY")
    override fun <V> getValue(prop: KIProperty<V>): V? = when (prop.name) {
        "top" -> top
        "date" -> date
        else -> DONTDOTHIS()
    } as V?

    override fun <V> setValue(prop: KIProperty<V>, v: V?) {
        DONTDOTHIS("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V> setValue(prop: KIProperty<V>, version: Any, v: V?) {
        DONTDOTHIS("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun asTransient(): TransientEntity<String> {
        DONTDOTHIS("not implemented")
    }

    companion object {
        object Meta : KIJvmEntityMeta(TestFilter::class, TestFilter::class) {
            override val root: KClass<*>
                get() = TestFilter::class
            override val parent: KClass<*>?
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
    given("a string") {
        Metas.register(TestFilter.Companion.Meta)
        val f = filter<TestFilter, String>(mock(), TestFilter.Companion.Meta) {
            parse("TestFilter{top > 5}", Metas)
        }
        on("parsing it") {
            it("should be parsed as a Filter") {
                f `should be instance of` KIFilter::class
                f `should be instance of` EntityFilter.FilterWrapper::class
                (f as EntityFilter.FilterWrapper).f `should be instance of` GTFilter::class
            }
        }
        val f1 = filter<TestFilter, String>(mock(), TestFilter.Companion.Meta) {
            parse<TestFilter, String>("TestFilter{(top < 2&&top>=0) || top<20}", Metas)
        }
    }

    given("a string with a date") {
        on("parsing") {
            val f = filter<TestFilter, String>(mock(), TestFilter.Companion.Meta) {
                parse("TestFilter{date < date(\"12.3.2014\",\"d.M.yyyy\")}", Metas)
            }
            it("should not fail") {}
            it("should be a proper filter") {
                f `should be instance of` EntityFilter.FilterWrapper::class
            }
        }
    }
})