package info.kinterest.core.jvm.filters.parser

import info.kinterest.*
import info.kinterest.filter.Filter
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.filter.GTFilter
import info.kinterest.jvm.filter.filter
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import org.amshove.kluent.`should be instance of`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.time.LocalDate
import kotlin.reflect.KClass

@Suppress("MemberVisibilityCanBePrivate")
class TestFilter(override val id: String, val top: Int?, val date: LocalDate) : KIEntity<String> {
    @Suppress("UNUSED_PARAMETER", "unused")
    constructor(ds: DataStore, id: String) : this(id, null, LocalDate.now())
    @Suppress("PropertyName")
    override val _store: DataStore
        get() = DONTDOTHIS("not implemented")

    @Suppress("PropertyName")
    override val _meta: KIEntityMeta
        get() = Meta

    @Suppress("UNCHECKED_CAST", "IMPLICIT_CAST_TO_ANY")
    override fun <V, P : KIProperty<V>> getValue(prop: P): V? = when (prop.name) {
        "top" -> top
        "date" -> date
        else -> DONTDOTHIS()
    } as V?

    override fun <V, P : KIProperty<V>> setValue(prop: P, v: V?) {
        DONTDOTHIS("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V, P : KIProperty<V>> setValue(prop: P, version: Any, v: V?) {
        DONTDOTHIS("not implemented") //To change body of created functions use File | Settings | File Templates.
    }


    override fun asTransient(): KITransientEntity<String> {
        DONTDOTHIS()
    }

    companion object {
        val Meta: KIJvmEntityMeta by lazy {
            object : KIJvmEntityMeta(TestFilter::class, TestFilter::class) {
                override val root: KClass<*>
                    get() = TestFilter::class
                override val parent: KClass<*>?
                    get() = null
                override val versioned: Boolean
                    get() = false

                override val hierarchy: List<KIEntityMeta>
                    get() = listOf()
            }
        }
    }
}

val Metas = MetaProvider().apply {
    register(TestFilter.Companion.Meta)
}


object SimpleTest : Spek({
    given("a string") {
        val f = filter<TestFilter, String>(TestFilter.Companion.Meta) {
            parse("TestFilter{top > 5}", TestFilter.Companion.Meta)
        }
        on("parsing it") {
            it("should be parsed as a Filter") {
                f `should be instance of` Filter::class
                f `should be instance of` EntityFilter.LiveFilterWrapper::class
                f.f `should be instance of` GTFilter::class
            }
        }
    }

    given("a string with a date") {
        on("parsing") {
            val f = filter<TestFilter, String>(TestFilter.Companion.Meta) {
                parse("TestFilter{date < date(\"12.3.2014\",\"d.M.yyyy\")}", TestFilter.Companion.Meta)
            }
            it("should not fail") {}
            it("should be a proper filter") {
                f `should be instance of` EntityFilter.LiveFilterWrapper::class
            }
        }
    }
})