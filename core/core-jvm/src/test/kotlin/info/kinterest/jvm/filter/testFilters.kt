package info.kinterest.jvm.filter

import info.kinterest.DONTDOTHIS
import info.kinterest.Klass
import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.MetaProvider
import info.kinterest.meta.KIProperty
import mu.KotlinLogging
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.time.LocalDate
import kotlin.reflect.KClass

val log = KotlinLogging.logger {  }

@Suppress("MemberVisibilityCanBePrivate", "PropertyName")
class TestFilter(id: String, val number: Long, val date: LocalDate) : KIJvmEntity<TestFilter, String>(mock(), id) {
    override val _meta: KIJvmEntityMeta
        get() = Meta
    override val _me: KClass<*>
        get() = Meta.me

    @Suppress("UNCHECKED_CAST", "IMPLICIT_CAST_TO_ANY")
    override fun <V, P : KIProperty<V>> getValue(prop: P): V? = when (prop.name) {
        "number" -> number
        "date" -> date
        "id" -> id
        else -> DONTDOTHIS("unknown prop ${prop.name}")
    } as V?


    override fun <V, P : KIProperty<V>> setValue(prop: P, v: V?) {
        DONTDOTHIS("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V, P : KIProperty<V>> setValue(prop: P, version: Any, v: V?) {
        DONTDOTHIS("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun asTransient(): TestFilter {
        DONTDOTHIS("not implemented")
    }

    companion object {
        object Meta : KIJvmEntityMeta(TestFilter::class, TestFilter::class) {
            override val root: Klass<*>
                get() = TestFilter::class
            override val parent: Klass<*>?
                get() = null
            override val versioned: Boolean
                get() = false
        }
    }
}



object TestSimpleFilter : Spek({
    val metaProvider = MetaProvider()
    metaProvider.register(TestFilter.Companion.Meta)
    given("a simple set of entities") {
        val entities = mutableListOf<TestFilter>()
        for(c in 'A'..'Z') {
            entities += TestFilter("$c", c.toLong()-'A'.toLong(), LocalDate.now())
        }
        val fids = filter<TestFilter, String>(TestFilter.Companion.Meta) {
            ids("A","B","C")
        }
        val res =entities.filter(fids::matches)
        on("creating a simple id filter") {
            it("should return the proper result") {
                res.size `should equal` 3
                res.map(TestFilter::id).toSet() `should equal`  setOf("A", "B", "C")
            }
            val inv = fids.inverse()
            val rinv = entities.filter(inv::matches)
            it("its inverse should not contain any of the result") {

                rinv.size `should equal` 23
            }
            val together = (entities.filter(fids::matches) + entities.filter(inv::matches)).toSet()
            it("adding both filter and its inverse should give the original set") {
                log.debug { together }
                together `should equal` entities.toSet()
            }
        }

        val lt = filter<TestFilter, String>(TestFilter.Companion.Meta) {
            parse("TestFilter{number>24}", TestFilter.Companion.Meta)
        }
        val ltr = entities.filter { lt.matches(it) }
        on("filtering on a field") {
            it("should return the proper result") {
                ltr.size `should be greater than` 0
                ltr.size `should be` 1
                ltr.map { it.id }.toSet() `should equal`  setOf("Z")
            }
        }
        val ids = filter<TestFilter, String>(TestFilter.Companion.Meta) {
            parse("TestFilter{id > \"W\"}", TestFilter.Companion.Meta)
        }
        on("filtering on ids") {
            it("should be a property filter") {
                ids `should be instance of` EntityFilter.FilterWrapper::class
                ids.f `should be instance of` IdComparisonFilter::class
                val idf = ids.f as IdComparisonFilter
                val gt = idf.valueFilter as GTFilter<TestFilter, String, String>
                gt.prop.name `should equal` "id"
                gt.value `should equal` "W"
            }
            it("should not accept 'A'") {
                entities[0].id `should equal` "A"
                ids.matches(entities[0]) `should not be equal to` true
            }
            it("should filter") {
                val idF = entities.filter { ids.matches(it) }
                idF.size `should equal` 3
            }
        }
        on("another id filter") {
            val ids1 = filter<TestFilter, String>(TestFilter.Companion.Meta) {
                parse("TestFilter{id >=\"W\"}", TestFilter.Companion.Meta)
            }
            it("should filter") {
                val idF = entities.filter { ids1.matches(it) }
                idF.size `should equal` 4
            }
        }

        on("reversed id filter") {
            val ids1 = filter<TestFilter, String>(TestFilter.Companion.Meta) { parse("TestFilter{\"W\"<=id}", TestFilter.Companion.Meta) }
            it("should filter") {
                val idF = entities.filter { ids1.matches(it) }
                idF.size `should equal` 3
            }
        }
    }
})