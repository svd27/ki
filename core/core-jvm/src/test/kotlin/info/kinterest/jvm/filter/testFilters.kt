package info.kinterest.jvm.filter

import info.kinterest.DataStore
import info.kinterest.Klass
import info.kinterest.TransientEntity
import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.MetaProvider
import mu.KotlinLogging
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.reflect.KClass

val log = KotlinLogging.logger {  }
class TestFilter(override val id:String, val number:Long) : KIJvmEntity<TestFilter,String>() {
    override val _meta: KIJvmEntityMeta
        get() = Meta
    override val _me: KClass<*>
        get() = Meta.me
    override val _store: DataStore
        get() = TODO("not implemented")

    override fun asTransient(): TransientEntity<String> {
        TODO("not implemented")
    }

    companion object {
        object Meta : KIJvmEntityMeta(TestFilter::class, TestFilter::class) {
            override val root: Klass<*>
                get() = TestFilter::class
            override val parent: Klass<*>?
                get() = null


        }
    }
}



object TestSimpleFilter : Spek({
    val Metas = MetaProvider()
    Metas.register(TestFilter.Companion.Meta)
    given("a simple set of entities") {
        val entities = mutableListOf<TestFilter>()
        for(c in 'A'..'Z') {
            entities += TestFilter("$c", c.toLong()-'A'.toLong())
        }
        val fids = filter<TestFilter,String>(TestFilter.Companion.Meta) {
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
            it("adding both filter and its inverse should give the orignal set") {
                log.debug { together }
                together `should equal` entities.toSet()
            }
        }

        val lt = parse<TestFilter,String>("TestFilter{number>24}", Metas)
        val ltr = entities.filter { lt.matches(it) }
        on("filtering on a field") {
            it("should return the proper result") {
                ltr.size `should be greater than` 0
                ltr.size `should be` 1
                ltr.map { it.id }.toSet() `should equal`  setOf("Z")
            }
        }
        val ids = parse<TestFilter,String>("TestFilter{id > \"W\"}", Metas)
        on("filtering on ids") {
            it("should be a propert filter") {
                ids `should be instance of` GTFilter::class
                val gt = ids as GTFilter<TestFilter,String,String>
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
            val ids = parse<TestFilter,String>("TestFilter{id >=\"W\"}", Metas)
            it("should filter") {
                val idF = entities.filter { ids.matches(it) }
                idF.size `should equal` 4
            }
        }

        on("reversed id filter") {
            val ids = parse<TestFilter,String>("TestFilter{\"W\"<=id}", Metas)
            it("should filter") {
                val idF = entities.filter { ids.matches(it) }
                idF.size `should equal` 3
            }
        }
    }
})