package info.kinterest.jvm.query

import info.kinterest.paging.Paging
import mu.KLogging
import org.amshove.kluent.`should equal`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on


class PagingTest : Spek({
    given("a paging") {
        val paging = Paging(0, 10)
        on("selecting next") {
            val p = paging.next
            it("should have a proper offset") {
                p.offset `should equal` 10
            }
        }
        on("selecting next twice") {
            val p = paging.next.next
            logger.debug { p }
            it("should have a proper offset") {
                p.offset `should equal` 20
            }
        }
    }
}) {
    companion object : KLogging()
}