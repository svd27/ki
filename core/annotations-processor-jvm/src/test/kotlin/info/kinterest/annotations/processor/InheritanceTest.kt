package info.kinterest.annotations.processor

import info.kinterest.KIEntity
import info.kinterest.annotations.Entity
import info.kinterest.annotations.processor.jvm.EmployeeJvm
import info.kinterest.annotations.processor.jvm.ManagerJvm
import info.kinterest.annotations.processor.jvm.PersonJvm
import org.amshove.kluent.`should equal`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.time.LocalDate

@Entity
interface Person : KIEntity<String> {
    override val id: String
    val first: String
    val last: String
    val dob: LocalDate
}

@Entity
interface Employee : Person {
    override val id: String
    val company: String
}

@Entity
interface Manager : Employee {
    override val id: String
    val rank: String
}

class InheritanceTest : Spek({
    given("a hierarchy of entities") {
        on("having generated them") {
            it("should have proper super interfaces") {
                EmployeeJvm.Companion.Meta.parent `should equal` Person::class
                ManagerJvm.Companion.Meta.hierarchy `should equal` listOf(EmployeeJvm.meta, PersonJvm.meta)
                PersonJvm.Companion.Meta.hierarchy `should equal` listOf()
                setOf(EmployeeJvm.meta.root, ManagerJvm.meta.root, PersonJvm.meta.root) `should equal` setOf(PersonJvm.meta.root)
            }
        }
    }
})