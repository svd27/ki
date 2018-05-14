package info.kinterest.datastores.jvm.filter.tree

import info.kinterest.KIEntity
import info.kinterest.LocalDate
import info.kinterest.jvm.annotations.Entity

@Entity
interface SomeEntity : KIEntity<Long> {
    override val id: Long
    val name: String
    var online: Boolean
    var dob: LocalDate?
}