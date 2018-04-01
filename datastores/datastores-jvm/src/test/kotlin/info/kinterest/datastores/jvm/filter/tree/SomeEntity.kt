package info.kinterest.datastores.jvm.filter.tree

import info.kinterest.KIEntity
import info.kinterest.LocalDate
import info.kinterest.annotations.Entity
import info.kinterest.annotations.StorageTypes

@Entity
@StorageTypes(["jvm.mem"])
interface SomeEntity : KIEntity<Long> {
    override val id: Long
    val name: String
    var online: Boolean
    var dob: LocalDate?
}