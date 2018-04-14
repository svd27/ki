package info.kinterest.annotations.processor

import info.kinterest.KIEntity
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.annotations.Relation
import org.jetbrains.spek.api.Spek

@Entity
//@Relations(
//rels =[
//    (Relation(targetVal = "Person", property = "likes")),
//    (Relation(targetVal = "Person", property = "friends")),
//    (Relation(targetVal = "Person", property = "bestFriend")),
//    (Relation(targetVal = "Employee", property = "customerOf"))
//]
//)
interface RelPerson : KIEntity<String> {
    override val id: String
    var name: String


    val likes: MutableSet<RelPerson>
        @Relation(target = RelPerson::class, targetId = String::class) get() = TODO()

    val friends: MutableSet<RelPerson>
        @Relation(target = RelPerson::class, targetId = String::class) get() = TODO()

    var bestFriend: RelPerson?
        @Relation(target = RelPerson::class, targetId = String::class) get() = TODO()
        set(value) = TODO()

    var customerOf: RelEmployee?
        @Relation(target = RelEmployee::class, targetId = String::class) get() = TODO()
        set(value) = TODO()
}

@Entity
//@Relations(rels = [
//    (Relation(property = "superior", targetVal = "Employee")),
//    (Relation(property = "colleagues", targetVal = "Employee"))
//])
interface RelEmployee : RelPerson {
    override val id: String
    val rank: String

    var superior: RelEmployee?
        @Relation(target = RelEmployee::class, targetId = String::class) get() = TODO()
        set(value) = TODO()

    val colleagues: Set<RelEmployee>
        @Relation(target = RelEmployee::class, targetId = String::class, mutableCollection = false) get() = TODO()
}

class RelationTest : Spek({

})