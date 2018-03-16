package info.kinterest.jvm

import info.kinterest.KIEntity
import kotlin.reflect.KClass

interface KIJvmEntity<T:Comparable<T>> : KIEntity<T> {

}

interface KIJvmEntityMeta<T:Comparable<T>> {
    val root : KClass<KIEntity<T>>
}