package info.kinterest.annotations.processor

import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.TransientEntity
import info.kinterest.datastores.jvm.memory.JvmMemoryDataStore
import info.kinterest.datastores.jvm.memory.KIJvmMemEntity
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.KIJvmEntitySupport
import java.util.*
import kotlin.reflect.KClass

class ExTotalTestJvmMem(store: JvmMemoryDataStore, id: UUID, total: Int?, adapt: Boolean?) : KIJvmMemEntity<UUID>(store, id), TotalTest {
    constructor(ds:JvmMemoryDataStore, id:UUID, map:Map<String,Any?>) : this(ds, id, map["total"] as Int?, map["addap"] as Boolean?)
    override val _meta: KIJvmEntityMeta<KIEntity<*>>
        get() = Meta
    override val _me: KClass<*>
        get() = _meta.me

    override val total: Int? = total
    override var adapt: Boolean? = adapt


    override fun asTransient(): Transient = Transient(id, mapOf("id" to id))

    companion object : KIJvmEntitySupport<TotalTest, UUID> {
        object Meta : KIJvmEntityMeta<KIEntity<*>>(ExTotalTestJvmMem::class) {
            override val root: KClass<*> = TotalTest::class
            override val me: KClass<*> = TotalTest::class
            override val parent: KClass<*>? = null
        }
        class Transient(private val _id:UUID?, private val values:Map<String,Any?>) : TotalTest, TransientEntity<UUID> {
            override val id: UUID
                get() = if(_id!=null) _id else TODO("not implemented")
            override val _store: DataStore
                get() = TODO("not implemented")
            override val total: Int?  by values
            override var adapt: Boolean? = values["adapt"] as Boolean?

            override fun asTransient(): Transient = this
        }

        override val meta: KIJvmEntityMeta<KIEntity<*>>
            get() = Meta

        override fun transient(id:UUID?, values: Map<String,Any?>): Transient = Transient(id, values)
        override fun<DS:DataStore> create(ds: DS, id:UUID, map: Map<String, Any?>): TotalTest = ExTotalTestJvmMem(ds as JvmMemoryDataStore, id, map)
    }


}

class Y {
    init {
        Y.all { true }
        Y.Z[0]
    }
    internal companion object Z : List<Int> by mutableListOf()
}