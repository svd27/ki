package info.kinterest.annotations.processor

import info.kinterest.DONTDOTHIS
import info.kinterest.KIEntity
import info.kinterest.KIVersionedEntity
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Try
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.annotations.TypeArgs
import info.kinterest.query.QueryManager
import kotlinx.coroutines.experimental.Deferred

interface SomeGeneric<G> {
    fun doStuff() : G
}

@Entity
interface SomeEntity<G> : KIEntity<Long>, SomeGeneric<G> {
    override val id: Long
        get() = DONTDOTHIS("")
}

@Entity
interface SomeImpl : SomeEntity<Long> {
    override fun doStuff(): Long = 0
}

enum class TxState {
    WAITING,PROCESSING,FAILING,FAILED,DONE
}

interface TransactionManager {
    val qm : QueryManager
    val txStore : DataStoreFacade
    fun<R> done(r:R, tx:Transaction<R>)

    operator fun plus(tx:Transaction<*>) : Try<Unit>

    fun<R> addAndExecute(tx:Transaction<R>) : Try<Deferred<Try<R>>>

}

@Entity
interface Transaction<R> : KIVersionedEntity<Long> {
    override val id: Long
        get() = DONTDOTHIS()
    var state: TxState


    fun execute(tm:TransactionManager, transaction: Iterable<Transaction<*>>) : Iterable<Transaction<*>>
}

@Entity
interface CreateTransaction : Transaction<KIEntity<Any>> {
    val entity : KIEntity<Any>
        @TypeArgs(arrayOf(Any::class))
      get() = DONTDOTHIS("")
    override fun execute(tm: TransactionManager, transaction: Iterable<Transaction<*>>): Iterable<Transaction<*>> = listOf()
}