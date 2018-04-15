package info.kinterest.jvm.datastores

import com.nhaarman.mockito_kotlin.whenever
import info.kinterest.DataStore
import info.kinterest.KIEntity
import info.kinterest.LocalDate
import info.kinterest.cast
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.datastores.QueryMsg
import info.kinterest.datastores.QueryResultMsg
import info.kinterest.filter.NOFILTER
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty
import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.query.EntityProjection
import info.kinterest.query.EntityProjectionResult
import info.kinterest.query.Query
import info.kinterest.query.QueryResult
import info.kinterest.sorting.Ordering
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import org.amshove.kluent.`should be true`
import org.amshove.kluent.`should equal`
import org.amshove.kluent.any
import org.amshove.kluent.mock
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

class RemoteEntity(override val id: LocalDate, val name: String, var token: String, override val _store: DataStore) : KIEntity<LocalDate> {
    override val _meta: KIEntityMeta
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override fun asTransient(): RemoteEntity {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V, P : KIProperty<V>> getValue(prop: P): V? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V, P : KIProperty<V>> setValue(prop: P, v: V?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <V, P : KIProperty<V>> setValue(prop: P, version: Any, v: V?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

abstract class BaseDataSource(override val name: String) : DataStoreFacade {
    override fun <K : Any> version(type: KIEntityMeta, id: K): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }


    override fun <E : KIEntity<K>, K : Any> create(type: KIEntityMeta, entities: Iterable<E>): Try<Deferred<Try<Iterable<E>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <E : KIEntity<K>, K : Any> retrieveLenient(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <E : KIEntity<K>, K : Any> retrieve(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <E : KIEntity<K>, K : Any> delete(type: KIEntityMeta, entities: Iterable<E>): Try<Deferred<Try<Iterable<K>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getValues(type: KIEntityMeta, id: Any): Deferred<Try<Map<String, Any?>?>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getValues(type: KIEntityMeta, id: Any, vararg props: KIProperty<*>): Deferred<Try<Map<String, Any?>?>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getValues(type: KIEntityMeta, id: Any, props: Iterable<KIProperty<*>>): Deferred<Try<Map<String, Any?>?>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setValues(type: KIEntityMeta, id: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setValues(type: KIEntityMeta, id: Any, version: Any, values: Map<KIProperty<*>, Any?>): Deferred<Try<Unit>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }


}

open class RemIn(name: String, override val pool: CoroutineDispatcher, override val ch: SendChannel<QueryMsg>, override val chResp: ReceiveChannel<QueryResultMsg>) : BaseDataSource(name), RemoteDataStoreFacade {
    override var pendingQueries: Map<Long, Pair<Query<*, *>, CompletableDeferred<Try<QueryResult<*, *>>>>> = mapOf()

    final override fun receiverInit() = super.receiverInit()


    init {
        receiverInit()
    }
}

open class RemOut(override val ds: DataStoreFacade, override val chIn: ReceiveChannel<QueryMsg>, override val chOut: SendChannel<QueryResultMsg>, override val pool: CoroutineDispatcher) : BaseDataSource(ds.name), RemoteOutgoingDataStoreFacade {
    var id: Long = 0
    override val nextId: Long
        get() = id++

    final override fun initReceiver() = super.initReceiver()

    init {
        initReceiver()
    }
}

class RemoteTest : Spek({
    given("a mock datasource and two ends of a remote datasource") {
        val channel = Channel<QueryMsg>()
        val chResp = Channel<QueryResultMsg>()
        val ds: DataStoreFacade = mock()
        whenever(ds.name).thenReturn("test")
        //Page(Paging(0, 1), listOf(RemoteEntity(java.time.LocalDate.now(), "", ",", ds)))
        whenever(ds.query<RemoteEntity, LocalDate>(any())).thenReturn(Try { CompletableDeferred(Try { QueryResult(Query<RemoteEntity, LocalDate>(NOFILTER.cast(), listOf()), mapOf("entities" to EntityProjectionResult(EntityProjection(Ordering.NATURAL.cast(), Paging.ALL, null), Page(Paging(0, 1), listOf(RemoteEntity(java.time.LocalDate.now(), "", ",", ds)))))) }) })
        val dispatcher: CoroutineDispatcher = newFixedThreadPoolContext(4, "test")
        val remInc = RemIn(ds.name, dispatcher, channel, chResp)
        RemOut(ds, channel, chResp, dispatcher)

        on("querying") {
            val tq = remInc.query<RemoteEntity, LocalDate>(Query(NOFILTER.cast(), listOf()))
            it("should be a success") {
                tq.isSuccess.`should be true`()
            }
            val deferred = tq.getOrElse { throw it }
            val res = runBlocking { deferred.await() }
            it("the deferred should also be a success") {
                res.isSuccess.`should be true`()
            }
            val qr = res.getOrElse { throw it }
            qr.projections.size `should equal` 1
            val proj = qr.projections.map { it.value }.filterIsInstance<EntityProjectionResult<RemoteEntity, LocalDate>>().first()
            proj.name `should equal` "entities"
            proj.page.entities.size `should equal` 1

        }
    }
})