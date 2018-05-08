package info.kinterest.jvm.datastores

import com.nhaarman.mockito_kotlin.whenever
import info.kinterest.*
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.datastores.IEntityTrace
import info.kinterest.datastores.QueryMsg
import info.kinterest.datastores.QueryResultMsg
import info.kinterest.filter.FilterWrapper
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.coreKodein
import info.kinterest.meta.*
import info.kinterest.paging.Page
import info.kinterest.paging.Paging
import info.kinterest.query.*
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
import org.kodein.di.Kodein
import kotlin.reflect.KClass

val remmeta: KIEntityMeta = object : KIEntityMeta() {
    override val root: KClass<*>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val impl: KClass<*>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val me: KClass<*>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val parent: KClass<*>?
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val name: String
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val versioned: Boolean
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val idProperty: KIProperty<*>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val props: Map<String, KIProperty<*>>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val hierarchy: List<KIEntityMeta>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val types: List<KIEntityMeta>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val idInfo: IdInfo
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override fun <K : Any> new(ds: DataStore, id: K): KIEntity<K> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class RemoteEntity(override val id: LocalDate, val name: String, var token: String, override val _store: DataStore) : KIEntity<LocalDate> {
    override val _meta: KIEntityMeta = remmeta


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

open class RemIn(name: String, override val pool: CoroutineDispatcher, override val ch: SendChannel<QueryMsg>, override val chResp: ReceiveChannel<QueryResultMsg>) : RemoteDataStoreFacade(name) {
    override val qm: QueryManager
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override val metaProvider: MetaProvider
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override fun <K : Any> version(type: KIEntityMeta, id: K): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getBookedRelationsSync(rel: KIRelationProperty, entity: KIEntity<Any>, sourceMeta: KIEntityMeta): Try<Iterable<IEntityTrace>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <E : KIEntity<K>, K : Any> retrieve(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <E : KIEntity<K>, K : Any> retrieveLenient(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <E : KIEntity<K>, K : Any> create(type: KIEntityMeta, entity: E): Try<Deferred<Try<E>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <K : Any> generateKey(type: KIEntityMeta): K {
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

    override fun setValues(type: KIEntityMeta, id: Any, version: Any, values: Map<KIProperty<*>, Any?>, retries: Int): Deferred<Try<Unit>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> addRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> removeRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> getRelations(rel: KIRelationProperty, source: S): Try<Deferred<Try<Iterable<T>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> getRelationsSync(rel: KIRelationProperty, source: S): Try<Iterable<T>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <T : KIEntity<L>, L : Any, S : KIEntity<K>, K : Any> bookRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <T : KIEntity<L>, L : Any, S : KIEntity<K>, K : Any> unbookRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> setRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> setRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> unsetRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> unsetRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    init {
        receiverInit()
    }
}

open class RemOut(kodein: Kodein, name: String, override val ds: DataStoreFacade, override val chIn: ReceiveChannel<QueryMsg>, override val chOut: SendChannel<QueryResultMsg>) : RemoteOutgoingDataStoreFacade(name, kodein) {
    override val qm: QueryManager
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    override val metaProvider: MetaProvider
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override fun <K : Any> version(type: KIEntityMeta, id: K): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <E : KIEntity<K>, K : Any> retrieve(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getBookedRelationsSync(rel: KIRelationProperty, entity: KIEntity<Any>, sourceMeta: KIEntityMeta): Try<Iterable<IEntityTrace>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <E : KIEntity<K>, K : Any> retrieveLenient(type: KIEntityMeta, ids: Iterable<K>): Try<Deferred<Try<Iterable<E>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <E : KIEntity<K>, K : Any> create(type: KIEntityMeta, entity: E): Try<Deferred<Try<E>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <K : Any> generateKey(type: KIEntityMeta): K {
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

    override fun setValues(type: KIEntityMeta, id: Any, version: Any, values: Map<KIProperty<*>, Any?>, retries: Int): Deferred<Try<Unit>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> addRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> removeRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> getRelations(rel: KIRelationProperty, source: S): Try<Deferred<Try<Iterable<T>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> getRelationsSync(rel: KIRelationProperty, source: S): Try<Iterable<T>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <T : KIEntity<L>, L : Any, S : KIEntity<K>, K : Any> bookRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <T : KIEntity<L>, L : Any, S : KIEntity<K>, K : Any> unbookRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> setRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> setRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> unsetRelation(rel: Relation<S, T, K, L>): Try<Deferred<Try<Boolean>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <S : KIEntity<K>, K : Any, T : KIEntity<L>, L : Any> unsetRelationSync(rel: Relation<S, T, K, L>): Try<Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class RemoteTest : Spek({
    given("a mock datasource and two ends of a remote datasource") {
        val channel = Channel<QueryMsg>()
        val chResp = Channel<QueryResultMsg>()
        val ds: DataStoreFacade = mock()
        val kodein = Kodein {
            import(coreKodein)
        }
        //Page(Paging(0, 1), listOf(RemoteEntity(java.time.LocalDate.now(), "", ",", ds)))
        whenever(ds.query<RemoteEntity, LocalDate>(any())).thenReturn(Try {
            CompletableDeferred(Try {
                val projection = EntityProjection<RemoteEntity, LocalDate>(Ordering.NATURAL.cast(), Paging.ALL, null)
                @Suppress("UNCHECKED_CAST")
                QueryResult(Query<RemoteEntity, LocalDate>(FilterWrapper.nofilter(remmeta), listOf()), mapOf(
                        projection to
                                EntityProjectionResult(projection, Page(Paging(0, 1),
                                        listOf(RemoteEntity(java.time.LocalDate.now(), "", ",", ds))))) as Map<Projection<RemoteEntity, LocalDate>, ProjectionResult<RemoteEntity, LocalDate>>
                )
            })
        })
        val dispatcher: CoroutineDispatcher = newFixedThreadPoolContext(2, "test")
        val remInc = RemIn("test", dispatcher, channel, chResp)
        RemOut(kodein, "test", ds, channel, chResp)

        on("querying") {
            val tq = remInc.query<RemoteEntity, LocalDate>(Query(FilterWrapper.nofilter(remmeta), listOf()))
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