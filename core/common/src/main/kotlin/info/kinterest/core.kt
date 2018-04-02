package info.kinterest

import info.kinterest.meta.KIEntityMeta
import info.kinterest.meta.KIProperty


@Suppress("unused")
expect interface Klass<T : Any> {
    val simpleName: String?
}

expect class UUID

interface Keyed<out T:Any> {
    val id: T
}

expect interface DataStore {
    val name: String
}

interface KIEntity<out T:Any> : Keyed<T> {
    @Suppress("PropertyName")
    val _store: DataStore
    @Suppress("PropertyName")
    val _meta: KIEntityMeta
    fun asTransient(): TransientEntity<T>
    fun <V : Any?, P : KIProperty<V>> getValue(prop: P): V?
    fun <V : Any?, P : KIProperty<V>> setValue(prop: P, v: V?)
    fun <V : Any?, P : KIProperty<V>> setValue(prop: P, version: Any, v: V?)
}

interface TransientEntity<out T:Any> : KIEntity<T> {
    val values: Map<String, Any?>
}

interface EntitySupport<K : Any> {
    /**
     * creates a new transient entity, requires that all properties are given in their ctor order
     */
    fun transient(id: K?, values: Map<String, Any?>): TransientEntity<K>

    fun <DS : DataStore> create(ds: DS, id: K, values: Map<String, Any?>)
}

interface Versioned<out V> {
    @Suppress("PropertyName")
    val _version: V
}

sealed class KIError(msg: String, cause: Throwable?) : Exception(msg, cause)
class KIFatalError(msg: String, cause: Throwable?) : KIError(msg, cause)

sealed class KIRecoverableError(msg: String, cause: Throwable?) :
        KIError(msg, cause)

class FilterError(msg: String, cause: Throwable? = null) :
        KIRecoverableError(msg, cause)

sealed class DataStoreError(val ds: DataStore, msg: String, cause: Throwable?) :
        KIRecoverableError(msg, cause) {
    @Suppress("unused")
    sealed class EntityError(val meta: KIEntityMeta, val key: Any, ds: DataStore, msg: String, cause: Throwable? = null) : DataStoreError(ds, msg, cause) {
        class EntityNotFound(meta: KIEntityMeta, key: Any, ds: DataStore, cause: Throwable? = null) :
                EntityError(meta, key, ds, "Entity ${meta.name} with Key $key not found in DataStore ${ds.name}", cause)

        class EntityExists(meta: KIEntityMeta, key: Any, ds: DataStore, cause: Throwable? = null) :
                EntityError(meta, key, ds, "Entity ${meta.name} with Key $key already exists in DataStore ${ds.name}", cause)

        class VersionNotFound(meta: KIEntityMeta, key: Any, ds: DataStore, cause: Throwable? = null) :
                EntityError(meta, key, ds,"version for Entity ${meta.name} with id $key not found", cause)

        class VersionAlreadyExists(meta: KIEntityMeta, key: Any, ds: DataStore, cause: Throwable? = null) :
                EntityError(meta, key, ds, "version for Entity ${meta.name} with id $key not found", cause)
    }

    class MetaDataNotFound(@Suppress("CanBeParameter", "MemberVisibilityCanBePrivate") val kc: Klass<*>, ds: DataStore, cause: Throwable? = null) :
            DataStoreError(ds, "Metadata for Entity $kc not found", cause)

    @Suppress("MemberVisibilityCanBePrivate", "CanBeParameter")
    class OptimisticLockException(val meta: KIEntityMeta, val key: Any, val expectedVersion: Any, val actualVersion: Any, ds: DataStore, cause: Throwable? = null) :
            DataStoreError(ds, "wrong version for ${meta.name} with id $key, expected: $expectedVersion, actual: $actualVersion", cause)

    class BatchError(msg: String, val meta: KIEntityMeta, ds: DataStore, cause: Throwable? = null) : DataStoreError(ds, msg, cause)
}

sealed class InterestError(msg: String, cause: Throwable?) : KIRecoverableError(msg, cause) {
    class InterestCreatiopError(msg: String, cause: Throwable?) : InterestError(msg, cause)
    class InterestQueryError(val i: Interest<*, *>, msg: String, cause: Throwable?) : InterestError(msg, cause)
}
