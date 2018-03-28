package info.kinterest

import info.kinterest.meta.KIEntityMeta

val NULL: Any? = null

expect interface Klass<T : Any> {
    val simpleName: String?
}

expect class UUID

interface Keyed<out T : Comparable<T>> {
    val id: T
}

expect interface DataStore {
    val name: String
}

interface DataStoreManager {
    val type: String
    val dataStores: Map<String, DataStore>
    fun add(ds: DataStore)
}

/**
 * this will be a singleton on any platform instance
 */
interface DataStores {
    val types: Map<String, DataStoreManager>
    fun add(type: String, m: DataStoreManager)
}

interface KIEntity<T : Comparable<T>> : Keyed<T> {
    val _store: DataStore
    val _meta: KIEntityMeta<T>
    fun asTransient(): TransientEntity<T>
}

interface TransientEntity<T : Comparable<T>> : KIEntity<T> {
    val values: Map<String, Any?>
}

interface EntitySupport<out E : KIEntity<K>, K : Comparable<K>> {
    /**
     * creates a new transient entity, requires that all properties are given in their ctor order
     */
    fun transient(id: K?, values: Map<String, Any?>): TransientEntity<K>

    fun <DS : DataStore> create(ds: DS, id: K, values: Map<String, Any?>)
}

interface Versioned<out V> {
    val _version: V
}

sealed class KIError(msg: String, cause: Throwable?) : Exception(msg, cause)
class KIFatalError(msg: String, cause: Throwable?) : KIError(msg, cause)

sealed class KIRecoverableError(msg: String, cause: Throwable?, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
        KIError(msg, cause)

class FilterError(msg: String, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
        KIRecoverableError(msg, cause, enableSuppression, writeableStackTrace)

sealed class DataStoreError(val ds: DataStore, msg: String, cause: Throwable?, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
        KIRecoverableError(msg, cause, enableSuppression, writeableStackTrace) {
    sealed class EntityError(val meta: KIEntityMeta<*>, val key: Comparable<*>, ds: DataStore, msg: String, cause: Throwable? = null) : DataStoreError(ds, msg, cause) {
        class EntityNotFound(meta: KIEntityMeta<*>, key: Comparable<*>, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
                EntityError(meta, key, ds, "Entity ${meta.name} with Key $key not found in DataStore ${ds.name}", cause)

        class EntityExists(meta: KIEntityMeta<*>, key: Comparable<*>, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
                EntityError(meta, key, ds, "Entity ${meta.name} with Key $key already exists in DataStore ${ds.name}", cause)
        class VersionNotFound(meta: KIEntityMeta<*>, key: Comparable<*>, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
                EntityError(meta, key, ds,"version for Entity ${meta.name} with id $key not found", cause)

        class VersionAlreadyExists(meta: KIEntityMeta<*>, key: Comparable<*>, ds: DataStore, cause: Throwable? = null) :
                EntityError(meta, key, ds, "version for Entity ${meta.name} with id $key not found", cause)
    }

    class MetaDataNotFound(val kc: Klass<*>, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
            DataStoreError(ds, "Metadata for Entity ${kc} not found", cause, enableSuppression, writeableStackTrace)

    class OptimisticLockException(val meta: KIEntityMeta<*>, val key: Comparable<*>, val expectedVersion: Any, val actualVersion: Any, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) :
            DataStoreError(ds, "wrong version for ${meta.name} with id $key, expected: $expectedVersion, actual: $actualVersion", cause, enableSuppression, writeableStackTrace)

    class BatchError(msg: String, val meta: KIEntityMeta<*>, ds: DataStore, cause: Throwable? = null, enableSuppression: Boolean = false, writeableStackTrace: Boolean = true) : DataStoreError(ds, msg, cause, enableSuppression, writeableStackTrace)
}
