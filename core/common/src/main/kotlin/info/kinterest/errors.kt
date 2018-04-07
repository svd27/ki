package info.kinterest

import info.kinterest.meta.KIEntityMeta
import info.kinterest.query.Query
import kotlin.reflect.KClass


sealed class KIError(msg: String, cause: Throwable?) : Exception(msg, cause)
class KIFatalError(msg: String, cause: Throwable?) : KIError(msg, cause)

sealed class KIRecoverableError(msg: String, cause: Throwable?) :
        KIError(msg, cause)

class QueryError(val q: Query<*, *>, msg: String, cause: Throwable? = null) : KIRecoverableError(msg, cause)

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
                EntityError(meta, key, ds, "version for Entity ${meta.name} with id $key not found", cause)

        class VersionAlreadyExists(meta: KIEntityMeta, key: Any, ds: DataStore, cause: Throwable? = null) :
                EntityError(meta, key, ds, "version for Entity ${meta.name} with id $key not found", cause)
    }

    class MetaDataNotFound(@Suppress("CanBeParameter", "MemberVisibilityCanBePrivate") val kc: KClass<*>, ds: DataStore, cause: Throwable? = null) :
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
