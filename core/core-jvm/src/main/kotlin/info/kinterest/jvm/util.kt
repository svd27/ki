package info.kinterest.jvm

import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.async

fun <T, R> Deferred<T>.map(cb: (T) -> R): Deferred<R> = async { cb(if (this@map.isCompleted) this@map.getCompleted() else await()) }