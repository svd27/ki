package info.kinterest.jvm

//fun <T, R> Deferred<T>.map(cb: (T) -> R): Deferred<R> = async { cb(if (this@map.isCompleted) this@map.getCompleted() else await()) }