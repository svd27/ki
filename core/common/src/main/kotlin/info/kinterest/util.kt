package info.kinterest

inline fun<reified T> Any.cast() = this as T


fun DONTDOTHIS(msg: String = "Don't do this"): Nothing = throw Exception(msg)