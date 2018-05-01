package info.kinterest.functional

sealed class Try<out R> {
    val isSuccess = this is Success<R>
    val isFailure = this is Failure

    abstract fun <T> map(m: (R) -> T): Try<T>

    fun toEither(): Either<Exception, R> = when (this) {
        is Success -> Either.right(res)
        is Failure -> Either.left(error)
    }

    class Success<R>(val res: R) : Try<R>() {
        override fun <T> map(m: (R) -> T): Try<T> = Try.Companion<T>({ m(res) })
    }

    class Failure<R>(val error: Exception) : Try<R>() {
        @Suppress("UNCHECKED_CAST")
        override fun <T> map(m: (R) -> T): Try<T> = raise(error)
    }

    inline fun <B> fold(crossinline fa: (Exception) -> B, crossinline fb: (R) -> B): B =
            when (this) {
                is Failure -> fa(error)
                is Success -> fb(res)
            }

    companion object {
        fun <R> raise(e: Exception): Failure<R> = Failure(e)
        operator fun <R> invoke(inv: () -> R): Try<R> = try {
            Success(inv())
        } catch (e: Exception) {
            errorHandler(e)
            Failure(e)
        }

        fun <R> succeed(r: R): Success<R> = Success(r)

        var errorHandler: (e: Exception) -> Unit = {}
    }
}

fun <T> Try<Try<T>>.flatten(): Try<T> = this.fold({ ex -> Try.raise(ex) }, { it })
fun <B> Try<B>.getOrElse(default: (Throwable) -> B): B = fold(default, { it })
fun <R> Try<R>.getOrDefault(cb: (Exception) -> R): R = fold(cb, { it })

sealed class Either<out L, out R>() {
    val isLeft = this is Left
    val isRight = this is Right

    fun swap(): Either<R, L> = when (this) {
        is Left -> right(left)
        is Right -> left(right)
    }

    class Left<out L, out R>(val left: L) : Either<L, R>()

    class Right<out L, out R>(val right: R) : Either<L, R>()

    inline fun <Out> fold(crossinline lf: (L) -> Out, crossinline rf: (R) -> Out): Out = when (this) {
        is Left -> lf(left)
        is Right -> rf(right)
    }

    companion object {
        fun <L, R> left(l: L): Either<L, R> = Left(l)
        fun <L, R> right(r: R): Either<L, R> = Right(r)
    }
}

fun <L, R> Either<L, R>.getOrElse(f: (L) -> R): R = fold(f, { it })