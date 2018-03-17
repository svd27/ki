package info.kinterest.annotations.processor.generators

import org.yanex.takenoko.KoType
import javax.lang.model.type.TypeKind

val TypeKind.koType
  get() = when(this) {
      TypeKind.BOOLEAN -> KoType.BOOLEAN
      TypeKind.BYTE -> KoType.BYTE
      TypeKind.CHAR -> KoType.CHAR
      TypeKind.DOUBLE -> KoType.DOUBLE
      TypeKind.FLOAT -> KoType.FLOAT
      TypeKind.INT -> KoType.INT
      TypeKind.LONG -> KoType.LONG
      TypeKind.SHORT -> KoType.SHORT
      else -> throw IllegalStateException("unsupported primitive $this")
  }