package info.kinterest.annotations.processor.generators

import info.kinterest.GeneratedId
import info.kinterest.KIEntity
import info.kinterest.annotations.processor.Processor
import info.kinterest.datastores.jvm.memory.JvmMemoryDataStore
import info.kinterest.datastores.jvm.memory.KIJvmMemEntity
import info.kinterest.jvm.KIJvmEntity
import org.jetbrains.annotations.Nullable
import org.yanex.takenoko.*
import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.lang.model.element.*
import javax.lang.model.type.*
import javax.tools.Diagnostic

class EntityInfo(val type: TypeElement, round: RoundEnvironment, env: ProcessingEnvironment) {
    val srcPkg = type.qualifiedName.split('.').dropLast(1).joinToString(separator = ".")
    val targetPkg = "$srcPkg.${JvmMemoryGenerator.store}"

    init {
        env.note("targetPkg: $targetPkg")
    }

    val name = type.simpleName.toString() + JvmMemoryGenerator.suffix;
    val idGetter = type.enclosedElements.filter { el: Element? -> el is ExecutableElement }.first { it.simpleName.toString().equals("getId") }
    val idExecutableType: ExecutableType = idGetter.asType() as ExecutableType
    val idDecType = when (idExecutableType.returnType) {
        is DeclaredType -> idExecutableType.returnType as DeclaredType
        else -> throw IllegalStateException("wrong type ${idExecutableType.returnType}")
    }
    val idType = when (idDecType.asElement()) {
        is TypeElement -> idDecType.asElement() as TypeElement
        else -> throw IllegalStateException("wrong type ${idDecType.asElement()}")
    }
    val fields: List<FieldInfo> = type.enclosedElements.filterIsInstance<ExecutableElement>().filter {
        env.note("filter: $it")
        it.simpleName.toString() != "getId" && Modifier.STATIC !in it.modifiers && it.simpleName.startsWith("get")
                && it.simpleName.toString().length > 3
    }.map {
        val nm = it.simpleName.toString().substring(3).decapitalize()
        val setterNm = "set${nm.capitalize()}"
        val setter = type.enclosedElements.filterIsInstance<ExecutableElement>().filter { it.simpleName.toString() == setterNm }
        FieldInfo(
                nm,
                it.returnType,
                setter.isEmpty(),
                it.getAnnotation(Nullable::class.java) != null
        )
    }

    class IdInfo(val type: TypeElement, val generateKey: Boolean)


    class FieldInfo(val name: String, _type: TypeMirror, val readOnly: Boolean, val nullable: Boolean, val transient: Boolean = false) {
        val type: Typing = when (_type) {
            is DeclaredType -> when (_type.asElement()) {
                is TypeElement -> Typing.Declared(_type.asElement() as TypeElement)
                else -> throw IllegalStateException("wrong type ${_type.asElement()}")
            }
            is PrimitiveType -> Typing.Primitive(_type)
            else -> throw IllegalStateException("wrong type ${_type} ${_type::class}")
        }

        fun Name.normalize(): String = if (startsWith("java.lang")) {
            val str = toString()
            when (str) {
                java.lang.Boolean::class.java.canonicalName -> Boolean::class.qualifiedName!!
                java.lang.Byte::class.java.canonicalName -> Byte::class.qualifiedName!!
                java.lang.Character::class.java.canonicalName -> Char::class.qualifiedName!!
                java.lang.Double::class.java.canonicalName -> Double::class.qualifiedName!!
                java.lang.Float::class.java.canonicalName -> Float::class.qualifiedName!!
                java.lang.Integer::class.java.canonicalName -> Int::class.qualifiedName!!
                java.lang.Short::class.java.canonicalName -> Short::class.qualifiedName!!

                else -> toString()
            }
        } else {
            this.toString()
        }


        val koType = when (type) {
            is Typing.Declared -> parseType(type.type.qualifiedName.normalize())
            is Typing.Primitive -> type.type.kind.koType
        }.let { if (nullable) it.nullable else it }

        override fun toString(): String =
                "FieldInfo(name='$name', nullable=$nullable, transient=$transient, type=$type, koType=$koType)"

    }
}

sealed class Typing {
    class Declared(val type: TypeElement) : Typing()
    class Primitive(val type: PrimitiveType) : Typing()
}

object JvmMemoryGenerator : Generator {
    override val store: String
        get() = "jvm.mem"
    val suffix = store.split('.').map { it.capitalize() }.joinToString("")


    override fun generate(type: TypeElement, round: RoundEnvironment, env: ProcessingEnvironment): Pair<String, String>? = kotlin.run {
        val entity = EntityInfo(type, round, env)
        env.note("Fields:")
        entity.fields.forEach(env::note)
        env.messager.printMessage(Diagnostic.Kind.NOTE, "found element $type")
        entity.name to
                kotlinFile(packageName = entity.targetPkg) {
                    import(entity.type.qualifiedName.toString())
                    import(KIJvmMemEntity::class.qualifiedName!!)
                    classDeclaration(entity.name) {
                        primaryConstructor() {
                            param("store", KoType.Companion.parseType(JvmMemoryDataStore::class.qualifiedName!!))
                            param("id", KoType.Companion.parseType(entity.idType.qualifiedName.toString()))
                            entity.fields.forEach { param(it.name, it.koType) }
                        }
                        extends(
                                KoType.Companion.parseType("${KIJvmMemEntity::class.qualifiedName}<${entity.idType.qualifiedName}>"),
                                "store", "id")
                        implements(KoType.Companion.parseType("${entity.type.qualifiedName}"))
                        entity.fields.forEach {
                            val mod = if (it.readOnly) VAL else VAR
                            property(it.name, it.koType, OVERRIDE + mod) {
                                initializer(it.name)
                            }
                        }
                    }

                    type.enclosedElements.filterIsInstance<ExecutableType>().forEach {
                        it.getAnnotation(GeneratedId::class.java)?.let { ann ->
                            env.note("$it has $ann")
                        }
                        it.annotationMirrors.forEach { env.note("mirror $it") }
                        env.note("returns ${it.returnType}")
                    }
                }.accept(PrettyPrinter(PrettyPrinterConfiguration()))
    }
}