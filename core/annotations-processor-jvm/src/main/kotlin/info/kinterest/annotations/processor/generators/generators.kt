package info.kinterest.annotations.processor.generators

import info.kinterest.*
import info.kinterest.annotations.processor.Processor
import info.kinterest.datastores.jvm.memory.JvmMemoryDataStore
import info.kinterest.datastores.jvm.memory.KIJvmMemEntity
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.KIJvmEntitySupport
import org.jetbrains.annotations.Nullable
import org.yanex.takenoko.*
import java.lang.reflect.Type
import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.lang.model.element.*
import javax.lang.model.type.*
import javax.tools.Diagnostic
import kotlin.reflect.KClass

class EntityInfo(val type: TypeElement, round: RoundEnvironment, env: ProcessingEnvironment) {
    val srcPkg = type.qualifiedName.split('.').dropLast(1).joinToString(separator = ".")
    val targetPkg = "$srcPkg.${JvmMemoryGenerator.store}"
    val root : TypeElement
    val parent : TypeElement?

    init {
        env.note("targetPkg: $targetPkg")
        fun findEntitySuper(t:TypeElement) : TypeElement = t.interfaces.filterIsInstance<DeclaredType>().firstOrNull{
            it.getAnnotation(Entity::class.java) != null
        }?.let {
            findEntitySuper(it.asElement() as TypeElement)
        }?:t
        root = findEntitySuper(type)
        parent = type.interfaces.filterIsInstance<DeclaredType>().firstOrNull {it.getAnnotation(Entity::class.java) != null}?.asElement() as? TypeElement
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
        val typeName : String = type.toString().normalize() + if(nullable) "?" else ""

        fun Name.normalize(): String = toString().normalize()

        fun String.normalize() : String = if (startsWith("java.lang")) {
            when (this) {
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
    class Declared(val type: TypeElement) : Typing() {
        override fun toString(): String = type.qualifiedName.toString()
    }
    class Primitive(val type: PrimitiveType) : Typing() {
        override fun toString(): String = type.toString()
    }
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
                        property("_meta", null, OVERRIDE+ VAL) {
                            getter(KoModifierList.Empty, true) {
                                append("Meta")
                            }
                        }
                        property("_me", parseType("${KClass::class.qualifiedName}<*>"), OVERRIDE+ VAL) {
                            getter(KoModifierList.Empty, true) {
                                append("_meta.me")
                            }
                        }
                        entity.fields.forEach {
                            val mod = if (it.readOnly) VAL else VAR
                            property(it.name, it.koType, OVERRIDE + mod) {
                                initializer(it.name)
                            }
                        }

                        function("asTransient", OVERRIDE) {
                            body(true) {
                                val fields = entity.fields.map(EntityInfo.FieldInfo::name).joinToString(",")
                                append("Transient(id,$fields)")
                            }
                        }

                        companionDeclaration("") {
                            implements(parseType("info.kinterest.jvm.KIJvmEntitySupport<${entity.type.qualifiedName},${entity.idType.qualifiedName}>"))
                            property("meta", null, VAL+ OVERRIDE) {
                                getter(KoModifierList.Empty, true) {
                                    append("Meta")
                                }
                            }
                            objectDeclaration("Meta") {
                                parseType("info.kinterest.jvm.KIJvmEntityMeta<*>")
                                extends("info.kinterest.jvm.KIJvmEntityMeta<info.kinterest.KIEntity<*>>", "${entity.name}::class")
                                parseType(entity.root.qualifiedName.toString())
                                property("root",null, OVERRIDE+VAL) {
                                    initializer("${entity.root}::class")
                                }
                                property("me", null, OVERRIDE+VAL) {
                                    initializer("${entity.type}::class")
                                }
                                property("parent", parseType("${KClass::class.qualifiedName}<*>").nullable, OVERRIDE+VAL) {
                                    initializer(entity.parent?.let {"${entity.parent}::class"}?:"null")
                                }

                            }

                            classDeclaration("Transient") {
                                implements(parseType(entity.type.qualifiedName.toString()))
                                implements("${TransientEntity::class.qualifiedName}<${entity.idType.qualifiedName}>")
                                primaryConstructor {
                                    property("_id", parseType("${entity.idType.qualifiedName}").nullable, VAL+ PRIVATE)
                                    property("values", parseType("Map<String,Any?>"), PRIVATE+VAL)
                                }
                                secondaryConstructor {
                                    param("id", parseType("${entity.idType.qualifiedName}").nullable)
                                    entity.fields.forEach {
                                        param(it.name, it.koType)
                                    }
                                    val map = entity.fields.map { "\"${it.name}\" to ${it.name}" }.joinToString(",", "mapOf(", ")")
                                    val arg = arrayOf("id", map)
                                    delegateCall("this", *arg)
                                }
                                property("id", parseType("${entity.idType.qualifiedName}"), VAL+ OVERRIDE) {
                                    getter(KoModifierList.Empty, true) {
                                        append("if(_id!=null) _id else TODO()")
                                    }
                                }

                                property("_store", parseType("${DataStore::class.qualifiedName}"), VAL+ OVERRIDE) {
                                    getter(KoModifierList.Empty, true) {
                                        append("TODO()")
                                    }
                                }

                                entity.fields.forEach {
                                    if(it.readOnly) {
                                        property(it.name, it.koType, OVERRIDE+VAL) {
                                            delegate("values")
                                        }
                                    } else {
                                        property(it.name, it.koType, OVERRIDE+VAR) {
                                            initializer("values[\"${it.name}\"] as ${it.typeName}")
                                            setter("v", "TODO()")
                                        }
                                    }

                                }

                                function("asTransient", OVERRIDE) {
                                    body(true) {append("this")}
                                }

                            }

                            function("transient", OVERRIDE) {
                                param("id", parseType("${entity.idType.qualifiedName}").nullable)
                                param("values", parseType("Map<String,Any?>"))
                                body(true) {
                                    append("Transient(id, values)")
                                }
                            }

                            function("create", OVERRIDE) {
                                typeParam("DS", parseType("${DataStore::class.qualifiedName}"))
                                param("ds", parseType("DS"))
                                param("id", parseType("${entity.idType.qualifiedName}"))
                                param("values", parseType("Map<String,Any?>"))
                                returnType(parseType("${entity.type.qualifiedName}"))
                                body(true) {
                                    val fields = entity.fields.map { "values[\"${it.name}\"] as ${it.typeName}"}.joinToString(", ")
                                    append("${entity.name}(ds as JvmMemoryDataStore, id, $fields)")
                                }
                            }
                        }
                    }
                }.accept(PrettyPrinter(PrettyPrinterConfiguration()))
    }
}