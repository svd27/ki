package info.kinterest.annotations.processor.generators

import info.kinterest.*
import info.kinterest.annotations.Entity
import info.kinterest.datastores.jvm.memory.JvmMemoryDataStore
import info.kinterest.datastores.jvm.memory.KIJvmMemEntity
import info.kinterest.meta.*
import org.jetbrains.annotations.Nullable
import org.yanex.takenoko.*
import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.lang.model.element.*
import javax.lang.model.type.*
import javax.tools.Diagnostic
import kotlin.reflect.KClass

class EntityInfo(val type: TypeElement, env: ProcessingEnvironment) {
    val srcPkg = type.qualifiedName.split('.').dropLast(1).joinToString(separator = ".")
    val targetPkg = "$srcPkg.${JvmMemoryGenerator.store}"
    val root: TypeElement
    val parent: TypeElement?

    init {
        env.note("targetPkg: $targetPkg")
        fun findEntitySuper(t: TypeElement): TypeElement = t.interfaces.filterIsInstance<DeclaredType>().firstOrNull {
            it.getAnnotation(Entity::class.java) != null
        }?.let {
            findEntitySuper(it.asElement() as TypeElement)
        } ?: t
        root = findEntitySuper(type)
        parent = type.interfaces.filterIsInstance<DeclaredType>().firstOrNull { it.getAnnotation(Entity::class.java) != null }?.asElement() as? TypeElement
    }


    val name = type.simpleName.toString() + JvmMemoryGenerator.suffix
    val idGetter = type.enclosedElements.filter { el: Element? -> el is ExecutableElement }.first { it.simpleName.toString().equals("getId") }
    val idExecutableType: ExecutableType = idGetter.asType() as ExecutableType
    val idDecType = when (idExecutableType.returnType) {
        is DeclaredType -> idExecutableType.returnType as DeclaredType
        else -> throw IllegalStateException("wrong type ${idExecutableType.returnType}")
    }
    private val idType = when (idDecType.asElement()) {
        is TypeElement -> idDecType.asElement() as TypeElement
        else -> throw IllegalStateException("wrong type ${idDecType.asElement()}")
    }
    val idTypeStr = idType.qualifiedName.normalize()
    val idKoType: KoType = parseType(idType.qualifiedName.normalize())

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

    val versioned = type.getAnnotation(info.kinterest.annotations.Versioned::class.java) != null

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
        val typeName: String = type.toString().normalize() + if (nullable) "?" else ""
        val propMetaType = typeName.let {
            if (typeName.endsWith("?")) typeName.dropLast(1) else typeName
        }.let {
            when (it) {
                Boolean::class.qualifiedName!! -> KIBooleanProperty::class
                Byte::class.qualifiedName!! -> TODO()
                Char::class.qualifiedName!! -> TODO()
                Double::class.qualifiedName!! -> KIDoubleProperty::class
                Float::class.qualifiedName -> TODO()
                Int::class.qualifiedName -> KIIntProperty::class
                Long::class.qualifiedName -> KILongProperty::class
                Short::class.qualifiedName!! -> TODO()

                String::class.qualifiedName!! -> KIStringProperty::class

                else -> KISimpleTypeProperty::class
            }
        }

        val metaName = "PROP_${name.toUpperCase()}"


        val koType = when (type) {
            is Typing.Declared -> parseType(type.type.qualifiedName.normalize())
            is Typing.Primitive -> type.type.kind.koType
        }.let { if (nullable) it.nullable else it }

        override fun toString(): String =
                "FieldInfo(name='$name', nullable=$nullable, transient=$transient, type=$type, koType=$koType)"

    }
}

fun Name.normalize(): String = toString().normalize()

fun String.normalize(): String = if (startsWith("java.lang")) {
    when (this) {
        java.lang.Boolean::class.java.canonicalName -> Boolean::class.qualifiedName!!
        java.lang.Byte::class.java.canonicalName -> Byte::class.qualifiedName!!
        java.lang.Character::class.java.canonicalName -> Char::class.qualifiedName!!
        java.lang.Double::class.java.canonicalName -> Double::class.qualifiedName!!
        java.lang.Float::class.java.canonicalName -> Float::class.qualifiedName!!
        java.lang.Integer::class.java.canonicalName -> Int::class.qualifiedName!!
        java.lang.Long::class.java.canonicalName -> Long::class.qualifiedName!!
        java.lang.Short::class.java.canonicalName -> Short::class.qualifiedName!!

        java.lang.String::class.java.canonicalName -> String::class.qualifiedName!!

        else -> toString()
    }
} else {
    this
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
        val entity = EntityInfo(type, env)

        env.messager.printMessage(Diagnostic.Kind.NOTE, "found element $type")
        entity.name to
                kotlinFile(packageName = entity.targetPkg) {
                    import(parseType(Versioned::class.java))
                    classDeclaration(entity.name) {
                        primaryConstructor() {
                            property("store", parseType(JvmMemoryDataStore::class.qualifiedName!!), VAL)
                            param("id", entity.idKoType)
                        }
                        extends(
                                parseType("${KIJvmMemEntity::class.qualifiedName}<${entity.type.simpleName},${entity.idTypeStr}>"),
                                "store", "id")
                        implements(KoType.Companion.parseType("${entity.type.qualifiedName}"))
                        if (entity.versioned)
                            implements(parseType("Versioned<Long>"))
                        property("_meta", null, OVERRIDE + VAL) {
                            getter(KoModifierList.Empty, true) {
                                append("Meta")
                            }
                        }
                        property("_me", parseType("${KClass::class.qualifiedName}<*>"), OVERRIDE + VAL) {
                            getter(KoModifierList.Empty, true) {
                                append("_meta.me")
                            }
                        }

                        if (entity.versioned)
                            property<Long>("_version", VAL + OVERRIDE) {
                                getter(KoModifierList.Empty, true) {
                                    append("""
                                        store.version<${entity.type.simpleName},${entity.idTypeStr}>(id)!!
                                    """.trimIndent())
                                }
                            }

                        entity.fields.forEach {
                            val mod = if (it.readOnly) VAL else VAR
                            property(it.name, it.koType, OVERRIDE + mod) {
                                val typeStr = it.typeName.let { if (it.endsWith("?")) it.dropLast(1) else it }
                                getter(KoModifierList.Empty, true) {
                                    append("""store.getProp<${entity.type.simpleName},${entity.idTypeStr},$typeStr>(id, Meta.${it.metaName})""")
                                    if (!it.nullable) append("!!")
                                }
                                if (!it.readOnly) {
                                    setter(KoModifierList.Empty, true, "value") {
                                        val suffix = if (entity.versioned) ",_version" else ""
                                        append("""Unit.apply { store.setProp<${entity.type.simpleName},${entity.idTypeStr},$typeStr>(id, Meta.${it.metaName}, value$suffix) }""")
                                    }
                                }
                            }
                        }

                        function("asTransient", OVERRIDE) {
                            body(true) {
                                val fields = entity.fields.map(EntityInfo.FieldInfo::name).joinToString(",")
                                append("Transient(id,$fields)")
                            }
                        }

                        companionDeclaration("") {
                            implements(parseType("info.kinterest.jvm.KIJvmEntitySupport<${entity.type.simpleName},${entity.idTypeStr}>"))
                            property("meta", null, VAL + OVERRIDE) {
                                getter(KoModifierList.Empty, true) {
                                    append("Meta")
                                }
                            }
                            objectDeclaration("Meta") {
                                val ext = parseType("info.kinterest.jvm.KIJvmEntityMeta")
                                extends(ext, "${entity.name}::class", "${entity.type}::class")
                                parseType(entity.root.qualifiedName.toString())
                                property("root", null, OVERRIDE + VAL) {
                                    initializer("${entity.root}::class")
                                }
                                property("parent", parseType("${KClass::class.qualifiedName}<*>").nullable, OVERRIDE + VAL) {
                                    initializer(entity.parent?.let { "${entity.parent}::class" } ?: "null")
                                }
                                entity.fields.forEach {
                                    if (it.propMetaType == KISimpleTypeProperty::class)
                                        property(it.metaName, parseType("${it.propMetaType.qualifiedName}<${it.typeName}>"), VAL) {
                                            initializer("props[\"${it.name}\"] as ${it.propMetaType.qualifiedName}<${it.typeName}>")
                                        }
                                    else {
                                        property(it.metaName, parseType(it.propMetaType.java), VAL) {
                                            initializer("props[\"${it.name}\"] as ${it.propMetaType.qualifiedName}")
                                        }
                                    }
                                }
                            }

                            classDeclaration("Transient") {
                                implements(parseType(entity.type.qualifiedName.toString()))
                                implements("${TransientEntity::class.qualifiedName}<${entity.idTypeStr}>")
                                primaryConstructor {
                                    property("_id", parseType(entity.idTypeStr).nullable, VAL + PRIVATE)
                                    property("values", parseType("Map<String,Any?>"), OVERRIDE + VAL)
                                }
                                secondaryConstructor {
                                    param("id", parseType(entity.idTypeStr).nullable)
                                    entity.fields.forEach {
                                        param(it.name, it.koType)
                                    }
                                    val map = entity.fields.map { "\"${it.name}\" to ${it.name}" }.joinToString(",", "mapOf(", ")")
                                    val arg = arrayOf("id", map)
                                    delegateCall("this", *arg)
                                }
                                property("id", parseType(entity.idTypeStr), VAL + OVERRIDE) {
                                    getter(KoModifierList.Empty, true) {
                                        append("if(_id!=null) _id else TODO()")
                                    }
                                }

                                property("_store", parseType("${DataStore::class.qualifiedName}"), VAL + OVERRIDE) {
                                    getter(KoModifierList.Empty, true) {
                                        append("TODO()")
                                    }
                                }

                                property("_meta", parseType("${KIEntityMeta::class.qualifiedName}"), VAL + OVERRIDE) {
                                    getter(KoModifierList.Empty, true) {
                                        append("TODO()")
                                    }
                                }

                                entity.fields.forEach {
                                    if (it.readOnly) {
                                        property(it.name, it.koType, OVERRIDE + VAL) {
                                            delegate("values")
                                        }
                                    } else {
                                        property(it.name, it.koType, OVERRIDE + VAR) {
                                            initializer("values[\"${it.name}\"] as ${it.typeName}")
                                            setter("v", "TODO()")
                                        }
                                    }

                                }

                                function("asTransient", OVERRIDE) {
                                    body(true) { append("this") }
                                }

                            }

                            function("transient", OVERRIDE) {
                                param("id", parseType(entity.idTypeStr).nullable)
                                param("values", parseType("Map<String,Any?>"))
                                body(true) {
                                    append("Transient(id, values)")
                                }
                            }

                            function("create", OVERRIDE) {
                                typeParam("DS", parseType("${DataStore::class.qualifiedName}"))
                                param("ds", parseType("DS"))
                                param("id", parseType(entity.idTypeStr))
                                param("values", parseType("Map<String,Any?>"))
                                body(true) {
                                    append("(ds as JvmMemoryDataStore).create(Meta.me, id, values) as Unit")
                                }
                            }
                        }
                    }
                }.accept(PrettyPrinter(PrettyPrinterConfiguration()))
    }
}