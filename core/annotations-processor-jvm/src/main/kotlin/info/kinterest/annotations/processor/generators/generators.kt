package info.kinterest.annotations.processor.generators

import info.kinterest.DONTDOTHIS
import info.kinterest.DataStore
import info.kinterest.EntitySupport
import info.kinterest.KIVersionedEntity
import info.kinterest.annotations.Entity
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.meta.*
import org.jetbrains.annotations.Nullable
import org.yanex.takenoko.*
import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.lang.model.element.ExecutableElement
import javax.lang.model.element.Modifier
import javax.lang.model.element.Name
import javax.lang.model.element.TypeElement
import javax.lang.model.type.DeclaredType
import javax.lang.model.type.ExecutableType
import javax.lang.model.type.PrimitiveType
import javax.lang.model.type.TypeMirror
import javax.tools.Diagnostic
import kotlin.reflect.KClass

class EntityInfo(val type: TypeElement, val env: ProcessingEnvironment) {
    val srcPkg = type.qualifiedName.split('.').dropLast(1).joinToString(separator = ".")
    val targetPkg = "$srcPkg.${JvmGenerator.type}"
    val root: TypeElement
    val parent: TypeElement?
    val hierarchy: List<TypeElement>

    init {
        env.note("targetPkg: $targetPkg")
        fun findSuperEntity(t: TypeElement): TypeElement? = t.interfaces.filterIsInstance<DeclaredType>().map { it.asElement() }.filter { env.note("$it ${it.annotationMirrors.joinToString("\n", "\n")}");it.getAnnotation(Entity::class.java) != null }.filterIsInstance<TypeElement>().firstOrNull()
        parent = findSuperEntity(type)
        var par = listOf(findSuperEntity(type))
        while (par.lastOrNull() != null) {
            par += findSuperEntity(par.last()!!)
        }
        env.note("super: $par")
        hierarchy = par.filterNotNull()
        fun findEntitySuper(t: TypeElement): TypeElement = t.interfaces.filterIsInstance<DeclaredType>().firstOrNull {
            it.getAnnotation(Entity::class.java) != null
        }?.let {
            findEntitySuper(it.asElement() as TypeElement)
        } ?: t
        env.note("interfaces: ${type.interfaces.map { "$it: ${it::class.java.interfaces.toList()}" }}")

        root = hierarchy.lastOrNull() ?: type
        env.note("$type has parent $parent")
    }


    private fun TypeElement.findGetter(name: String): ExecutableElement? {
        env.note("find $name in $this")
        val direct = findGetterDirect(name)
        return if (direct == null)
            (this.interfaces).map { env.note("super $it ${it::class}"); it }.filterIsInstance<DeclaredType>().map { te -> te.asElement() }.filterIsInstance<TypeElement>().map {
                this@EntityInfo.env.note("checking $it ${it.typeParameters.map { it.genericElement }}")
                it.findGetter(name)
            }.firstOrNull()
        else direct
    }

    private fun TypeElement.findGetterDirect(name: String): ExecutableElement? = enclosedElements.filterIsInstance<ExecutableElement>().firstOrNull { it.simpleName.toString() == "get${name.capitalize()}" }
    val name = type.simpleName.toString() + JvmGenerator.suffix
    private val idGetter = type.findGetter("id")!!
    private val idExecutableType: ExecutableType = idGetter.asType() as ExecutableType
    private val idDecType = when (idExecutableType.returnType) {
        is DeclaredType -> idExecutableType.returnType as DeclaredType
        else -> throw IllegalStateException("wrong type ${idExecutableType.returnType}")
    }
    private val idType = when (idDecType.asElement()) {
        is TypeElement -> idDecType.asElement() as TypeElement
        else -> throw IllegalStateException("wrong type ${idDecType.asElement()}")
    }
    val idTypeStr = idType.qualifiedName.normalize()
    val idKoType: KoType = parseType(idType.qualifiedName.normalize())

    val fields: List<FieldInfo> = (hierarchy + type).flatMap {
        it.enclosedElements.filterIsInstance<ExecutableElement>().filter {
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
    }

    val versioned = type.interfaces.any { it -> it is DeclaredType && it.asElement().simpleName.toString() == KIVersionedEntity::class.simpleName }

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
        java.lang.Object::class.java.canonicalName -> Any::class.qualifiedName!!

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
        override fun toString(): String = type.normalise()
    }

    fun PrimitiveType.normalise() = when (toString()) {
        "boolean" -> "kotlin.Boolean"
        "byte" -> "kotlin.Byte"
        "char" -> "kotlin.Char"
        "short" -> "kotlin.Short"
        "int" -> "kotlin.Int"
        "long" -> "kotlin.Long"
        "float" -> "kotlin.FLoar"
        "double" -> "kotlin.Double"
        else -> DONTDOTHIS("unknown type $this")
    }
}

object JvmGenerator : Generator {
    override val type: String
        get() = "jvm"
    val suffix = type.split('.').map { it.capitalize() }.joinToString("")


    override fun generate(type: TypeElement, round: RoundEnvironment, env: ProcessingEnvironment): Pair<String, String>? = kotlin.run {
        val entity = EntityInfo(type, env)

        env.messager.printMessage(Diagnostic.Kind.NOTE, "found element $type")
        entity.name to
                kotlinFile(packageName = entity.targetPkg) {
                    import(parseType(KIProperty::class.java))

                    classDeclaration(entity.name) {
                        primaryConstructor() {
                            param("_store", parseType(DataStoreFacade::class.qualifiedName!!))
                            param("id", entity.idKoType)
                        }
                        extends(
                                parseType("${KIJvmEntity::class.qualifiedName}<${entity.type.simpleName},${entity.idTypeStr}>"),
                                "_store", "id")
                        implements(parseType("${entity.type.qualifiedName}"))

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
                            property<Any>("_version", VAL + OVERRIDE) {
                                getter(KoModifierList.Empty, true) {
                                    append("""
                                        _store.version(_meta, id)
                                    """.trimIndent())
                                }
                            }

                        entity.fields.forEach {
                            val mod = if (it.readOnly) VAL else VAR
                            property(it.name, it.koType, OVERRIDE + mod) {
                                getter(KoModifierList.Empty, true) {
                                    append("""getValue(Meta.${it.metaName})""")
                                    if (!it.nullable) append("!!")
                                }
                                if (!it.readOnly) {
                                    setter(KoModifierList.Empty, true, "value") {
                                        if (entity.versioned) {
                                            append("setValue(Meta.${it.metaName}, _version, value)")
                                        } else {
                                            append("setValue(Meta.${it.metaName}, value)")
                                        }
                                    }
                                }
                            }
                        }

                        function("asTransient", OVERRIDE) {
                            body(true) {
                                append("Transient(this)")
                            }
                        }

                        companionDeclaration("") {
                            implements(parseType(EntitySupport::class.java))
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
                                property("versioned", parseType(Boolean::class.java), OVERRIDE + VAL) {
                                    initializer(entity.versioned)
                                }
                                entity.fields.forEach {
                                    env.note("${it.name} ${it.propMetaType} ${it.typeName} ${it.type}")
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
                                property("hierarchy", parseType("List<KIEntityMeta>"), OVERRIDE + VAL) {
                                    val hier = entity.hierarchy.joinToString(",", "listOf(", ")") { "${it.simpleName}Jvm.meta" }
                                    initializer(hier)
                                }
                            }

                            classDeclaration("Transient") {
                                implements(parseType(entity.type.qualifiedName.toString()))

                                primaryConstructor {
                                    property("_store", parseType(DataStore::class.java), VAL + OVERRIDE)
                                    property("_id", parseType(entity.idTypeStr).nullable, VAL + PRIVATE)
                                    if (entity.versioned)
                                        param("version", parseType(Any::class.java).nullable, VAL + PRIVATE)
                                    entity.fields.forEach {
                                        property(it.name, it.koType, OVERRIDE + if (it.readOnly) VAL else VAR)
                                    }
                                }
                                if (entity.versioned)
                                    secondaryConstructor {
                                        param("_store", parseType(DataStore::class.java))
                                        param("_id", parseType(entity.idTypeStr).nullable)
                                        entity.fields.forEach {
                                            param(it.name, it.koType)
                                        }
                                        val args = listOf("_store", "_id", "null") +
                                                entity.fields.map { it.name }
                                        delegateCall("this", *args.toTypedArray())
                                    }
                                secondaryConstructor {
                                    param("e", parseType(entity.type.qualifiedName.toString()))
                                    val args = listOf("e._store", "e.id") +
                                            (if (entity.versioned) listOf("e._version") else listOf()) +
                                            entity.fields.map { "e.${it.name}" }
                                    delegateCall("this", *args.toTypedArray())
                                }
                                property("id", parseType(entity.idTypeStr), VAL + OVERRIDE) {
                                    getter(KoModifierList.Empty, true) {
                                        append("if(_id!=null) _id else TODO()")
                                    }
                                }


                                property("_meta", parseType("${KIEntityMeta::class.qualifiedName}"), VAL + OVERRIDE) {
                                    getter(KoModifierList.Empty, true) {
                                        append("Meta")
                                    }
                                }

                                if (entity.versioned) {
                                    property("_version", parseType(Any::class.java), VAL + OVERRIDE) {
                                        getter(KoModifierList.Empty, true) {
                                            append("if(version==null) TODO() else version")
                                        }
                                    }
                                }


                                function("asTransient", OVERRIDE) {
                                    body(true) { append("Transient(this)") }
                                }

                                function("getValue", OVERRIDE) {
                                    typeParam("V")
                                    typeParam("P:KIProperty<V>")
                                    param("p", "P")
                                    returnType("V?")
                                    body(true) {
                                        append("""when(p) {
                                              Meta.idProperty -> id
                                              ${entity.fields.map { "Meta.${it.metaName} -> ${it.name}" }.joinToString("\n")}
                                              else -> TODO(p.name)
                                            } as V?
                                        """.trimIndent())
                                    }
                                }

                                function("setValue", OVERRIDE) {
                                    typeParam("V")
                                    typeParam("P:KIProperty<V>")
                                    param("p", "P")
                                    param("v", "V?")
                                    body(true) {
                                        append("""when(p) {
                                              ${entity.fields.filter { !it.readOnly }.map {
                                            "Meta.${it.metaName} -> ${it.name} = v as ${(renderType(parseType(it.typeName)))}"
                                        }.joinToString("\n")}
                                              else -> TODO()
                                            }
                                        """.trimIndent())
                                    }
                                }

                                function("setValue", OVERRIDE) {
                                    typeParam("V")
                                    typeParam("P:KIProperty<V>")
                                    param("p", "P")
                                    param("version", "Any")
                                    param("v", "V?")
                                    body(true) {
                                        append("""when(p) {
                                              ${entity.fields.filter { !it.readOnly }.map { "Meta.${it.metaName} -> ${it.name} = v as ${renderType(parseType(it.typeName))}" }.joinToString("\n")}
                                              else -> TODO()
                                            }
                                        """.trimIndent())
                                    }
                                }
                            }
                        }
                    }
                }.accept(PrettyPrinter(PrettyPrinterConfiguration()))
    }
}

