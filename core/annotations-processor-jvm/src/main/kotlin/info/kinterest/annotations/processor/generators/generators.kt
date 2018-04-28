@file:Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")

package info.kinterest.annotations.processor.generators

import info.kinterest.*
import info.kinterest.datastores.DataStoreFacade
import info.kinterest.functional.Try
import info.kinterest.functional.getOrElse
import info.kinterest.jvm.KIJvmEntity
import info.kinterest.jvm.annotations.Entity
import info.kinterest.jvm.annotations.Relation
import info.kinterest.jvm.annotations.TypeArgs
import info.kinterest.meta.*
import org.jetbrains.annotations.Nullable
import org.yanex.takenoko.*
import java.util.Set
import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.lang.model.element.*
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
    val abstract : Boolean

    val rels: List<Pair<TypeElement, ExecutableElement>>

    init {
        env.note("targetPkg: $targetPkg")
        abstract = type.typeParameters.size>0
        //type.getAnnotation(Relations::class.java)?.let{ env.note("rel: $it") }
        fun findSuperEntity(t: TypeElement): TypeElement? = t.interfaces.filterIsInstance<DeclaredType>().map { it.asElement() }.filter { it.getAnnotation(Entity::class.java) != null }.filterIsInstance<TypeElement>().firstOrNull()
        parent = findSuperEntity(type)
        var par = listOf(findSuperEntity(type))
        while (par.lastOrNull() != null) {
            par += findSuperEntity(par.last()!!)
        }
        hierarchy = par.filterNotNull()
        rels = type.enclosedElements.filterIsInstance<ExecutableElement>().filter { env.note("rel check: $it ${it.annotationMirrors}"); it.getAnnotation(Relation::class.java) != null }.map { type to it } + hierarchy.flatMap { type -> type.enclosedElements.filterIsInstance<ExecutableElement>().filter { it.getAnnotation(Relation::class.java) != null }.map { type to it } }
        env.note("RELS: $rels")

        root = hierarchy.lastOrNull() ?: type
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

    val fields: List<FieldInfo> = (hierarchy + type).flatMap { t ->
        t.enclosedElements.filterIsInstance<ExecutableElement>().filter {
            it.simpleName.toString() !in rels.map { it.second.simpleName.toString() }
        }.filter {
            it.simpleName.toString() != "getId" && Modifier.STATIC !in it.modifiers && it.simpleName.startsWith("get")
                    && it.simpleName.toString().length > 3
        }.map {
            val nm = it.simpleName.toString().substring(3).decapitalize()
            val setterNm = "set${nm.capitalize()}"
            val setter = t.enclosedElements.filterIsInstance<ExecutableElement>().filter { it.simpleName.toString() == setterNm }
            FieldInfo(
                    name= nm,
                    _type = it.returnType,
                    readOnly = setter.isEmpty(),
                    nullable = it.getAnnotation(Nullable::class.java) != null,
                    transient = it.getAnnotation(info.kinterest.jvm.annotations.Transient::class.java)!=null,
                    getter = it
            )
        }
    }

    val relations: List<RelationInfo> = rels.map { val rel = RelationInfo(it.second, it.first); env.note("rel: ${it.second} ${it.second.simpleName} prop: ${rel.property}"); rel }

    val versioned = (hierarchy+type).flatMap { it.interfaces }.any { it -> it is DeclaredType && it.asElement().simpleName.toString() == KIVersionedEntity::class.simpleName }

    class IdInfo(val type: TypeElement, val generateKey: Boolean)

    inner class RelationInfo(val element: ExecutableElement, type1: TypeElement) {
        val multi: Boolean
        val typeName: String
        val readOnly: Boolean = type1.enclosedElements.firstOrNull { it.simpleName.toString() == element.simpleName.toString().replaceFirst("get", "set") } == null
        val nullable: Boolean = element.getAnnotation(Nullable::class.java) != null
        val property: String = element.simpleName.toString().substring(3).decapitalize()
        val targetVal: AnnotationValue
        val targetIdVal: AnnotationValue
        val target: String
        val targetId: String
        val mutableCollection: Boolean
        val metaName: String
        val metaType: String

        init {
            var replaceAfter = element.returnType.toString().replaceAfter('<', "")
            if (replaceAfter != element.returnType.toString()) replaceAfter = replaceAfter.dropLast(1)
            multi = Try { Class.forName(replaceAfter) == Set::class.java }.getOrElse { false }

            val annotation = element.annotationMirrors.first { it.annotationType.toString() == Relation::class.qualifiedName }

            targetVal = annotation!!.elementValues.entries.first { it.key.simpleName.toString() == "target" }.value
            targetIdVal = annotation.elementValues.entries.first { it.key.simpleName.toString() == "targetId" }.value
            env.note("${annotation.elementValues}")
            target = targetVal.value.toString()
            targetId = targetIdVal.value.toString().normalize()
            mutableCollection = (annotation.elementValues.entries.firstOrNull { it.key.simpleName.toString() == "mutableCollection" }?.value?.value
                    ?: true) as Boolean
            env.note("$target $targetId $mutableCollection")
            metaName = "PROP_${property.toUpperCase()}"
            metaType = "${KIRelationProperty::class.qualifiedName}"
            val type = if (multi) {
                element.returnType.toString().replace("java.util.Set", if (mutableCollection) "kotlin.MutableSet" else "kotlin.Set")
            } else element.returnType.toString()
            typeName = type + if (nullable) "?" else ""
        }
    }

    inner class FieldInfo(val name: String, _type: TypeMirror, val readOnly: Boolean, val nullable: Boolean, val transient: Boolean = false, val getter:ExecutableElement) {
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
            is Typing.Declared -> parseType(type.type.qualifiedName.normalize() +
                    if(type.type.typeParameters.size>0) {
                        val typeArgs = getter.annotationMirrors.find { mir -> val annType = mir.annotationType;
                            annType.toString() == TypeArgs::class.qualifiedName
                        }
                        if(typeArgs==null) throw IllegalStateException("need to annotate generic types with TypeArgs")
                        else {
                            env.note("mirror: $typeArgs, ${typeArgs.elementValues}")
                            val annotationValue: AnnotationValue = typeArgs.elementValues.filter { it.key.simpleName.toString()==TypeArgs::args.name }.map { it.value }.first()
                            val args = annotationValue.value as List<*>

                            env.note("args: $annotationValue ${annotationValue::class} ${annotationValue.value} ${annotationValue.value::class}")
                            type.type.typeParameters.mapIndexed {
                                idx, par -> args[idx].toString().removeSuffix(".class").normalize()
                            }.joinToString(",", "<", ">")
                        }
                    } else "")
            is Typing.Primitive -> type.type.kind.koType
        }.let { if (nullable) it.nullable else it }

        init {
            env.note("koType: ${renderType(koType)}")
        }

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

        else -> this
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
                    import(parseType(KIEntityMeta::class.java))
                    import(parseType(RelationDelegate::class.java))
                    import(parseType(RelationNullableDelegate::class.java))
                    import(parseType(RelationRODelegate::class.java))
                    import(parseType(RelationNullableRODelegate::class.java))
                    import(parseType(RelationMutableSet::class.java))
                    import(parseType(RelationSet::class.java))

                    env.note("import: ${Try::class.qualifiedName!!.replaceAfterLast('.', "*")}")
                    extraImport(Try::class.qualifiedName!!.replaceAfterLast('.', "getOrElse"))
                    //import(Try::class.qualifiedName!!.replaceAfterLast('.', "*"))

                    val typeSuffix = if(entity.type.typeParameters.size==0) "" else "<${entity.type.typeParameters.map { it.simpleName }.joinToString(",")}>"
                    classDeclaration("${entity.name}", if(entity.abstract) ABSTRACT else KoModifierList.Empty) {
                        if(entity.type.typeParameters.size>0) {
                            entity.type.typeParameters.forEach {
                                typeParam(it.simpleName.toString())
                            }
                        }
                        primaryConstructor() {
                            param("_store", parseType(DataStoreFacade::class.qualifiedName!!))
                            property("id", entity.idKoType, VAL+ OVERRIDE)
                        }

                        extends(
                                parseType("${KIJvmEntity::class.qualifiedName}<${entity.type.simpleName}$typeSuffix,${entity.idTypeStr}>"),
                                "_store", "id")
                        implements(parseType("${entity.type.qualifiedName}$typeSuffix"))

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
                                if(!it.transient) {
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
                        }



                        entity.relations.forEach {
                            val mod = if (it.readOnly) VAL else VAR
                            property(it.property, parseType(it.typeName), OVERRIDE + mod) {
                                if (!it.multi) {
                                    if (it.readOnly) {
                                        if (it.nullable) {
                                            delegate("RelationNullableRODelegate(_store, Meta.${it.metaName})")
                                        } else {
                                            delegate("RelationRODelegate(_store, Meta.${it.metaName})")
                                        }
                                    } else {
                                        if (it.nullable)
                                            delegate("RelationNullableDelegate(_store, Meta.${it.metaName})")
                                        else
                                            delegate("RelationDelegate(_store, Meta.${it.metaName})")
                                    }
                                } else {
                                    require(!it.nullable)
                                    if (!it.mutableCollection) {
                                        initializer("RelationSet(this, _store, Meta.${it.metaName})")
                                    } else {
                                        initializer("RelationMutableSet(this, _store, Meta.${it.metaName})")
                                    }
                                }
                            }
                        }

                        if(!entity.abstract) function("asTransient", OVERRIDE) {
                            body(true) {
                                append("Transient(this)")
                            }
                        }

                        if(!entity.abstract) classDeclaration("Transient") {
                            implements(parseType(entity.type.qualifiedName.toString()))

                            primaryConstructor {
                                property("_store", parseType(DataStore::class.java), VAL + OVERRIDE)
                                property("_id", parseType(entity.idTypeStr).nullable, VAL + PRIVATE)
                                if (entity.versioned)
                                    param("version", parseType(Any::class.java).nullable, VAL + PRIVATE)
                                entity.fields.forEach {
                                    property(it.name, it.koType, OVERRIDE + if (it.readOnly) VAL else VAR)
                                }
                                entity.relations.forEach {
                                    property(it.property, parseType(it.typeName), OVERRIDE + if (it.readOnly) VAL else VAR)
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
                                            entity.fields.map { it.name } + entity.relations.map { "e." + if (it.multi) "${it.element.simpleName}.toMutableSet()" else it.element.simpleName }
                                    delegateCall("this", *args.toTypedArray())
                                }
                            secondaryConstructor {
                                param("e", parseType(entity.type.qualifiedName.toString()))
                                val args = listOf("e._store", "e.id") +
                                        (if (entity.versioned) listOf("e._version") else listOf()) +
                                        entity.fields.map { "e.${it.name}" } + entity.relations.map { "e." + if (it.multi) "${it.property}.toMutableSet()" else it.property }
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
                                              ${entity.relations.map { "Meta.${it.metaName} -> ${it.property}" }.joinToString("\n")}
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
                                        ${entity.relations.filter { !it.readOnly }.map {
                                        "Meta.${it.metaName} -> ${it.property} = v as ${(renderType(parseType(it.typeName)))}"
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
                                              ${entity.relations.filter { !it.readOnly }.map {
                                        "Meta.${it.metaName} -> ${it.property} = v as ${(renderType(parseType(it.typeName)))}"
                                    }.joinToString("\n")}
                                              else -> TODO()
                                            }
                                        """.trimIndent())
                                }
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
                                if (it.propMetaType == KISimpleTypeProperty::class) {
                                    val koType = parseType("${it.propMetaType.qualifiedName}<${renderType(it.koType)}>")
                                    property(it.metaName, koType, VAL) {
                                        initializer("props[\"${it.name}\"] as ${renderType(koType)}")
                                    }
                                } else {
                                    property(it.metaName, parseType(it.propMetaType.java), VAL) {
                                        initializer("props[\"${it.name}\"] as ${it.propMetaType.qualifiedName}")
                                    }
                                }
                            }
                            entity.relations.forEach {
                                property(it.metaName, parseType(it.metaType), VAL) {
                                    initializer("props[\"${it.property}\"] as ${it.metaType}")
                                }
                            }
                            property("hierarchy", parseType("List<KIEntityMeta>"), OVERRIDE + VAL) {
                                val hier = entity.hierarchy.joinToString(",", "listOf(", ")") { "${it.simpleName}Jvm.meta" }
                                initializer(hier)
                            }
                        }


                        companionDeclaration("") {
                            implements(parseType(EntitySupport::class.java))
                            property("meta", null, VAL + OVERRIDE) {
                                getter(KoModifierList.Empty, true) {
                                    append("Meta")
                                }
                            }

                        }
                    }
                }.accept(PrettyPrinter(PrettyPrinterConfiguration()))
    }
}

