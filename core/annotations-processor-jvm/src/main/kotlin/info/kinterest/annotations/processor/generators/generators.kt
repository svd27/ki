package info.kinterest.annotations.processor.generators

import info.kinterest.GeneratedId
import info.kinterest.annotations.processor.Processor
import org.yanex.takenoko.PrettyPrinter
import org.yanex.takenoko.PrettyPrinterConfiguration
import org.yanex.takenoko.kotlinFile
import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.lang.model.element.Element
import javax.lang.model.element.ExecutableElement
import javax.lang.model.element.TypeElement
import javax.lang.model.type.ExecutableType
import javax.lang.model.type.TypeMirror
import javax.tools.Diagnostic

class EntityInfo(type: TypeElement, round: RoundEnvironment, env: ProcessingEnvironment) {
    val srcPkg = type.qualifiedName.split('.').dropLast(1).joinToString(separator = ".")
    val name = type.simpleName.toString()
    val idGetter = type.enclosedElements.filter { el: Element? -> el is ExecutableElement  }.first { it.simpleName.equals("getId") }
    val type1 = idGetter.asType() as ExecutableType


    class IdInfo(val type: TypeElement,val generateKey:Boolean)
    class FieldInfo(val name:String, val nullable:Boolean, val transient:Boolean)
}

object JvmMemoryGenerator : Generator {
    override val store: String
        get() = "jvm.mem"
    val suffix = store.split('.').map { it.capitalize() }.joinToString("")

    override fun generate(type: TypeElement, round: RoundEnvironment, env: ProcessingEnvironment): Pair<String, String>? = kotlin.run {
        val srcpkg = type.qualifiedName.split('.').dropLast(1).joinToString(separator = ".")
        env.messager.printMessage(Diagnostic.Kind.NOTE, "found element $type")
        val name = type.simpleName.toString() + suffix
        name to
        kotlinFile(packageName = srcpkg + store) {
            classDeclaration(name)

            type.enclosedElements.filterIsInstance<ExecutableType>().forEach {
                it.getAnnotation(GeneratedId::class.java)?.let {
                    ann -> env.note("$it has $ann")
                }
                it.annotationMirrors.forEach { env.note("mirror $it") }
                env.note("returns ${it.returnType}")
            }
        }.accept(PrettyPrinter(PrettyPrinterConfiguration()))
    }
}