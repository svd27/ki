package info.kinterest.annotations.processor

import info.kinterest.annotations.Entity
import info.kinterest.annotations.StorageTypes
import info.kinterest.annotations.processor.generators.Generator
import info.kinterest.annotations.processor.generators.JvmMemoryGenerator
import info.kinterest.annotations.processor.generators.note
import org.slf4j.LoggerFactory
import javax.annotation.processing.*
import javax.lang.model.SourceVersion
import javax.lang.model.element.TypeElement
import javax.tools.Diagnostic
import java.io.File
import javax.lang.model.element.Element

@SupportedAnnotationTypes("info.kinterest.annotations.Entity")
@SupportedOptions(Processor.KAPT_KOTLIN_GENERATED_OPTION_NAME, "kapt.verbose", "targets")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
class Processor : AbstractProcessor() {
    companion object {
        val LOG = LoggerFactory.getLogger(Processor::class.java)

        init {
            LOG.info("!!! starting Processor !!!")
        }

        const val KAPT_KOTLIN_GENERATED_OPTION_NAME = "kapt.kotlin.generated"
        val generators = mutableMapOf<String, Generator>().apply {
            this[JvmMemoryGenerator.store] = JvmMemoryGenerator
        }
    }

    override fun process(annotations: MutableSet<out TypeElement>, roundEnv: RoundEnvironment): Boolean = kotlin.run {
        processingEnv.messager.printMessage(Diagnostic.Kind.NOTE, "options: ${processingEnv.options}")
        processingEnv.note("generators: $generators")
        val targets = processingEnv.options["targets"]!!
        val outDir = processingEnv.options[KAPT_KOTLIN_GENERATED_OPTION_NAME]!!
        LOG.info("got elements $annotations")
        targets.split(',').forEach { target ->
            processingEnv.messager.printMessage(Diagnostic.Kind.NOTE, "got elements $annotations")
            roundEnv.getElementsAnnotatedWith(Entity::class.java).map { it.toTypeElementOrNull() }.filterNotNull().forEach { source ->
                val st = source.getAnnotation(StorageTypes::class.java)
                st?.let {
                    processingEnv.note("types: ${it.stores.toList()}")
                    it.stores.forEach {
                        generators[it]?.let {
                            processingEnv.note("using generator $it")
                            it.generate(source, roundEnv, processingEnv)?.let { (fn, txt) ->
                                File(outDir, fn+".kt").writeText(txt)
                            }
                        }
                    }
                }
                source.annotationMirrors.map { it.annotationType.getAnnotation(StorageTypes::class.java) }.filterNotNull().forEach { ann ->
                    ann.stores.forEach {
                        processingEnv.note("ann: $ann tyoe: ${it.toList()}")
                    }
                }
            }
        }
        false
    }

    fun Element.toTypeElementOrNull(): TypeElement? {
        if (this !is TypeElement) {
            processingEnv.messager.printMessage(Diagnostic.Kind.ERROR, "Invalid element type, class expected", this)
            return null
        }

        return this
    }
}