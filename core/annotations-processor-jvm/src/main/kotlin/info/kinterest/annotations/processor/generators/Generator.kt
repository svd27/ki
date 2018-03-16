package info.kinterest.annotations.processor.generators

import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.lang.model.element.TypeElement
import javax.tools.Diagnostic

interface Generator {
    val store : String
    fun generate(type:TypeElement, round:RoundEnvironment, env:ProcessingEnvironment) : Pair<String,String>?
}

fun ProcessingEnvironment.note(msg:String) = messager.printMessage(Diagnostic.Kind.NOTE, msg)