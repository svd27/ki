package info.kinterest.core.jvm.filters.parser

import info.kinterest.KIEntity
import info.kinterest.cast
import info.kinterest.jvm.FilterError
import info.kinterest.jvm.KIJvmEntityMeta
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.filter.*
import info.kinterest.meta.*
import norswap.autumn.Grammar
import norswap.autumn.UncaughtException
import norswap.autumn.UnexpectedChar
import norswap.autumn.parsers.*
import norswap.utils.cast
import java.math.BigDecimal

sealed class FNode
data class FilterNode(val entity: NameNode, val root:FNode) : FNode()
sealed class Constituents : FNode()
sealed class ValueNode<T:Comparable<T>>(val value:T) : FNode()
data class NumberNode(val n: Number) : ValueNode<BigDecimal>(when(n) {
    is Long, is Int -> BigDecimal.valueOf(n.toLong())
    is Double, is Float -> BigDecimal.valueOf(n.toDouble())
    else -> throw FilterError("unsupported number $n")
})
data class StringNode(val str:String) : ValueNode<String>(str)
data class NameNode(val name:String) : Constituents()
data class OpNode(val op:String) : Constituents()
sealed class CombinationNode : FNode()
data class Comparison(val op:OpNode, val op1:FNode, val op2:FNode) : CombinationNode()
data class Logical(val op:OpNode, val ops:Iterable<FNode>) : CombinationNode()

class FilterGrammar : Grammar() {
    fun uint() = repeat1 { digit() }

    fun int() = build_str(
            syntax = { println("int: ${this.stack}"); seq { opt { "+".str || "-".str } && uint() }},
            value = { NumberNode(it.toInt()) }
    )

    fun quote() = char_pred { it == '"' }
    fun strcontent() = build_str(
            syntax = {repeat0 { char_pred { it != '"' }  }},
            value = {StringNode(it)}
    )
    fun astr() = seq { char_set("\"") &&  strcontent() && char_set("\"") }

    fun value() = choice { int() || astr() }


    fun name() = build_str(
            syntax = {println("name: ${this.stack}"); seq {repeat1 { char_pred { it.isJavaIdentifierStart() && it!='_' } } && repeat0 { char_pred { it.isJavaIdentifierPart() } }}},
            value = {NameNode(it)}
    )

    fun tcCmp() = choice { string("!=") || string("<=") || string(">=") }
    fun ocCmp() = char_set("<>=")
    fun op() = build_str(
            syntax = {choice { tcCmp() || ocCmp() }},
            value = {OpNode(it)}
    )

    fun combiner() = build_str (
            syntax = {choice { string("&&") || string("||") }},
            value = {OpNode(it)}
    )

    fun logical() = build(
            syntax = {seq {opt { whitespace() } && term() && combiner()  && opt { whitespace() } && term()}},
            effect = {
                println("logical: ${it.toList()}")
                Logical(it(1), listOf(it(0), it(2)))
            }
    )

    fun binCmp() = seq { tcCmp() || tcCmp() }
    fun comparison() = build(
            syntax = {choice {
                       seq { word { name() } && op() && opt { whitespace() }  && word { value() }}
                    || seq { word { value() } && op()&& opt { whitespace() }  && word { name() }} }},
            effect = {
                println("compatison ${it.toList()}")
                Comparison(it(1), it(0), it(2))
            }
    )

    fun term(): Boolean = choice {
        comparison()
        || parens { logical() }
    }

    fun filter() = build(
            syntax = {seq {opt { whitespace() } && seq {word { name() } && opt { whitespace() } && curlies {
                seq {opt { whitespace() } && choice { logical() || term()} && opt { whitespace() }}
            } }}
                    && opt { whitespace() }},
            effect = {FilterNode(it(0), it(1))}
    )


    override fun root(): Boolean = filter()
}

fun diagnose (grammar: Grammar, input: String)
{
    if (grammar.parse(input))
        println("success ${grammar.stack}")
    else {
        println("failure: " + grammar.failure?.invoke())
        val f = grammar.failure
        if(f is UncaughtException) f.e.printStackTrace()
        if(f is UnexpectedChar) println("at : " +grammar.fail_pos)
        println("stack: " + grammar.stack)
    }

    grammar.reset()
}

fun<E:KIEntity<K>,K:Comparable<K>> parse(s:String, meta:MetaProvider) : EntityFilter<E,K> = run {
    val grammar = FilterGrammar()
    if(grammar.parse(s)) {
        val root = grammar.stack[0]
        val c = Creator<E,K>(meta)
        c.create(root as FilterNode)
    } else {
        println("${grammar.stack}")
        throw FilterError("cannot parse $s: ${grammar.failure}")
    }
}

class Creator<E:KIEntity<K>,K:Comparable<K>>(val metas: MetaProvider) {
    var meta: KIEntityMeta<K>? = null
    fun create(fn:FilterNode) : EntityFilter<E,K> = metas.meta(fn.entity.name)?.let {
        meta = it.cast()
        val root = fn.root
        compose(root)
    }?: throw FilterError("metas for ${fn.entity.name} not found")

    fun compose(n:FNode) : EntityFilter<E,K> = when(n) {
        is FilterNode -> throw FilterError("cant happen $n")
        is Constituents -> throw FilterError("cant happen $n")
        is CombinationNode -> when(n) {
            is Comparison -> compare(n)
            is Logical -> combine(n)
        }
        else -> throw FilterError("cant happen $n")
    }

    private fun compare(n:Comparison) : EntityFilter<E,K> = run {
        val first = n.op1
        val second = n.op2
        require(!(first is NameNode && second is NameNode))
        when(first) {
            is NameNode -> when(second) {
                is ValueNode<*> -> when(second) {
                    is NumberNode -> cmp(n.op.op, first.name, second.value)
                    is StringNode -> cmp(n.op.op, first.name, second.value)
                }
                else -> throw FilterError("not supported $n")
            }
            is ValueNode<*> -> when(second) {
                is NameNode -> when(first) {
                    is NumberNode -> cmp(n.op.op, second.name, first.value).inverse()
                    is StringNode -> cmp(n.op.op, second.name, first.value).inverse()
                }
                else -> throw FilterError("not supported $n")
            }
            else -> throw FilterError("not supported $n")
        }
    }

    private fun<T:Comparable<T>> cmp(op:String, name:String,value:T) : PropertyFilter<E, K,T> = run {
        val prop = meta!!.props[name]!! as KIProperty<T>
        val v = if(value is BigDecimal) prop.cast(value) else value
        when(op) {
            "<"  -> LTFilter<E,K,T>(prop, meta!!,v as T)
            "<=" -> LTEFilter(prop, meta!!, v as T)
            ">"  -> GTFilter<E,K,T>(prop, meta!!,v as T)
            ">=" -> GTEFilter(prop, meta!!, v as T)
            "="  -> EQFilter<E,K,T>(prop, meta!!,v as T)
            "!=" -> NEQFilter(prop, meta!!, v as T)
            else -> throw FilterError("not supported $op")
        }
    }
    private fun combine(n: Logical): EntityFilter<E,K> = run {
        val ops = n.ops.map(this::compose)
        when(ops.size){
            0 -> throw FilterError("cant happen")
            1 -> ops[0]
            else -> when(n.op.op) {
                "&&" -> ops[0].and(ops.drop(1))
                "||" -> ops[0].or(ops.drop(1))
                else -> throw FilterError("unknown operator ${n.op}")
            }
        }
    }
}

fun KIProperty<*>.cast(n:BigDecimal) : Comparable<*> = when(this) {
    is KILongProperty -> n.toLong()
    is KIIntProperty -> n.toInt()
    is KIDoubleProperty -> n.toDouble()
    else -> throw FilterError("unsupported type $this")
}

fun main (args: Array<String>)
{
    val grammar = FilterGrammar()
    //diagnose(grammar, "Test {1 < x}") // success
    //diagnose(grammar, " Test{x>=-12}") // failure: unexpected character
    diagnose(grammar, "Test {d = 1 && 12 != z}")
    diagnose(grammar, "Test {  12 !=z  }")
    diagnose(grammar, "Test{a=1&&12<b}")
}