package info.kinterest.core.jvm.filters

import info.kinterest.KIEntity
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.filter.filter
import info.kinterest.meta.KIEntityMeta

inline fun<reified E:KIEntity<K>,K:Comparable<K>> parse(s:String, metaProvider: MetaProvider) = run {
    filter<E,K>(metaProvider) {
        val meta = metaProvider.meta(E::class)!!
        val q = if("${meta.name}\\s*\\{.*}".toRegex().matches(s.trim())) s else "${meta.name}{$s}"
        info.kinterest.core.jvm.filters.parser.parse<E,K>(q, metaProvider)
    }
}