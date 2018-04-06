package info.kinterest.core.jvm.filters

import info.kinterest.KIEntity
import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.filter.FilterWrapper
import info.kinterest.jvm.filter.filter
import info.kinterest.meta.KIEntityMeta

inline fun <reified E : KIEntity<K>, K : Any> parse(s: String, meta: KIEntityMeta): FilterWrapper<E, K> = run {
    filter(meta) {
        val q = if("${meta.name}\\s*\\{.*}".toRegex().matches(s.trim())) s else "${meta.name}{$s}"
        EntityFilter.Empty<E, K>(meta).parse(q, meta)
    }
}