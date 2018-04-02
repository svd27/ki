package info.kinterest.core.jvm.filters

import info.kinterest.KIEntity
import info.kinterest.core.jvm.filters.parser.parse
import info.kinterest.jvm.MetaProvider
import info.kinterest.jvm.datastores.DataStoreFacade
import info.kinterest.jvm.filter.EntityFilter
import info.kinterest.jvm.filter.filter

inline fun <reified E : KIEntity<K>, K : Any> parse(s: String, metaProvider: MetaProvider, ds: DataStoreFacade) = run {
    filter<E,K>(ds, metaProvider) {
        val meta = metaProvider.meta(E::class)!!
        val q = if("${meta.name}\\s*\\{.*}".toRegex().matches(s.trim())) s else "${meta.name}{$s}"
        EntityFilter.Empty<E, K>(meta).parse(q, metaProvider)
    }
}