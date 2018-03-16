val jvm by extra {
    subprojects.filter {
        it.name.endsWith("-jvm")
    }
}

println("JVM: $jvm")

configure(jvm) {
    plugins {
        java
        kotlin("jvm")
    }
    apply {
        plugin("kotlin-platform-jvm")
    }

    dependencies {
        "compile"(kotlin("reflect"))
        "compile"(kotlin("stdlib-jdk8"))
        "compile"("ch.qos.logback:logback-classic:1.0.13")
        "compile"( "com.github.salomonbrys.kodein:kodein:4.1.0")
        "compile"("io.vertx:vertx-web:3.5.1")
        "compile"("io.vertx:vertx-lang-kotlin-coroutines:3.5.1")
        "compile"("io.vertx:vertx-lang-kotlin:3.5.1")
        "testCompile"("io.kotlintest:kotlintest:2.0.7")
    }

    repositories {
        jcenter()
        maven ( "https://jitpack.io")
    }
}

