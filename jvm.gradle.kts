val jvm by extra {
    subprojects.filter {
        it.name.endsWith("-jvm")
    }
}

configure(jvm) {
    plugins {
        java
        kotlin("jvm")
    }

    apply { plugin("kotlin-platform-jvm") }

    dependencies {
        "expectedBy"(project(":core:common"))
        "compile"(kotlin("reflect"))
        "compile"(kotlin("stdlib-jdk8"))
        "compile"("ch.qos.logback:logback-classic:1.0.13")
        "compile"( "com.github.salomonbrys.kodein:kodein:4.1.0")
        "compile"("io.vertx:vertx-web:3.5.1")
        "compile"("io.vertx:vertx-lang-kotlin-coroutines:3.5.1")
        "compile"("io.vertx:vertx-lang-kotlin:3.5.1")
        "compile"("com.beust:klaxon:2.1.13")
        "testCompile"("io.kotlintest:kotlintest:2.0.7")
    }

    tasks.withType<Test>() {
        doFirst {
            systemProperties = systemProperties + ("projectConfigScan" to "false")
            testLogging {
                showStandardStreams = true
            }
        }
    }

    repositories {
        jcenter()
        maven ( "https://jitpack.io")
    }
}

