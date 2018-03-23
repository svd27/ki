val jvm by extra {
    subprojects.filter {
        it.name.endsWith("-jvm")
    }
}

configure(jvm) {
    plugins {
        java
        kotlin("jvm")
        kotlin("kapt")
    }

    apply {
        plugin("kotlin-platform-jvm")
        plugin("kotlin-kapt")
    }

    val arrowVersion = "0.6.1"
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
        "compile" ("io.github.microutils:kotlin-logging:1.4.9")

        "compile"("io.arrow-kt:arrow-core:$arrowVersion")
        "compile"("io.arrow-kt:arrow-typeclasses:$arrowVersion")
        "compile"("io.arrow-kt:arrow-instances:$arrowVersion")
        "compile"("io.arrow-kt:arrow-data:$arrowVersion")
        "compile"("io.arrow-kt:arrow-syntax:$arrowVersion")
        "kapt"("io.arrow-kt:arrow-annotations-processor:$arrowVersion")

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

