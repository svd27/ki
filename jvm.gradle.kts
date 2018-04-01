
val jvm by extra {
    subprojects.filter {
        it.name.endsWith("-jvm")
    }
}

configure(jvm) {
    buildscript {
        repositories {
            mavenCentral()
        }
        dependencies {
            classpath ("org.jetbrains.kotlin:kotlin-gradle-plugin:1.2.30")
            classpath("org.junit.platform:junit-platform-gradle-plugin:1.1.0")
            classpath("com.kncept.junit5.reporter:junit-reporter:1.1.0")
        }
    }
    plugins {
        java
        kotlin("jvm")
        kotlin("kapt")
    }

    apply {
        plugin("kotlin-platform-jvm")
        plugin("kotlin-kapt")
        plugin("org.junit.platform.gradle.plugin")
        plugin("com.kncept.junit5.reporter")
    }

    val arrowVersion = "0.6.1"
    dependencies {
        "expectedBy"(project(":core:common"))
        "compile"(kotlin("reflect"))
        "compile"(kotlin("stdlib-jdk8"))
        "compile"("com.github.svd27.functional-stuff:jvm:v0.1.1")
        "compile"("ch.qos.logback:logback-classic:1.0.13")
        "compile"( "com.github.salomonbrys.kodein:kodein:4.1.0")
        "compile"("io.vertx:vertx-web:3.5.1")
        "compile"("io.vertx:vertx-lang-kotlin-coroutines:3.5.1")
        "compile"("io.vertx:vertx-lang-kotlin:3.5.1")
        "compile"("com.beust:klaxon:2.1.13")
        "compile" ("io.github.microutils:kotlin-logging:1.4.9")

        "testCompile"("org.jetbrains.spek:spek-api:1.1.5") {
            exclude("org.jetbrains.kotlin")
        }
        "testRuntime"("org.jetbrains.spek:spek-junit-platform-engine:1.1.5") {
            exclude("org.jetbrains.kotlin")
        }

        "testImplementation"("org.amshove.kluent:kluent:1.35")
        /*
        "compile"("io.arrow-kt:arrow-core:$arrowVersion")
        "compile"("io.arrow-kt:arrow-typeclasses:$arrowVersion")
        "compile"("io.arrow-kt:arrow-instances:$arrowVersion")
        "compile"("io.arrow-kt:arrow-data:$arrowVersion")
        "compile"("io.arrow-kt:arrow-syntax:$arrowVersion")
        "kapt"("io.arrow-kt:arrow-annotations-processor:$arrowVersion")
        */

        /*
        "testCompile"("io.kotlintest:kotlintest:2.0.7")
        "testCompileOnly"("junit:junit:4.12")
        "testRuntimeOnly"("org.junit.vintage:junit-vintage-engine:5.1.0")
        "testImplementation"("org.junit.jupiter:junit-jupiter-api:5.1.0")
        "testRuntimeOnly"("org.junit.jupiter:junit-jupiter-engine:5.1.0")
        */
    }


    /*
    tasks.withType<Test>() {
        useJUnitPlatform()
        System.setProperty("idea.io.use.fallback", "true")
        beforeTest( closureOf<Any>{
            logger.lifecycle("Running Test: $this")
        })
        doFirst {
            useJUnitPlatform()
            System.setProperty("idea.io.use.fallback", "true")
            systemProperties = systemProperties + ("projectConfigScan" to "false")
            testLogging {
                showStandardStreams = true
            }
        }
    }
    */

    repositories {
        jcenter()
        maven ( "https://jitpack.io")
    }
}

