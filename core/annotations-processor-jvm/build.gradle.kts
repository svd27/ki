import org.jetbrains.kotlin.gradle.dsl.KotlinCompile

import org.jetbrains.kotlin.gradle.dsl.Coroutines

dependencies {
    "expectedBy"(project(":core:common"))
    compile("org.yanex.takenoko:takenoko:0.1.1")
    "testCompile"(project("example-jvm"))
    "kaptTest"(project(":"))
    compile(project(":core:core-jvm"))
    compile(project(":datastores:datastores-jvm:memory-jvm"))
}

tasks.withType<Test> {
    doFirst {
        systemProperties = systemProperties + ("projectConfigScan" to "false")
    }
}


plugins {
    kotlin("jvm")
    kotlin("kapt")
    idea
    maven
    jacoco
}

jacoco {
    toolVersion = "0.8.0"
}

kapt {
    arguments { arg("targets", "jvm.memory") }
}
kotlin {
    experimental.coroutines = Coroutines.ENABLE
}

repositories {
    mavenLocal()
    maven("${System.getProperties()["user.home"]}/.m2/repository")
    maven ( "https://jitpack.io")
    repositories.filterIsInstance<MavenArtifactRepository>().forEach { println("REPO ${it.name} ${it.url}") }
}

idea {
    module {
        testSourceDirs = testSourceDirs + files("build/generated/source/kapt/test", "build/generated/source/kaptKotlin/test")
    }
}

