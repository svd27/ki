import org.jetbrains.kotlin.gradle.dsl.KotlinCompile

import org.jetbrains.kotlin.gradle.dsl.Coroutines

dependencies {
    "expectedBy"(project(":core:common"))
    "compile"("org.yanex.takenoko:takenoko:0.1.1")
    "testCompile"(project("example-jvm"))
    "kaptTest"(project(":"))
}

plugins {
    kotlin("kapt")
    idea
    maven
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

