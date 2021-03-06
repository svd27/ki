import org.jetbrains.kotlin.gradle.dsl.KotlinCompile

import org.jetbrains.kotlin.gradle.dsl.Coroutines

dependencies {
    "expectedBy"(project(":core:common"))
    compile("org.yanex.takenoko:takenoko:0.1.1")
    "kaptTest"(project(":"))
    "kapt"(project(":"))
    compile(project(":core:core-jvm"))
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
    arguments { arg("targets", "jvm") }
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

