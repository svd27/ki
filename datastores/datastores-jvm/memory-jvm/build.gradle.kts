import org.jetbrains.kotlin.gradle.dsl.Coroutines

dependencies {
    "expectedBy"(project(":core:common"))
    "compile"(project(":datastores:datastores-jvm"))
    "compile"("org.mapdb:mapdb:3.0.5")
    "kaptTest"(project(":core:annotations-processor-jvm"))
    "testCompile"(project(":core:core-jvm:filters-utils-jvm"))
}

plugins {
    idea
}

kotlin {
    experimental.coroutines = Coroutines.ENABLE
}


kapt {
    arguments { arg("targets", "jvm") }
}

tasks.withType<Test>() {
    systemProperties = systemProperties + ("logback.configurationFile" to  File(projectDir,"src/test/resources/logback-test.xml").absolutePath)
}


idea {
    module {
        testSourceDirs = testSourceDirs + files("build/generated/source/kapt/test", "build/generated/source/kaptKotlin/test")
    }
}