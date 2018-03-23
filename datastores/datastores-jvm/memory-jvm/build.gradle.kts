import org.jetbrains.kotlin.gradle.dsl.Coroutines

dependencies {
    "expectedBy"(project(":core:common"))
    "compile"(project(":datastores:datastores-jvm"))
    "compile"("org.mapdb:mapdb:3.0.5")
    "kaptTest"(project(":core:annotations-processor-jvm"))
}

plugins {
    idea
}

kotlin {
    experimental.coroutines = Coroutines.ENABLE
}


kapt {
    arguments { arg("targets", "jvm.memory") }
}

idea {
    module {
        testSourceDirs = testSourceDirs + files("build/generated/source/kapt/test", "build/generated/source/kaptKotlin/test")
    }
}