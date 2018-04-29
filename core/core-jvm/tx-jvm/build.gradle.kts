import org.jetbrains.kotlin.gradle.dsl.Coroutines

dependencies {
    "expectedBy"(project(":core:common"))
    compile(project(":core:core-jvm"))
    "kaptTest"(project(":core:annotations-processor-jvm"))
    "kapt"(project(":core:annotations-processor-jvm"))
}

plugins { idea }

kapt {
    arguments { arg("targets", "jvm") }
}

kotlin {
    experimental.coroutines = Coroutines.ENABLE
}

idea {
    module {
        testSourceDirs = testSourceDirs + files("build/generated/source/kapt/test", "build/generated/source/kaptKotlin/test")
    }
}