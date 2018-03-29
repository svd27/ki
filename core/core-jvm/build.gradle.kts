import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension

dependencies {
    "expectedBy"(project(":core:common"))
    testCompile(project(":core:core-jvm:filters-parser-jvm"))
}

configure<KotlinJvmProjectExtension> {
    experimental.coroutines = Coroutines.ENABLE
}