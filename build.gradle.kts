import org.gradle.util.GradleVersion
import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.dsl.KotlinCompile
import org.jetbrains.kotlin.gradle.dsl.KotlinProjectExtension
import org.junit.platform.gradle.plugin.JUnitPlatformExtension

/*
 * This file was generated by the Gradle 'init' task.
 *
 * This is a general purpose Gradle build.
 * Learn how to create Gradle builds at https://guides.gradle.org/creating-new-gradle-builds/
 */
println("Gradle Version: ${GradleVersion.current().version}")
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
    id("ch.netzwerg.release") version "1.2.3"
    maven
    `build-scan`
}

afterEvaluate {
    configure<KotlinProjectExtension> {
        experimental.coroutines = Coroutines.ENABLE
    }
}


buildScan {
    setLicenseAgreementUrl("https://gradle.com/terms-of-service")
    setLicenseAgree("yes")
}

apply {
    plugin("kotlin-platform-common")
}

allprojects {
    group = "info.kinterest"
    plugins { maven }
}



apply {
    from("common.gradle.kts")
    from("jvm.gradle.kts")
}

repositories {
    jcenter()
    mavenLocal()
}



val jvm by extra {
    subprojects.filter {
        it.name.endsWith("-jvm")
    }
}

configure(jvm) {
    tasks.withType<JavaExec>() {
        println("Task $name")
        if(name == "junitPlatformTest") finalizedBy("junitHtmlReport")
    }
    configure<JUnitPlatformExtension> {
        filters {
            engines {
                include("spek")
            }
        }
    }
}


