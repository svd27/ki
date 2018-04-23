import org.gradle.util.GradleVersion
import org.jetbrains.dokka.DokkaConfiguration
import org.jetbrains.dokka.gradle.DokkaPlugin
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.dsl.KotlinCompile
import org.jetbrains.kotlin.gradle.dsl.KotlinProjectExtension
import org.junit.platform.gradle.plugin.JUnitPlatformExtension
import java.net.URI

/*
 * This file was generated by the Gradle 'init' task.
 *
 * This is a general purpose Gradle build.
 * Learn how to create Gradle builds at https://guides.gradle.org/creating-new-gradle-builds/
 */
println("Gradle Version: ${GradleVersion.current().version}")
buildscript {
    repositories {
        jcenter()
        mavenCentral()
        maven("https://dl.bintray.com/kotlin/kotlin-eap/")
    }

    val dokkaVersion = "0.9.16-eap-3"
    dependencies {
        classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:1.2.40")
        classpath("org.junit.platform:junit-platform-gradle-plugin:1.1.0")
        classpath("com.kncept.junit5.reporter:junit-reporter:1.1.0")
        classpath("org.jetbrains.dokka:dokka-gradle-plugin:${dokkaVersion}")
    }
}

repositories {
    mavenCentral()
}

plugins {
    id("ch.netzwerg.release") version "1.2.3"
    maven
    `build-scan`
}


tasks.withType<DokkaTask>().forEach {
    it.outputFormat = "html"
    it.outputDirectory = File(buildDir, "javadoc").absolutePath
}

apply {
    plugin("org.jetbrains.dokka")
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

    afterEvaluate {
        configure<KotlinProjectExtension> {
            experimental.coroutines = Coroutines.ENABLE
        }
    }
}



apply {
    from("common.gradle.kts")
    from("jvm.gradle.kts")
    from("js.gradle.kts")
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
    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().all {
        kotlinOptions {
            jvmTarget = "1.8"
        }
    }
    configure<JUnitPlatformExtension> {
        filters {
            engines {
                include("spek")
            }
        }
    }
}


