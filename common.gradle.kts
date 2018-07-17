val common by extra {
    subprojects.filter {
        !it.name.endsWith("-jvm") && !it.name.endsWith("-js")
    }
}

configure(common) {
    apply {
        plugin("kotlin-platform-common")
    }

    dependencies {
        "compile"("org.jetbrains.kotlinx:kotlinx-coroutines-core-common:0.23.4")
        "compile"(kotlin("stdlib-common"))
        "testCompile"(kotlin("test-annotations-common"))
        "testCompile"(kotlin("test-common"))
    }

    repositories {
        jcenter()
        maven("https://jitpack.io")
    }
}