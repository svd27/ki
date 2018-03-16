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
        compile(kotlin("stdlib-common"))
        //testCompile
        //compile "org.jetbrains.kotlin:kotlin-stdlib-common:$kotlin_version"
        //testCompile "org.jetbrains.kotlin:kotlin-test-annotations-common:$kotlin_version"
        //testCompile "org.jetbrains.kotlin:kotlin-test-common:$kotlin_version"
    }
}