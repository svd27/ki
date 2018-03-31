dependencies {
    "expectedBy"(project(":core:common"))
    compile(project(":core:core-jvm"))
    "kaptTest"(project(":core:annotations-processor-jvm"))
    "testCompile"(project(":core:core-jvm:filters-utils-jvm"))
    "testCompile"(project(":datastores:datastores-jvm:memory-jvm"))
}

plugins { idea }

kapt {
    arguments { arg("targets", "jvm.memory") }
}

idea {
    module {
        testSourceDirs = testSourceDirs + files("build/generated/source/kapt/test", "build/generated/source/kaptKotlin/test")
    }
}