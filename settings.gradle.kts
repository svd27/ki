/*
 * This file was generated by the Gradle 'init' task.
 *
 * The settings file is used to specify which projects to include in your build.
 * 
 * Detailed information about configuring a multi-project build in Gradle can be found
 * in the user guide at https://docs.gradle.org/4.6/userguide/multi_project_builds.html
 */

rootProject.name = "kinterest"

include("core:common",
        "core:annotations-processor-jvm",
        "core:core-jvm",
        "core:annotations-processor-jvm:example-jvm",
        "datastores:datastores-jvm:memory-jvm",
        "datastores:datastores-jvm:mongo-jvm")

includeBuild(rootDir.resolve("takenoko"))