val js by extra {
    subprojects.filter {
        it.name.endsWith("-js")
    }
}