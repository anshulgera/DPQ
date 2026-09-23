// Pure queue engine: no HTTP, JSON or metrics-library dependencies (D2, D14a).
plugins {
    id("dpq.java-conventions")
    `java-library`
}

dependencies {
    testImplementation(libs.jqwik)
}
