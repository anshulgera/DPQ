// Pure queue engine: no HTTP, JSON or metrics-library dependencies (D2, D14a).
plugins {
    id("dpq.java-conventions")
    `java-library`
    `java-test-fixtures`
}

dependencies {
    testImplementation(libs.jqwik)
}
