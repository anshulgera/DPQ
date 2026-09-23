// HTTP/JSON and Prometheus layer over the core engine (D2, D14a).
plugins {
    id("dpq.java-conventions")
    application
}

dependencies {
    implementation(project(":core"))
    implementation(libs.javalin)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.jsr310)
    implementation(libs.prometheus.core)
    implementation(libs.prometheus.textformats)
    runtimeOnly(libs.slf4j.simple)

    testImplementation(testFixtures(project(":core")))
    testImplementation(libs.javalin.testtools)
}

application {
    mainClass = "dpq.server.Main"
}
