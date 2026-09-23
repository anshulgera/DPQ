// Producer/consumer load harness; talks to the server over HTTP only (D14b).
plugins {
    id("dpq.java-conventions")
    application
}

dependencies {
    implementation(libs.jackson.databind)
    implementation(libs.hdrhistogram)

    // The demo test starts a real server in-process; the harness itself never depends on it.
    testImplementation(project(":server"))
    testImplementation(project(":core"))
    testImplementation(libs.javalin)
}

application {
    mainClass = "dpq.harness.Harness"
}
