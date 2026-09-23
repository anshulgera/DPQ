// Pure queue engine: no HTTP, JSON or metrics-library dependencies (D2, D14a).
plugins {
    id("dpq.java-conventions")
    `java-library`
    `java-test-fixtures`
}

dependencies {
    testImplementation(libs.jqwik)
    testImplementation(libs.awaitility)
}

// Stress tests (D13a) run separately: `./gradlew :core:stressTest`; `check` skips them.
tasks.test {
    useJUnitPlatform {
        excludeTags("stress")
    }
}

val stressTest by tasks.registering(Test::class) {
    description = "Runs the concurrency stress tests (tag: stress)."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform {
        includeTags("stress")
    }
    shouldRunAfter(tasks.test)
}
