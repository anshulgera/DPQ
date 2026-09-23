plugins {
    // Downloads JDK 21 when it isn't installed locally (D18a).
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "dpq"

include("core", "server", "harness")
