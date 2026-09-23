# syntax=docker/dockerfile:1

# Build: compile and assemble the server and harness distributions with the Gradle wrapper. The JDK here is 21,
# so the Gradle toolchain resolves to it without downloading anything.
FROM eclipse-temurin:21-jdk-noble AS build
WORKDIR /src
COPY gradlew settings.gradle.kts ./
COPY gradle gradle
COPY buildSrc buildSrc
COPY core/build.gradle.kts core/
COPY server/build.gradle.kts server/
COPY harness/build.gradle.kts harness/
RUN ./gradlew --no-daemon --quiet help
COPY core/src core/src
COPY server/src server/src
COPY harness/src harness/src
RUN ./gradlew --no-daemon :server:installDist :harness:installDist

# Run: JRE only, non-root. The same image runs the harness (see docker-compose.yml).
FROM eclipse-temurin:21-jre-noble
RUN useradd --system --uid 10001 dpq
COPY --from=build /src/server/build/install/server /opt/dpq/server
COPY --from=build /src/harness/build/install/harness /opt/dpq/harness
USER dpq
ENV DPQ_PORT=8080
EXPOSE 8080
# bash's /dev/tcp, so the check doesn't depend on curl being in the base image.
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=12 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/${DPQ_PORT} && printf "GET /health HTTP/1.0\r\n\r\n" >&3 && grep -q UP <&3'
ENTRYPOINT ["/opt/dpq/server/bin/server"]
