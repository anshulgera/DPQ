package dpq.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dpq.core.QueueService;
import dpq.core.id.SequentialIdGenerator;
import dpq.core.id.SequentialReceiptGenerator;
import dpq.core.model.Priority;
import dpq.core.model.QueueConfig;
import dpq.core.time.FakeClock;
import io.javalin.testtools.JavalinTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** {@code GET /metrics} (Prometheus text) and {@code GET /queues/{name}/metrics} (JSON), D12a. */
class MetricsEndpointTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern SAMPLE = Pattern.compile("([a-zA-Z_:][a-zA-Z0-9_:]*)(\\{([^}]*)})? (\\S+)");
    private static final Pattern LABEL = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)=\"((?:[^\"\\\\]|\\\\.)*)\"");

    /** Expected families (plan.md §6): exposed name → type and label names. */
    private static final Map<String, Family> EXPECTED = new LinkedHashMap<>();

    static {
        EXPECTED.put("dpq_messages_ready", new Family("gauge", Set.of("queue", "priority")));
        EXPECTED.put("dpq_messages_in_flight", new Family("gauge", Set.of("queue")));
        EXPECTED.put("dpq_oldest_message_age_seconds", new Family("gauge", Set.of("queue", "priority")));
        EXPECTED.put("dpq_messages_enqueued_total", new Family("counter", Set.of("queue", "priority")));
        EXPECTED.put("dpq_messages_delivered_total", new Family("counter", Set.of("queue", "priority")));
        EXPECTED.put("dpq_dequeue_empty_total", new Family("counter", Set.of("queue")));
        EXPECTED.put("dpq_messages_acked_total", new Family("counter", Set.of("queue")));
        EXPECTED.put("dpq_messages_redelivered_total", new Family("counter", Set.of("queue")));
        EXPECTED.put("dpq_messages_dead_lettered_total", new Family("counter", Set.of("queue")));
        EXPECTED.put("dpq_messages_expired_total", new Family("counter", Set.of("queue", "priority")));
        EXPECTED.put("dpq_enqueue_rejected_total", new Family("counter", Set.of("queue")));
        EXPECTED.put("dpq_operation_duration_seconds", new Family("histogram", Set.of("queue", "op")));
    }

    private record Family(String type, Set<String> labels) {}

    private record Sample(String name, Map<String, String> labels, double value) {}

    private final FakeClock clock = new FakeClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final QueueService service = new QueueService(clock, new SequentialIdGenerator(),
            new SequentialReceiptGenerator(), QueueService.Options.defaults().withoutReaper());
    private final AtomicLong nanos = new AtomicLong();
    private final HttpClient http = HttpClient.newHttpClient();
    private String origin;

    @AfterEach
    void close() {
        service.close();
    }

    @Test
    void theExpositionIsWellFormedAndHasEveryMetricWithItsTypeAndLabels() {
        run(() -> {
            scriptedScenario();
            send("POST", "/queues/orders/dequeue", ""); // one timed request, so the histogram has a series
            HttpResponse<String> response = send("GET", "/metrics", "");

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.headers().firstValue("Content-Type").orElse(""))
                    .startsWith("text/plain; version=0.0.4");
            Map<String, String> types = new HashMap<>();
            Set<String> helps = new HashSet<>();
            for (String line : response.body().split("\n")) {
                if (line.startsWith("# TYPE ")) {
                    String[] parts = line.split(" ");
                    types.put(parts[2], parts[3]);
                } else if (line.startsWith("# HELP ")) {
                    helps.add(line.split(" ")[2]);
                } else if (!line.isBlank()) {
                    assertThat(line).as("sample line").matches(SAMPLE);
                    assertThat(Double.parseDouble(SAMPLE.matcher(line).results().findFirst().orElseThrow().group(4)))
                            .isNotNaN();
                }
            }
            EXPECTED.forEach((name, family) -> {
                assertThat(types).as("TYPE of " + name).containsEntry(name, family.type());
                assertThat(helps).as("HELP of " + name).contains(name);
                String sampleName = family.type().equals("histogram") ? name + "_count" : name;
                assertThat(samples(response.body(), sampleName)).as("samples of " + sampleName).isNotEmpty()
                        .allSatisfy(s -> assertThat(s.labels().keySet()).containsExactlyInAnyOrderElementsOf(
                                family.labels()));
            });
        });
    }

    @Test
    void valuesMatchTheServiceStateAfterAScriptedScenario() {
        run(() -> {
            scriptedScenario();
            String body = send("GET", "/metrics", "").body();

            assertThat(value(body, "dpq_messages_ready", "orders", "priority", "LOW")).isEqualTo(1);
            assertThat(value(body, "dpq_messages_ready", "orders", "priority", "HIGH")).isZero();
            assertThat(value(body, "dpq_messages_in_flight", "orders")).isEqualTo(1);
            assertThat(value(body, "dpq_oldest_message_age_seconds", "orders", "priority", "LOW")).isEqualTo(2.0);
            assertThat(value(body, "dpq_messages_enqueued_total", "orders", "priority", "HIGH")).isEqualTo(2);
            assertThat(value(body, "dpq_messages_delivered_total", "orders", "priority", "HIGH")).isEqualTo(2);
            assertThat(value(body, "dpq_dequeue_empty_total", "orders")).isZero();
            assertThat(value(body, "dpq_messages_acked_total", "orders")).isEqualTo(1);
            assertThat(value(body, "dpq_enqueue_rejected_total", "orders")).isEqualTo(1);
            assertThat(value(body, "dpq_messages_dead_lettered_total", "orders")).isZero();
        });
    }

    @Test
    void theDlqIsItsOwnQueueLabel() {
        run(() -> {
            service.createQueue("jobs", QueueConfig.of(10L, 1, null));
            service.enqueue("jobs", "poison", Priority.MEDIUM, null);
            service.dequeue("jobs").orElseThrow();
            clock.advance(Duration.ofSeconds(10));
            service.dequeue("jobs"); // dead-letters it

            String body = send("GET", "/metrics", "").body();

            assertThat(value(body, "dpq_messages_dead_lettered_total", "jobs")).isEqualTo(1);
            assertThat(value(body, "dpq_messages_ready", "jobs.dlq", "priority", "MEDIUM")).isEqualTo(1);
            assertThat(value(body, "dpq_messages_enqueued_total", "jobs.dlq", "priority", "MEDIUM")).isEqualTo(1);
        });
    }

    @Test
    void operationDurationsAreRecordedPerQueueAndOperation() {
        run(() -> {
            send("PUT", "/queues/orders", "{}");
            for (int i = 0; i < 3; i++) {
                send("POST", "/queues/orders/messages", "{\"payload\": \"x\", \"priority\": \"LOW\"}");
            }
            JsonNode d = JSON.readTree(send("POST", "/queues/orders/dequeue", "").body());
            send("POST", "/queues/orders/dequeue", "");
            send("POST", "/queues/orders/messages/" + d.get("messageId").asText() + "/ack",
                    "{\"receiptHandle\": \"" + d.get("receiptHandle").asText() + "\"}");
            send("POST", "/queues/nope/messages", "{\"payload\": \"x\", \"priority\": \"LOW\"}"); // unknown queue
            send("POST", "/queues/nope/messages", "{not json"); // fails before the queue lookup

            String body = send("GET", "/metrics", "").body();

            assertThat(histogram(body, "_count", "orders", "enqueue")).isEqualTo(3);
            assertThat(histogram(body, "_sum", "orders", "enqueue")).isEqualTo(0.015); // 5ms each (fake ticker)
            assertThat(histogram(body, "_count", "orders", "dequeue")).isEqualTo(2);
            assertThat(histogram(body, "_count", "orders", "ack")).isEqualTo(1);
            // Unknown queue names are never recorded, so request paths can't inflate label cardinality.
            assertThat(samples(body, "dpq_operation_duration_seconds_count"))
                    .noneMatch(s -> "nope".equals(s.labels().get("queue")));
        });
    }

    @Test
    void theJsonEndpointMatchesTheSnapshotIncludingRates() {
        run(() -> {
            service.createQueue("orders", QueueConfig.of(null, null, null));
            for (int second = 0; second < 60; second++) {
                service.enqueue("orders", "m", Priority.HIGH, null);
                clock.advance(Duration.ofSeconds(1));
            }
            service.enqueue("orders", "m", Priority.LOW, null);
            clock.advance(Duration.ofSeconds(3));

            HttpResponse<String> response = send("GET", "/queues/orders/metrics", "");
            JsonNode m = JSON.readTree(response.body());

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(m.get("queue").asText()).isEqualTo("orders");
            assertThat(m.get("ready").get("HIGH").asLong()).isEqualTo(60);
            assertThat(m.get("ready").get("LOW").asLong()).isEqualTo(1);
            assertThat(m.get("totalReady").asLong()).isEqualTo(61);
            assertThat(m.get("inFlight").asLong()).isZero();
            assertThat(m.get("oldestMessageAgeSeconds").asDouble()).isEqualTo(63.0);
            assertThat(m.get("oldestAgeSecondsByPriority").get("LOW").asDouble()).isEqualTo(3.0);
            assertThat(m.get("enqueued").get("HIGH").asLong()).isEqualTo(60);
            assertThat(m.get("deadLettered").asLong()).isZero();
            // Seconds 3..62 hold 57 of the HIGH enqueues and the LOW one.
            assertThat(m.get("enqueueRatePerSec").asDouble()).isEqualTo(58.0 / 60);
            assertThat(m.get("ackRatePerSec").asDouble()).isZero();

            assertThat(send("GET", "/queues/nope/metrics", "").statusCode()).isEqualTo(404);
        });
    }

    /** orders: 2 HIGH enqueued and delivered, 1 acked, 1 in flight; 1 LOW ready for 2s; 1 enqueue rejected. */
    private void scriptedScenario() {
        service.createQueue("orders", QueueConfig.of(null, null, 3));
        var first = service.enqueue("orders", "a", Priority.HIGH, null);
        service.enqueue("orders", "b", Priority.HIGH, null);
        service.enqueue("orders", "c", Priority.LOW, null);
        try {
            service.enqueue("orders", "d", Priority.LOW, null);
        } catch (dpq.core.error.QueueFullException expected) {
            // counted as rejected
        }
        var delivered = service.dequeue("orders").orElseThrow();
        service.ack("orders", first, delivered.receipt());
        service.dequeue("orders").orElseThrow();
        clock.advance(Duration.ofSeconds(2));
    }

    private interface Body {
        void run() throws Exception;
    }

    private void run(Body body) {
        // Each read of the ticker moves it 5ms, so every timed request takes exactly 5ms.
        JavalinTest.test(DpqApp.create(service, () -> nanos.addAndGet(5_000_000)), (server, client) -> {
            origin = client.getOrigin();
            body.run();
        });
    }

    private HttpResponse<String> send(String method, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(origin + path))
                .method(method, HttpRequest.BodyPublishers.ofString(body)).build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static java.util.List<Sample> samples(String exposition, String name) {
        return exposition.lines().filter(l -> !l.startsWith("#")).map(SAMPLE::matcher).filter(Matcher::matches)
                .filter(m -> m.group(1).equals(name))
                .map(m -> new Sample(m.group(1), labels(m.group(3)), Double.parseDouble(m.group(4))))
                .toList();
    }

    private static Map<String, String> labels(String raw) {
        Map<String, String> labels = new HashMap<>();
        if (raw != null) {
            LABEL.matcher(raw).results().forEach(r -> labels.put(r.group(1), r.group(2)));
        }
        return labels;
    }

    private static double value(String exposition, String name, String queue, String... extra) {
        Map<String, String> wanted = new HashMap<>(Map.of("queue", queue));
        for (int i = 0; i < extra.length; i += 2) {
            wanted.put(extra[i], extra[i + 1]);
        }
        return samples(exposition, name).stream().filter(s -> s.labels().equals(wanted)).findFirst()
                .orElseThrow(() -> new AssertionError("no sample " + name + wanted)).value();
    }

    private static double histogram(String exposition, String suffix, String queue, String op) {
        return value(exposition, "dpq_operation_duration_seconds" + suffix, queue, "op", op);
    }
}
