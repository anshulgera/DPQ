package dpq.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dpq.core.FakeClock;
import dpq.core.QueueService;
import dpq.core.SequentialIdGenerator;
import dpq.core.SequentialReceiptGenerator;
import io.javalin.testtools.JavalinTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Every row of the HTTP API table in plan.md §5, through a real server on a random port. */
class DpqAppTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final ObjectMapper JSON = new ObjectMapper();

    private final FakeClock clock = new FakeClock(START);
    private QueueService service = service(QueueService.Options.defaults().withoutReaper());
    private final HttpClient http = HttpClient.newHttpClient();
    private String origin;

    private record Reply(int status, String body) {
        JsonNode json() throws Exception {
            return JSON.readTree(body);
        }
    }

    @AfterEach
    void close() {
        service.close();
    }

    @Test
    void createIsIdempotentAndConflictsOnADifferentConfig() {
        run(() -> {
            Reply created = put("/queues/orders", "{\"visibilityTimeoutSeconds\": 60}");
            assertThat(created.status()).isEqualTo(201);
            assertThat(created.json().get("visibilityTimeoutSeconds").asLong()).isEqualTo(60);
            assertThat(created.json().get("maxDeliveries").asInt()).isEqualTo(5);

            assertThat(put("/queues/orders", "{\"visibilityTimeoutSeconds\": 60, \"maxDepth\": 10000}").status())
                    .isEqualTo(200);

            Reply conflict = put("/queues/orders", "{\"visibilityTimeoutSeconds\": 61}");
            assertThat(conflict.status()).isEqualTo(409);
            assertThat(conflict.json().get("error").asText()).isEqualTo("QUEUE_CONFIG_CONFLICT");
            assertThat(conflict.json().has("message")).isTrue();
        });
    }

    @Test
    void createAcceptsAnEmptyBodyAndRejectsInvalidInput() {
        run(() -> {
            assertThat(put("/queues/a", "").status()).isEqualTo(201);
            assertThat(put("/queues/bad.name", "{}").status()).isEqualTo(400);
            assertThat(put("/queues/b", "{\"maxDeliveries\": 0}").status()).isEqualTo(400);
            assertThat(put("/queues/c", "{not json").json().get("error").asText()).isEqualTo("VALIDATION_ERROR");
        });
    }

    @Test
    void getReturnsTheConfigOr404() {
        run(() -> {
            put("/queues/orders", "{}");

            Reply found = get("/queues/orders");
            assertThat(found.status()).isEqualTo(200);
            assertThat(found.json().get("name").asText()).isEqualTo("orders");
            assertThat(found.json().get("visibilityTimeoutSeconds").asLong()).isEqualTo(30);

            Reply missing = get("/queues/nope");
            assertThat(missing.status()).isEqualTo(404);
            assertThat(missing.json().get("error").asText()).isEqualTo("QUEUE_NOT_FOUND");
        });
    }

    @Test
    void theFullEnqueueDequeueAckFlowWorks() {
        run(() -> {
            put("/queues/orders", "{}");

            Reply enqueued = post("/queues/orders/messages", "{\"payload\": \"hello\", \"priority\": \"HIGH\"}");
            assertThat(enqueued.status()).isEqualTo(201);
            String id = enqueued.json().get("messageId").asText();

            Reply dequeued = post("/queues/orders/dequeue", "");
            assertThat(dequeued.status()).isEqualTo(200);
            JsonNode m = dequeued.json();
            assertThat(m.get("messageId").asText()).isEqualTo(id);
            assertThat(m.get("payload").asText()).isEqualTo("hello");
            assertThat(m.get("priority").asText()).isEqualTo("HIGH");
            assertThat(m.get("deliveryCount").asInt()).isEqualTo(1);
            assertThat(m.get("enqueuedAt").asText()).isEqualTo("2026-01-01T00:00:00Z");
            assertThat(m.get("visibleUntil").asText()).isEqualTo("2026-01-01T00:00:30Z");
            assertThat(m.has("deadLetter")).isFalse();
            assertThat(m.has("deliverySeq")).isFalse(); // internal, for ordering tests only

            String ack = "{\"receiptHandle\": \"" + m.get("receiptHandle").asText() + "\"}";
            assertThat(post("/queues/orders/messages/" + id + "/ack", ack).status()).isEqualTo(204);
            assertThat(post("/queues/orders/messages/" + id + "/ack", ack).status()).isEqualTo(404);
        });
    }

    @Test
    void enqueueValidatesItsInput() {
        run(() -> {
            put("/queues/orders", "{}");

            Reply noPriority = post("/queues/orders/messages", "{\"payload\": \"x\"}");
            assertThat(noPriority.status()).isEqualTo(400);
            assertThat(noPriority.json().get("error").asText()).isEqualTo("VALIDATION_ERROR");

            assertThat(post("/queues/orders/messages", "{\"payload\": \"x\", \"priority\": \"URGENT\"}").status())
                    .isEqualTo(400);
            assertThat(post("/queues/orders/messages", "{\"priority\": \"LOW\"}").status()).isEqualTo(400);
            assertThat(post("/queues/orders/messages", "{\"payload\": \"x\", \"priority\": \"LOW\", \"ttlSeconds\": 0}")
                    .status()).isEqualTo(400);

            String big = "x".repeat(256 * 1024 + 1);
            assertThat(post("/queues/orders/messages", "{\"payload\": \"" + big + "\", \"priority\": \"LOW\"}")
                    .status()).isEqualTo(400);
            String max = "x".repeat(256 * 1024);
            assertThat(post("/queues/orders/messages", "{\"payload\": \"" + max + "\", \"priority\": \"LOW\"}")
                    .status()).isEqualTo(201);
        });
    }

    @Test
    void enqueueToAnUnknownQueueIs404AndIntoADlqIs400() {
        run(() -> {
            put("/queues/orders", "{}");
            String body = "{\"payload\": \"x\", \"priority\": \"LOW\"}";

            assertThat(post("/queues/nope/messages", body).status()).isEqualTo(404);
            assertThat(post("/queues/orders.dlq/messages", body).status()).isEqualTo(400);
        });
    }

    @Test
    void aFullQueueAnswers429() {
        run(() -> {
            put("/queues/tiny", "{\"maxDepth\": 1}");
            String body = "{\"payload\": \"x\", \"priority\": \"LOW\"}";
            post("/queues/tiny/messages", body);

            Reply full = post("/queues/tiny/messages", body);
            assertThat(full.status()).isEqualTo(429);
            assertThat(full.json().get("error").asText()).isEqualTo("QUEUE_FULL");
        });
    }

    @Test
    void creatingMoreQueuesThanTheCapAnswers429() {
        service = service(QueueService.Options.defaults().withoutReaper().withMaxQueues(1));
        run(() -> {
            put("/queues/a", "{}");

            Reply over = put("/queues/b", "{}");
            assertThat(over.status()).isEqualTo(429);
            assertThat(over.json().get("error").asText()).isEqualTo("QUEUE_LIMIT_EXCEEDED");
        });
    }

    @Test
    void dequeueOnAnEmptyQueueIs204AndOnAnUnknownQueue404() {
        run(() -> {
            put("/queues/orders", "{}");

            Reply empty = post("/queues/orders/dequeue", "");
            assertThat(empty.status()).isEqualTo(204);
            assertThat(empty.body()).isEmpty();
            assertThat(post("/queues/nope/dequeue", "").status()).isEqualTo(404);
        });
    }

    @Test
    void ackOutcomesFollowTheD8dTable() {
        run(() -> {
            put("/queues/orders", "{\"visibilityTimeoutSeconds\": 10}");
            String id = post("/queues/orders/messages", "{\"payload\": \"x\", \"priority\": \"LOW\"}")
                    .json().get("messageId").asText();
            String receipt = post("/queues/orders/dequeue", "").json().get("receiptHandle").asText();
            String ack = "{\"receiptHandle\": \"" + receipt + "\"}";

            Reply wrongReceipt = post("/queues/orders/messages/" + id + "/ack", "{\"receiptHandle\": \"nope\"}");
            assertThat(wrongReceipt.status()).isEqualTo(409);
            assertThat(wrongReceipt.json().get("error").asText()).isEqualTo("STALE_RECEIPT");

            clock.advance(Duration.ofSeconds(10)); // the lease is over
            assertThat(post("/queues/orders/messages/" + id + "/ack", ack).status()).isEqualTo(409);

            Reply unknown = post("/queues/orders/messages/p0-00000000-0000-0000-0000-000000000099/ack", ack);
            assertThat(unknown.status()).isEqualTo(404);
            assertThat(unknown.json().get("error").asText()).isEqualTo("MESSAGE_NOT_FOUND");
            assertThat(post("/queues/orders/messages/not-an-id/ack", ack).status()).isEqualTo(400);
            assertThat(post("/queues/orders/messages/" + id + "/ack", "{}").status()).isEqualTo(400);
        });
    }

    @Test
    void theDlqListsItsMessagesAndDequeuesWithDeadLetterMetadata() {
        run(() -> {
            put("/queues/orders", "{\"visibilityTimeoutSeconds\": 10, \"maxDeliveries\": 1}");
            String id = post("/queues/orders/messages", "{\"payload\": \"poison\", \"priority\": \"MEDIUM\"}")
                    .json().get("messageId").asText();
            post("/queues/orders/dequeue", "");
            clock.advance(Duration.ofSeconds(10));
            post("/queues/orders/dequeue", ""); // drains the expired lease into orders.dlq

            Reply listed = get("/queues/orders.dlq");
            assertThat(listed.status()).isEqualTo(200);
            JsonNode entry = listed.json().get("messages").get(0);
            assertThat(entry.get("messageId").asText()).isEqualTo(id);
            assertThat(entry.get("state").asText()).isEqualTo("READY");
            assertThat(entry.get("deadLetter").get("sourceQueue").asText()).isEqualTo("orders");

            JsonNode dead = post("/queues/orders.dlq/dequeue", "").json();
            assertThat(dead.get("messageId").asText()).isEqualTo(id);
            assertThat(dead.get("deliveryCount").asInt()).isEqualTo(1);
            JsonNode meta = dead.get("deadLetter");
            assertThat(meta.get("sourceQueue").asText()).isEqualTo("orders");
            assertThat(meta.get("deliveryCount").asInt()).isEqualTo(1);
            assertThat(meta.get("reason").asText()).isEqualTo("MAX_DELIVERIES");
            assertThat(meta.get("deadLetteredAt").asText()).isEqualTo("2026-01-01T00:00:10Z");

            assertThat(get("/queues/orders.dlq?limit=0").status()).isEqualTo(400);
            assertThat(get("/queues/orders.dlq?limit=101").status()).isEqualTo(400);
        });
    }

    @Test
    void healthIsUp() {
        run(() -> assertThat(get("/health").status()).isEqualTo(200));
    }

    private interface Body {
        void run() throws Exception;
    }

    private void run(Body body) {
        JavalinTest.test(DpqApp.create(service), (server, client) -> {
            origin = client.getOrigin();
            body.run();
        });
    }

    private Reply get(String path) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(origin + path)).GET());
    }

    private Reply put(String path, String body) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(origin + path)).PUT(HttpRequest.BodyPublishers.ofString(body)));
    }

    private Reply post(String path, String body) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(origin + path)).POST(HttpRequest.BodyPublishers.ofString(body)));
    }

    private Reply send(HttpRequest.Builder request) throws Exception {
        HttpResponse<String> response = http.send(request.header("Content-Type", "application/json").build(),
                HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(), response.body());
    }

    private QueueService service(QueueService.Options options) {
        return new QueueService(clock, new SequentialIdGenerator(), new SequentialReceiptGenerator(), options);
    }
}
