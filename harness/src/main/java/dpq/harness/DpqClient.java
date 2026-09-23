package dpq.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

/** A minimal blocking client for the DPQ HTTP API (plan.md §5); safe to share across (virtual) threads. */
final class DpqClient {

    record Delivery(String messageId, String receiptHandle) {}

    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final String baseUrl;

    DpqClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    void createQueue(String queue, int visibilityTimeoutSeconds) throws IOException, InterruptedException {
        HttpResponse<String> r = send("PUT", "/queues/" + queue,
                "{\"visibilityTimeoutSeconds\": " + visibilityTimeoutSeconds + "}");
        expect(r, 200, 201);
    }

    String enqueue(String queue, String payload, String priority) throws IOException, InterruptedException {
        String body = JSON.createObjectNode().put("payload", payload).put("priority", priority).toString();
        HttpResponse<String> r = send("POST", "/queues/" + queue + "/messages", body);
        expect(r, 201);
        return JSON.readTree(r.body()).get("messageId").asText();
    }

    Optional<Delivery> dequeue(String queue) throws IOException, InterruptedException {
        HttpResponse<String> r = send("POST", "/queues/" + queue + "/dequeue", "");
        if (r.statusCode() == 204) {
            return Optional.empty();
        }
        expect(r, 200);
        JsonNode m = JSON.readTree(r.body());
        return Optional.of(new Delivery(m.get("messageId").asText(), m.get("receiptHandle").asText()));
    }

    /** True if acked; false if the lease had ended (409) or the message is gone (404), both normal under load. */
    boolean ack(String queue, Delivery d) throws IOException, InterruptedException {
        HttpResponse<String> r = send("POST", "/queues/" + queue + "/messages/" + d.messageId() + "/ack",
                JSON.createObjectNode().put("receiptHandle", d.receiptHandle()).toString());
        expect(r, 204, 404, 409);
        return r.statusCode() == 204;
    }

    long deadLettered(String queue) throws IOException, InterruptedException {
        HttpResponse<String> r = send("GET", "/queues/" + queue + "/metrics", null);
        expect(r, 200);
        return JSON.readTree(r.body()).get("deadLettered").asLong();
    }

    private HttpResponse<String> send(String method, String path, String body)
            throws IOException, InterruptedException {
        HttpRequest.BodyPublisher publisher = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .method(method, publisher)
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void expect(HttpResponse<String> r, int... allowed) {
        for (int status : allowed) {
            if (r.statusCode() == status) {
                return;
            }
        }
        throw new IllegalStateException(r.request().method() + " " + r.request().uri() + " -> " + r.statusCode()
                + " " + r.body());
    }
}
