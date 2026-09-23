package dpq.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dpq.core.MessageId;
import dpq.core.MessageNotFoundException;
import dpq.core.QueueAlreadyExistsException;
import dpq.core.QueueConfig;
import dpq.core.QueueDescription;
import dpq.core.QueueFullException;
import dpq.core.QueueLimitExceededException;
import dpq.core.QueueNotFoundException;
import dpq.core.QueueService;
import dpq.core.ReceiptHandle;
import dpq.core.StaleReceiptException;
import dpq.core.ValidationException;
import dpq.server.Dtos.AckRequest;
import dpq.server.Dtos.CreateQueueRequest;
import dpq.server.Dtos.DequeueResponse;
import dpq.server.Dtos.EnqueueRequest;
import dpq.server.Dtos.EnqueueResponse;
import dpq.server.Dtos.ErrorResponse;
import dpq.server.Dtos.HealthResponse;
import dpq.server.Dtos.MessageJson;
import dpq.server.Dtos.MessageListResponse;
import dpq.server.Dtos.MetricsResponse;
import dpq.server.Dtos.QueueResponse;
import io.javalin.Javalin;
import io.javalin.config.RoutesConfig;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpStatus;
import io.javalin.json.JavalinJackson;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The HTTP/JSON API over a {@link QueueService} (D2, plan.md §5). Thin: all queue logic lives in core. */
public final class DpqApp {

    private static final Logger LOG = LoggerFactory.getLogger(DpqApp.class);
    private static final int DEFAULT_LIST_LIMIT = 100;
    // Room for a 256 KiB payload even if every character is JSON-escaped; core enforces the real limit.
    private static final long MAX_REQUEST_BYTES = 2 * 1024 * 1024;
    // Seconds; dense around the p95 < 100ms target.
    private static final double[] DURATION_BUCKETS =
            {0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5};

    static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);

    private DpqApp() {}

    public static Javalin create(QueueService service) {
        return create(service, System::nanoTime);
    }

    /**
     * @param nanoTicker the time source for request durations; injectable so tests can make them exact. Queue
     *                   logic never uses it: that runs on the engine's {@code Clock}.
     */
    public static Javalin create(QueueService service, LongSupplier nanoTicker) {
        PrometheusRegistry registry = new PrometheusRegistry();
        registry.register(new QueueMetricsCollector(service));
        Histogram durations = Histogram.builder()
                .name("dpq_operation_duration_seconds")
                .help("Server-side duration of enqueue, dequeue and ack requests.")
                .labelNames("queue", "op")
                .classicOnly()
                .classicUpperBounds(DURATION_BUCKETS)
                .register(registry);
        Timer timer = new Timer(service, durations, nanoTicker);
        return Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson(JSON, false));
            config.http.maxRequestSize = MAX_REQUEST_BYTES;
            routes(config.routes, service, timer);
            metricsRoutes(config.routes, service, registry);
            errors(config.routes);
        });
    }

    /** Times a queue operation into {@code dpq_operation_duration_seconds{queue, op}} (D12a). */
    private record Timer(QueueService service, Histogram durations, LongSupplier ticker) {
        Handler timed(String op, Handler handler) {
            return ctx -> {
                long start = ticker.getAsLong();
                try {
                    handler.handle(ctx);
                } finally {
                    // Only existing queues become label values, so request paths can't inflate cardinality.
                    String queue = ctx.pathParam("name");
                    if (service.hasQueue(queue)) {
                        durations.labelValues(queue, op).observe((ticker.getAsLong() - start) / 1e9);
                    }
                }
            };
        }
    }

    private static void metricsRoutes(RoutesConfig routes, QueueService service, PrometheusRegistry registry) {
        PrometheusTextFormatWriter writer = PrometheusTextFormatWriter.create();
        routes.get("/metrics", ctx -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writer.write(out, registry.scrape()); // collectors read the queues now, not before
            ctx.contentType(PrometheusTextFormatWriter.CONTENT_TYPE).result(out.toByteArray());
        });
        routes.get("/queues/{name}/metrics",
                ctx -> ctx.json(MetricsResponse.of(service.metrics(ctx.pathParam("name")))));
    }

    private static void routes(RoutesConfig routes, QueueService service, Timer timer) {
        routes.get("/health", ctx -> ctx.json(new HealthResponse("UP")));

        routes.put("/queues/{name}", ctx -> {
            CreateQueueRequest req = Optional.ofNullable(body(ctx, CreateQueueRequest.class))
                    .orElse(new CreateQueueRequest(null, null, null));
            String name = ctx.pathParam("name");
            boolean created = service.createQueue(name,
                    QueueConfig.of(req.visibilityTimeoutSeconds(), req.maxDeliveries(), req.maxDepth()));
            ctx.status(created ? HttpStatus.CREATED : HttpStatus.OK).json(QueueResponse.of(service.getQueue(name)));
        });

        routes.get("/queues/{name}", ctx -> {
            QueueDescription queue = service.getQueue(ctx.pathParam("name"));
            if (!queue.deadLetterQueue()) {
                ctx.json(QueueResponse.of(queue));
                return;
            }
            // A DLQ exposes its messages rather than its fixed config (D18b).
            ctx.json(new MessageListResponse(service.listMessages(queue.name(), listLimit(ctx)).stream()
                    .map(MessageJson::of).toList()));
        });

        routes.post("/queues/{name}/messages", timer.timed("enqueue", ctx -> {
            EnqueueRequest req = body(ctx, EnqueueRequest.class);
            if (req == null) {
                throw new ValidationException("a JSON body with payload and priority is required");
            }
            Duration ttl = req.ttlSeconds() == null ? null : Duration.ofSeconds(req.ttlSeconds());
            MessageId id = service.enqueue(ctx.pathParam("name"), req.payload(), req.priority(), ttl);
            ctx.status(HttpStatus.CREATED).json(new EnqueueResponse(id.toString()));
        }));

        routes.post("/queues/{name}/dequeue", timer.timed("dequeue",
                ctx -> service.dequeue(ctx.pathParam("name")).ifPresentOrElse(
                        m -> ctx.json(DequeueResponse.of(m)),
                        () -> ctx.status(HttpStatus.NO_CONTENT))));

        routes.post("/queues/{name}/messages/{id}/ack", timer.timed("ack", ctx -> {
            AckRequest req = body(ctx, AckRequest.class);
            if (req == null || req.receiptHandle() == null || req.receiptHandle().isBlank()) {
                throw new ValidationException("receiptHandle is required");
            }
            service.ack(ctx.pathParam("name"), MessageId.parse(ctx.pathParam("id")),
                    new ReceiptHandle(req.receiptHandle()));
            ctx.status(HttpStatus.NO_CONTENT);
        }));
    }

    /** Maps engine exceptions to status codes and a {@code {error, message}} body (plan.md §5, D8d). */
    private static void errors(RoutesConfig routes) {
        routes.exception(ValidationException.class, (e, ctx) -> error(ctx, 400, "VALIDATION_ERROR", e));
        routes.exception(QueueNotFoundException.class, (e, ctx) -> error(ctx, 404, "QUEUE_NOT_FOUND", e));
        routes.exception(MessageNotFoundException.class, (e, ctx) -> error(ctx, 404, "MESSAGE_NOT_FOUND", e));
        routes.exception(QueueAlreadyExistsException.class,
                (e, ctx) -> error(ctx, 409, "QUEUE_CONFIG_CONFLICT", e));
        routes.exception(StaleReceiptException.class, (e, ctx) -> error(ctx, 409, "STALE_RECEIPT", e));
        routes.exception(QueueFullException.class, (e, ctx) -> error(ctx, 429, "QUEUE_FULL", e));
        routes.exception(QueueLimitExceededException.class,
                (e, ctx) -> error(ctx, 429, "QUEUE_LIMIT_EXCEEDED", e));
        routes.exception(Exception.class, (e, ctx) -> {
            LOG.error("unhandled error on {} {}", ctx.method(), ctx.path(), e);
            ctx.status(500).json(new ErrorResponse("INTERNAL_ERROR", "internal error"));
        });
    }

    private static void error(Context ctx, int status, String code, Exception e) {
        ctx.status(status).json(new ErrorResponse(code, e.getMessage()));
    }

    /** Parses the JSON body, or returns {@code null} for an empty one; malformed JSON is a 400. */
    private static <T> T body(Context ctx, Class<T> type) {
        String body = ctx.body();
        if (body.isBlank()) {
            return null;
        }
        try {
            return JSON.readValue(body, type);
        } catch (JacksonException e) {
            throw new ValidationException("invalid JSON body: " + e.getOriginalMessage());
        }
    }

    private static int listLimit(Context ctx) {
        String raw = ctx.queryParam("limit");
        if (raw == null) {
            return DEFAULT_LIST_LIMIT;
        }
        try {
            int limit = Integer.parseInt(raw);
            if (limit >= 1 && limit <= DEFAULT_LIST_LIMIT) {
                return limit;
            }
        } catch (NumberFormatException e) {
            // fall through
        }
        throw new ValidationException("limit must be between 1 and " + DEFAULT_LIST_LIMIT);
    }
}
