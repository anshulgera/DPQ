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
import dpq.server.Dtos.QueueResponse;
import io.javalin.Javalin;
import io.javalin.config.RoutesConfig;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import io.javalin.json.JavalinJackson;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The HTTP/JSON API over a {@link QueueService} (D2, plan.md §5). Thin: all queue logic lives in core. */
public final class DpqApp {

    private static final Logger LOG = LoggerFactory.getLogger(DpqApp.class);
    private static final int DEFAULT_LIST_LIMIT = 100;
    // Room for a 256 KiB payload even if every character is JSON-escaped; core enforces the real limit.
    private static final long MAX_REQUEST_BYTES = 2 * 1024 * 1024;

    static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);

    private DpqApp() {}

    public static Javalin create(QueueService service, java.util.function.LongSupplier nanoTicker) {
        return create(service);
    }

    public static Javalin create(QueueService service) {
        return Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson(JSON, false));
            config.http.maxRequestSize = MAX_REQUEST_BYTES;
            routes(config.routes, service);
            errors(config.routes);
        });
    }

    private static void routes(RoutesConfig routes, QueueService service) {
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

        routes.post("/queues/{name}/messages", ctx -> {
            EnqueueRequest req = body(ctx, EnqueueRequest.class);
            if (req == null) {
                throw new ValidationException("a JSON body with payload and priority is required");
            }
            Duration ttl = req.ttlSeconds() == null ? null : Duration.ofSeconds(req.ttlSeconds());
            MessageId id = service.enqueue(ctx.pathParam("name"), req.payload(), req.priority(), ttl);
            ctx.status(HttpStatus.CREATED).json(new EnqueueResponse(id.toString()));
        });

        routes.post("/queues/{name}/dequeue", ctx -> service.dequeue(ctx.pathParam("name")).ifPresentOrElse(
                m -> ctx.json(DequeueResponse.of(m)),
                () -> ctx.status(HttpStatus.NO_CONTENT)));

        routes.post("/queues/{name}/messages/{id}/ack", ctx -> {
            AckRequest req = body(ctx, AckRequest.class);
            if (req == null || req.receiptHandle() == null || req.receiptHandle().isBlank()) {
                throw new ValidationException("receiptHandle is required");
            }
            service.ack(ctx.pathParam("name"), MessageId.parse(ctx.pathParam("id")),
                    new ReceiptHandle(req.receiptHandle()));
            ctx.status(HttpStatus.NO_CONTENT);
        });
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
