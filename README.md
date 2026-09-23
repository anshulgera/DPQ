# DPQ: a distributed priority queue service

An in-memory, thread-safe priority queue service in Java 21. Queues have three priorities (HIGH, MEDIUM, LOW), FIFO within a priority, at-least-once delivery with visibility timeouts and receipt handles, retries with a dead-letter queue, per-message TTL, and Prometheus metrics. The engine is a plain Java library; a thin HTTP/JSON layer (Javalin) serves it.

Every significant choice is recorded, with the alternatives considered, in [`decisions.md`](decisions.md) (referenced below as D1, D8d, …). The build was delivered as a stack of reviewable PRs following [`plan.md`](plan.md).

- [Quick start](#quick-start)
- [API](#api)
- [Data model](#data-model)
- [Concurrency model](#concurrency-model)
- [Delivery guarantees](#delivery-guarantees)
- [Metrics](#metrics)
- [Testing](#testing)
- [Scaling, durability and node failure](#scaling-durability-and-node-failure)
- [Limits and memory](#limits-and-memory)
- [Extensions and what I'd do with more time](#extensions-and-what-id-do-with-more-time)

## Quick start

Requirements: any JDK 17+ to run Gradle (Gradle downloads JDK 21 itself if it isn't installed); Docker for the compose setup.

```bash
./gradlew check                 # unit, model-based, HTTP and harness tests
./gradlew :core:stressTest      # concurrency stress suite (about a minute)

./gradlew :server:run           # service on :8080 (or: make run)
```

With Docker: the service, Prometheus scraping it, and the load harness:

```bash
make docker-up                  # dpq on :8080, Prometheus on :9090
make harness                    # 8 producers × 8 consumers, 20k messages, 5% consumer crashes
make docker-down
```

A session with curl:

```bash
curl -X PUT localhost:8080/queues/orders -d '{"visibilityTimeoutSeconds": 30, "maxDeliveries": 5}'
curl -X POST localhost:8080/queues/orders/messages -d '{"payload": "hello", "priority": "HIGH", "ttlSeconds": 3600}'
# {"messageId":"p0-0192…"}
curl -X POST localhost:8080/queues/orders/dequeue
# {"messageId":"p0-0192…","receiptHandle":"9f2c…","payload":"hello","priority":"HIGH","deliveryCount":1,
#  "enqueuedAt":"…","visibleUntil":"…"}
curl -X POST localhost:8080/queues/orders/messages/p0-0192…/ack -d '{"receiptHandle": "9f2c…"}'   # 204
curl localhost:8080/queues/orders/metrics
curl localhost:8080/metrics
```

The server reads `--port=` / `DPQ_PORT` (default 8080) and `--reaper-interval-ms=` / `DPQ_REAPER_INTERVAL_MS` (default 100).

## API

| Method | Path | Body | Responses |
|---|---|---|---|
| PUT | `/queues/{name}` | `{visibilityTimeoutSeconds?, maxDeliveries?, maxDepth?}` | 201 created · 200 same config · 409 different config · 400 · 429 queue cap |
| GET | `/queues/{name}` | – | 200 config · 404. For `{name}.dlq`: 200 `{messages: [...]}` (ready and in flight, `?limit=` 1–100) |
| POST | `/queues/{name}/messages` | `{payload, priority, ttlSeconds?}` | 201 `{messageId}` · 400 · 404 · 429 full |
| POST | `/queues/{name}/dequeue` | – | 200 message · 204 empty · 404 |
| POST | `/queues/{name}/messages/{id}/ack` | `{receiptHandle}` | 204 · 404 gone · 409 stale receipt or lease over |
| GET | `/queues/{name}/metrics` | – | 200 JSON snapshot · 404 |
| GET | `/metrics` | – | Prometheus text exposition |
| GET | `/health` | – | 200 |

Errors are `{"error": "CODE", "message": "…"}` with codes `VALIDATION_ERROR`, `QUEUE_NOT_FOUND`, `MESSAGE_NOT_FOUND`, `QUEUE_CONFIG_CONFLICT`, `STALE_RECEIPT`, `QUEUE_FULL` and `QUEUE_LIMIT_EXCEEDED`.

- **Create is idempotent.** Configs are compared after defaults are applied (visibility 30s, 5 deliveries, depth 10k), so startup scripts on many instances are safe, and a mismatch is a 409 rather than a silent reconfiguration (D11c).
- **Priority is required** on enqueue; there is no silent default. TTL is optional (1s–14d). Payloads are strings of up to 256 KiB of UTF-8; use base64 for binary.
- **Dequeue never blocks** (D10). An empty queue answers 204 and clients back off.
- **The receipt goes in the body**, never the URL, so it stays out of access logs (D8a).
- **Every queue `X` gets a DLQ `X.dlq`** (D9a). Producers can't enqueue into it, but it can be dequeued and acked like any queue. A DLQ delivery carries `deadLetter: {sourceQueue, deliveryCount, reason, deadLetteredAt}`.

## Data model

```
QueueService ── ConcurrentHashMap<String, Queue>          "orders", "orders.dlq", …
                     │
                   Queue ── Partition (exactly one per queue today; the seam for N, D5b)
                               │  ReentrantLock guards everything below
                               ├─ lanes: EnumMap<Priority, Lane>
                               │      Lane = retry min-heap (by seq)  ─served first─▶  FIFO ArrayDeque
                               │             (redelivered messages)                    (never-delivered)
                               ├─ messages:  LinkedHashMap<MessageId, Message>   every live message, arrival order
                               ├─ ready:     Map<MessageId, Lane.Entry>          for in-place TTL tombstoning
                               ├─ inFlight:  Map<MessageId, Lease{receipt, deadline}>
                               ├─ visibilityDeadlines: TreeSet<(at, seq)>        one entry per lease
                               ├─ ttlDeadlines:        TreeSet<(at, seq)>        ready messages with a TTL
                               └─ counters + 60s sliding-window rates (plain longs)
```

Why this shape (D4):

- **Three lanes, not one sorted set.** Enqueue and dequeue are O(1) apart from deadline bookkeeping, FIFO within a priority comes from the structure itself, and the oldest-message age is O(1): the head of each lane.
- **Redelivery keeps the original position.** Under strict FIFO, every message already dequeued has a lower sequence number than every message still in the deque. Serving the retry heap first therefore puts a redelivered message exactly where it was, even when leases expire out of order.
- **Removable deadline sets instead of lazy heaps.** A lazy heap would keep an acked message's TTL entry until its deadline, up to 14 days: at 1k msg/s that's over a billion dead entries, and `maxDepth` wouldn't bound it. The `TreeSet`s hold only live deadlines, at O(log n) each.
- **Tombstones with compaction.** A message whose TTL passes while ready is marked dead in its lane and leaves the counts immediately. Dead entries are discarded when they reach a head, and the lane is compacted once they exceed half its size. Without compaction, a starved LOW lane that's never polled would keep its dead entries forever.
- **Message IDs are `p{partition}-{UUIDv7}`** (D11a): unique without coordination, time-sortable for debugging, and the prefix routes an ack to the owning partition. Receipts are separate random 128-bit tokens. Both generators are injectable, so tests use deterministic ones.

## Concurrency model

- **One `ReentrantLock` per partition** guards all of that partition's state (D5). The lanes, the index, the leases and the deadline sets must change together, and a single lock makes those invariants easy to reason about and to test. Critical sections cost O(log n) per message they touch, and the expiry drain at the start of an operation touches at most K messages. Queues never contend with each other: the registry is a `ConcurrentHashMap`, and the lock boundary is also the future shard boundary.
- **Counters are plain `long`s under that lock.** Every update already holds it, so `LongAdder` would add nothing, and a ring of `LongAdder` buckets would bring a bucket-reset race.
- **Lock order is source → DLQ.** Dead-lettering runs under the source partition's lock and takes the DLQ's lock, so a message is never in neither queue. A DLQ has no DLQ of its own, so the order is acyclic and can't deadlock (D9a).
- **Expiry is lazy plus a reaper** (D6):
  - Every operation first drains up to K = 256 expired leases and TTLs, so correctness never depends on a timer firing. The bound keeps a mass expiry (a whole consumer fleet dying at once) from turning one dequeue into a long critical section.
  - A single reaper thread fully drains every partition every 100ms, so idle queues still redeliver, dead-letter and report accurate metrics.
- **Time comes from an injected `Clock`.** Deadlines use a monotonic reading, so an NTP step can't shorten a lease. Each operation reads the clock **once** and derives any displayed wall-clock time from that same instant (D18j).
- **Rules are judged at the deadline, not at drain time** (D18i). If a lease ended before the message's TTL, the message is redelivered or dead-lettered even when the drain that notices it runs after the TTL. The outcome never depends on reaper timing.

## Delivery guarantees

**At-least-once, within the lifetime of the process.** A message leaves the queue only when a consumer acks it with the receipt of a lease that's still valid, when its TTL passes, or when it moves to the DLQ. A consumer that is slow past its visibility timeout may see the message delivered to someone else. Its late ack then gets 409, so it knows the work may be repeated. Two consumers never hold overlapping valid leases on the same message; the stress suite checks exactly this.

What happens in each situation (D8d):

| Situation | Outcome |
|---|---|
| Lease expires, TTL not passed, deliveries < `maxDeliveries` | Back to its lane at its original position; `redelivered_total++` |
| Lease expires, TTL not passed, deliveries ≥ `maxDeliveries` | Moved to the DLQ, reason `MAX_DELIVERIES` |
| Lease expires after the TTL passed | Dropped as expired (TTL beats DLQ); `expired_total++` |
| A ready message's TTL passes | Dropped as expired, not dead-lettered (D9b) |
| Ack with the current receipt before the lease deadline | 204, removed, even if the TTL has passed meanwhile (the lease wins, D9c) |
| Ack at or after the lease deadline, whether or not it was drained yet | **409**: the lease is over |
| Ack with an older delivery's receipt | 409 |
| Ack of an ID that was acked, dead-lettered, expired or never existed | **404** |

- **Treat 404 on ack as terminal.** A retried ack whose first response was lost gets 404: the message is gone either way. Idempotent acks would need a cache of recent receipts.
- **`maxDeliveries` counts total attempts, including the first** (like SQS `maxReceiveCount`, D8b). In the spec's wording, the redelivery threshold is `maxDeliveries − 1`.
- **Strict priority** (D7): a lower priority is never served while a higher one is ready. That's exactly what "priority ordering holds" asks for, but LOW can starve under sustained HIGH load. `dpq_oldest_message_age_seconds{priority="LOW"}` makes starvation visible, and the choice goes through a `SelectionPolicy` interface, so aging or weighted fairness is a small change.
- **What is lost on a crash:** everything. The state is in memory (D3). Nothing is lost *silently* while the process runs, though: every exit from the queue is an ack, a counted expiry or a counted dead-letter. See [durability](#scaling-durability-and-node-failure) for what production would add.

## Metrics

`GET /metrics` serves Prometheus text; `GET /queues/{name}/metrics` serves the same snapshot as JSON (D12a). Every series carries a `queue` label, and a DLQ is simply another queue (`queue="orders.dlq"`).

| Metric | Type | Extra labels | Answers |
|---|---|---|---|
| `dpq_messages_ready` | gauge | `priority` | Backlog by priority |
| `dpq_messages_in_flight` | gauge | – | Work in progress |
| `dpq_oldest_message_age_seconds` | gauge | `priority` | Consumer lag, and LOW starvation |
| `dpq_messages_enqueued_total` | counter | `priority` | Producer throughput |
| `dpq_messages_delivered_total` | counter | `priority` | Consumer throughput, redeliveries included |
| `dpq_dequeue_empty_total` | counter | – | Wasted polling (consumers over-provisioned) |
| `dpq_messages_acked_total` | counter | – | Completed work |
| `dpq_messages_redelivered_total` | counter | – | Consumers crashing or timing out |
| `dpq_messages_dead_lettered_total` | counter | – | Poison messages |
| `dpq_messages_expired_total` | counter | `priority` | Work that went stale before anyone did it |
| `dpq_enqueue_rejected_total` | counter | – | Back-pressure (queue full) |
| `dpq_operation_duration_seconds` | histogram | `op` | Server-side latency of enqueue, dequeue and ack, against the p95 < 100ms target |

- **Computed when scraped.** Values come from the queues at scrape time (a custom `MultiCollector`), so gauges are never stale, nothing runs between scrapes, and new queues appear without registration. Each queue costs O(1) under its lock: the lane sizes, the lane heads and the counters. The oldest age takes the head of each lane, and a redelivered message keeps its original enqueue time.
- **Counters are cumulative** (`_total`), so Prometheus `rate()` works and sums across instances. For the spec's "messages per second", the JSON view also has `enqueueRatePerSec` and `ackRatePerSec` over the last 60 complete seconds, from a ring of one-second buckets (D12b).
- **Label cardinality is bounded.** There are at most 1,000 user queues per node. A request path naming an unknown queue never becomes a label value.
- **Idle queues lag slightly.** Their metrics can be up to one reaper interval (100ms) old.

## Testing

Tests run at five layers, each aimed at a different kind of bug:

| Layer | Where | What it proves |
|---|---|---|
| Unit tests with a `FakeClock` (no sleeps) | `core`, 146 tests including the property below | Every rule and boundary: deadline − 1ms vs at the deadline, each D8d row, TTL × DLQ precedence, limits, idempotent create, the DLQ, metrics, the sliding window |
| Model-based tests (jqwik, 1,000 random action sequences) | `QueueModelProperties` | The real service matches a deliberately naive `ModelQueue` written from the spec and D8d, step by step: IDs, receipts, deliveries, errors and full metrics snapshots |
| Stress tests, `@Tag("stress")`, 60 runs | `ConservationStressTest`, `PriorityOrderingStressTest` | Under real contention with the reaper and a moving clock: conservation (acked + DLQ + expired = enqueued, exactly once), lease exclusivity, counters equal ground truth, liveness, and no priority inversion (phase and interleaved, via the delivery sequence) |
| HTTP tests (javalin-testtools, random port) | `server`, 17 tests | Every API row, the error codes, the Prometheus exposition format and values, and the histogram |
| Load harness | `harness` | End-to-end over HTTP: throughput, p50/p95/p99, and conservation with crashing consumers |

**The verification layers found real bugs**, each turned into a failing regression test before it was fixed:

1. **Found by the model-based tests** (D18i). Lease expiry was judged at drain time instead of at the lease deadline. A lease that ended before its TTL was dropped as *expired* when the drain ran after the TTL, instead of being redelivered or dead-lettered. jqwik found it within three tries and shrank it to a 7-step reproduction.
2. **Found by the stress test** (D18j). `visibleUntil` came from a second clock reading, so it could promise a lease 1ms longer than the real one.

The priority-ordering stress tests were also checked against a deliberately broken priority policy, to make sure they fail when they should: all 40 runs did.

**The harness:**

```bash
./gradlew :harness:run --args='--producers=8 --consumers=8 --messages=20000 --crash-rate=0.05'   # closed-loop
./gradlew :harness:run --args='--rate=1000 --duration=10'                                          # open-loop
```

- **Open-loop mode schedules requests at a fixed arrival rate** and measures each latency from its scheduled start, so queueing delay counts. A closed-loop harness hides it (coordinated omission).
- **Local results** on an Apple-silicon laptop over loopback:
  - closed-loop, 20k messages with 5% crashes: enqueue p95 1.7ms, dequeue p95 1.1ms;
  - open-loop at 1,000/s: enqueue p95 0.6ms;
  - conservation OK in both runs.

## Scaling, durability and node failure

This build is one process. Here is how the design extends (D15).

**Horizontal scaling.** A queue partition is the unit of state, and nothing has to stay consistent across partitions, so:

- A placement service (an etcd or ZooKeeper-style lease-and-epoch map) assigns partitions to nodes.
- A stateless gateway or smart client routes requests by queue name. An ack is routed by the partition prefix in the message ID, with no lookup. A wrong-owner response triggers a map refresh.
- A hot queue is split into N partitions. Consumers are **assigned** partitions, Kafka-style, rather than the server fanning each dequeue out across nodes.
- Priority stays strict within a partition and becomes approximate across partitions. The spec's "should usually receive the highest-priority message" allows this.
- The code already has the seam: `Queue → Partition`, the partition prefix in IDs, and partition-local locks, reaper work and metrics.

**Durability.** The engine sits behind a store seam, so persistence is an addition rather than a rewrite:

- The **durable** transitions (enqueue, ack, expire, dead-letter) become entries in a write-ahead log per partition, replicated by Raft across three replicas. Group commit keeps p95 under 100ms.
- **Deliveries and leases are deliberately not replicated.** Dequeue stays a local, leader-only operation with no consensus round trip per delivery.

**Node failure.**

- When a leader fails, a follower is elected and rebuilds its state by replaying the log. Every message that was in flight becomes visible again at once.
- At-least-once still holds: consumers of the old leader get 409 when they ack, and the work is redone.
- The price is that `deliveryCount` can undercount after a failover, so a poison message may take a few extra attempts to reach the DLQ, and duplicate deliveries spike briefly.
- Epoch/term fencing rejects a stale leader. The alternative, replicating deliveries, costs a consensus write on every dequeue and needs clock-independent lease deadlines.

**Producer retries.** A producer whose enqueue response is lost will retry and create a duplicate. The extension for this is an optional idempotency key with a deduplication window.

## Limits and memory

- **Back-pressure.** `maxDepth` (default 10k, counting ready and in-flight messages) rejects enqueues with 429 rather than letting one runaway producer exhaust the heap and take down every queue (D11b). DLQs are exempt so dead-lettering never drops a message; `dpq_messages_ready{queue="X.dlq"}` shows their growth.
- **Worst-case memory.** Depth counts messages, not bytes: the worst case is 10k × 256 KiB ≈ 2.5 GiB per queue, across up to 1,000 queues per node. The limits bound the blast radius without guaranteeing against running out of heap; a per-queue `maxBytes` and a node-wide byte budget would close that gap.

## Extensions and what I'd do with more time

**Extensions** that the design already has seams for:

- `changeVisibility` (nack or heartbeat), and DLQ redrive: dequeue from the DLQ, then enqueue into the source (D8c).
- Long-poll dequeue via a `Condition` on the partition lock, and batch dequeue: a loop under the same lock (D10).
- N partitions per queue, and an aging or weighted `SelectionPolicy` against LOW starvation (D5b, D7).
- Delayed delivery: a message released by the reaper into the retry heap, keeping its seq order.
- Numeric priorities: `TreeMap<Integer, Lane>` behind the same policy interface.
- Queue deletion and config updates.

**With more time:**

- Persistence (the WAL above).
- `maxBytes` and a node-wide byte budget.
- Idempotency keys.
- Linearizability checking with Lincheck. Its value is modest with coarse per-partition locks, where the lock is the linearization point.

## How it was built

The design came first:
- the requirements were worked through into a decision log ([`decisions.md`](decisions.md)), with the options considered for each choice;
- then into a plan of 14 stacked PRs ([`plan.md`](plan.md)).

The logic PRs were written test-first. Each shows a `test:` commit, where the new tests fail for the expected reason, before the `feat:` commit that makes them pass. Gaps found during implementation were resolved with the author and recorded as D18a–D18j rather than decided silently. The work was paired with an AI coding assistant (Claude Code) throughout; the verification layers above are what make that trustworthy.
