# Keychain DPQ — Design Decisions

This file records every significant design decision for the Distributed Priority Queue service. Each entry gives the **decision**, the **options considered**, **why** we chose it, and its **consequences**. Decisions are numbered so code, PRs and the README can reference them (e.g. "see D8a").

The requirements are in `Requirements.txt`; the implementation plan is in `plan.md`.

---

## D1 — Language / runtime: Java

- **Decision:** Java.
- **Options considered:** Go, Java/Kotlin, Rust, Python.
- **Why:**
  - It's the language the author is most comfortable in, and the follow-up session involves extending the code live.
  - `java.util.concurrent` offers mature primitives: `ReentrantLock`, `Condition`, `ConcurrentHashMap`, `LongAdder`, `ScheduledExecutorService`.
  - Java 21 adds records and virtual threads.
- **Consequences:**
  - Java has no built-in race detector (unlike Go's `-race`), so thread safety must be shown through deliberate tests (D13).

## D2 — API style and framework: library core + Javalin HTTP/JSON, Java 21, Gradle

- **Decision:** a framework-free Java library (`QueueService`) holds all queue logic. A thin HTTP/JSON layer on **Javalin** (Jackson) wraps it and serves `/metrics`. The build uses Java 21 LTS and Gradle (Kotlin DSL).
- **Options considered:**
  - Network layer: HTTP/JSON, gRPC, or library only.
  - Framework: Spring Boot, Javalin/plain JDK `HttpServer`, Quarkus/Micronaut.
  - Build: Maven/Gradle on Java 17 or 21.
- **Why:**
  - Keeping the engine free of framework code makes it trivially unit- and concurrency-testable.
  - HTTP is curl-able, easy to demo, and the natural transport for a Prometheus scrape.
  - Javalin keeps everything explicit, with no annotation magic, so the code is easy to walk through and extend live.
  - Java 21 gives records and virtual threads; the harness uses virtual threads.
- **Consequences:**
  - Metrics registry wiring, validation and error mapping are written by hand (small).
  - gRPC can be added later as a second adapter over the same core.

## D3 — Storage and durability: in-memory behind a pluggable seam

- **Decision:** all state lives in memory, behind a store/journal seam. Durability through a write-ahead log (WAL) plus replication is described as the production path (D15b), not implemented.
- **Options considered:**
  - Pure in-memory.
  - In-memory plus a WAL file with replay on startup.
  - An embedded database (SQLite/H2).
- **Why:**
  - The grading priorities are correctness, concurrency and data-model clarity.
  - A WAL adds replay, fsync-policy and compaction concerns, roughly two more PRs, without improving what is evaluated first.
  - An embedded database would hand concurrency over to the database and hide the in-memory data structures the reviewers want to see.
  - The seam makes "add persistence" a clean extension.
- **Consequences:**
  - A process crash loses all messages. The README states this plainly and explains what production would add.

## D4 — Core data model: three FIFO lanes per partition

- **Decision:** each partition holds:
  - `EnumMap<Priority, Lane>`, where each `Lane` is a `retry` min-heap (ordered by enqueue sequence), served **before** a main `ArrayDeque`;
  - `messages: Map<MessageId, Message>`, the index of every live message;
  - `inFlight: Map<MessageId, Lease>`, where each lease holds a receipt handle and a deadline;
  - two **removable ordered sets** (`TreeSet<Deadline(at, seq)>`), one for visibility deadlines and one for TTL deadlines. An entry is removed as soon as it stops mattering: the visibility entry on ack, the TTL entry on dequeue (TTL is checked again when the lease expires, per D9c; a redelivered message is re-added). The sets therefore hold only live deadlines.

  Removed lane entries become tombstones that are skipped lazily when they reach a lane head. When a lane's tombstones exceed 50% of its size, the lane is compacted (O(n), amortised). Counts are maintained eagerly.
- **Options considered:**
  - Three FIFO lanes: O(1), fixed set of priorities.
  - A single ordered set by (priority, seq): O(log n), generalises to numeric priorities.
- **Redelivery ordering:** a redelivered message keeps its **original position**; the alternative was the back of its lane.
- **Why:**
  - Enqueue and dequeue are O(1), and FIFO order within a priority is guaranteed by the structure itself.
  - The oldest-message age is the minimum over three lane heads.
  - Serving the retry heap first gives exact original ordering because, with strict FIFO, every message already dequeued has a lower sequence than every message still in the deque. Redelivered messages therefore always belong ahead of the deque, and the heap orders them among themselves even when their leases expire out of order.
  - Keeping the original position preserves FIFO order as closely as at-least-once delivery allows.
- **Why removable sets instead of lazy heaps:** with lazy heaps, an acked message's TTL entry would stay until its deadline, up to 14 days. At 1k msg/s that is over a billion stale entries, and `maxDepth` does not bound it. With removable sets, each deadline costs O(log n) and memory stays proportional to the live messages.
- **Why compaction:** under strict priority (D7), a starved LOW lane is never polled, so its TTL-expired tombstones would never reach the head and memory would grow without bound.
- **Consequences:**
  - Adding a priority level means adding a lane. Numeric priorities would change `EnumMap` into `TreeMap<Integer, Lane>` behind `SelectionPolicy`.
  - A tombstone's memory is freed late (at the head, or at compaction), but the counts are always exact.

### Horizontal-scaling check (after D4)

A queue (later, a queue partition) is the unit of state, and nothing needs to stay consistent across queues. Queues can therefore be sharded across nodes, with a stateless HTTP layer routing requests to the right one.

- The lock boundary equals the shard boundary.
- The reaper and TTL handling are local to each shard.
- There are no distributed locks.

The gap is **durability** (D3), not scale. A single hot queue needs partitions, which trades strict global priority for approximate priority. The spec's "should usually receive the highest-priority message" allows this.

## D5 — Concurrency model: one ReentrantLock per partition

- **Decision:**
  - A `ConcurrentHashMap<String, Queue>` registry holds the queues.
  - One `ReentrantLock` per partition guards **all** of that partition's mutable state.
  - Critical sections are O(1) or O(log n).
  - Counters and the sliding-window buckets are **plain `long` fields updated inside the partition's critical section**, and snapshots read them under the same lock. Since every mutation already holds the lock, `LongAdder` would add nothing, and a ring of `LongAdder` buckets would bring a bucket-rollover reset race.
- **Options considered:**
  - A single global lock.
  - Per-queue locks.
  - Lock-free structures (CAS operations plus skip lists).
  - An actor per queue: one thread owns the state and processes commands from a mailbox.
- **Why:**
  - Several structures (lanes, index, in-flight map, deadline sets, counts) must change together, and one lock makes those invariants trivial to reason about and test.
  - Queues don't contend with each other.
  - The lock boundary is the future shard boundary.
  - Lock-free designs make multi-structure invariants very hard to prove or extend live.
  - Actors add a hop of latency and asynchronous plumbing.
  - A global lock gives a weak story on lock granularity.
- **Consequences:**
  - A single queue's throughput is bounded by one lock. Since critical sections take microseconds, this is far beyond the p95 < 100ms target.
  - Scaling a single queue beyond that requires partitions (D5b).

## D5b — Partitioning: partition-ready, one partition per queue

- **Decision:**
  - A Queue→Partition abstraction exists, with the lock and state living on `Partition`, but every queue has exactly **one** partition.
  - Message IDs include the partition (D11a).
  - Metrics are labelled by queue.
- **Options considered:** implement N partitions now; no partition concept at all.
- **Why:**
  - With one partition, strict priority and FIFO hold, so tests stay simple and exact.
  - The abstraction makes N partitions a contained change, and a good live extension.
- **Consequences:** multi-partition dequeue policy (checking each partition's best head) is covered in the README only.

## D6 — Visibility-timeout and TTL enforcement: lazy drain + background reaper

- **Decision:**
  - Every partition operation first drains expired entries from the visibility and TTL sets, under the lock, up to a **bound of K entries (default 256, configurable)**.
  - In addition, one `ScheduledExecutorService` reaper sweeps all partitions periodically (default ~100ms, configurable) and drains each one completely.
  - Entries are removed from the sets as soon as they stop mattering (D4), so there is no stale-entry filtering.
  - All time comes from an injected `Clock`. Deadlines use a **monotonic** reading (`nanoTime`-based) so an NTP step cannot shorten or extend a lease. Wall-clock time is used only for displayed timestamps (`enqueuedAt`, `deadLetteredAt`). `FakeClock` is thread-safe (`AtomicLong`), so stress tests can advance it from another thread.
  - `ack` checks `lease.deadline > now` itself, so its result never depends on whether a drain has run (D8d).
- **Options considered:**
  - Lazy only.
  - Reaper only.
  - A timer per message (e.g. a `ScheduledExecutor` task or a hashed wheel timer).
- **Why:**
  - Correctness never depends on a timer firing: any operation sees an up-to-date state.
  - The reaper keeps idle queues accurate: in-flight counts, oldest-message age and DLQ moves.
  - Timers per message create many objects, and cancelling a timer races with an ack.
  - The injected clock makes all time behaviour deterministic in tests.
  - The bound on K stops a mass expiry (a whole consumer fleet dying at once) from turning the next dequeue into a long critical section that breaks p95.
- **Consequences:**
  - On an idle queue, metrics can lag by up to one reaper interval.
  - The reaper briefly takes each partition's lock. After a mass expiry, it can hold the lock for one full drain.
  - Under a mass expiry, some redeliveries appear up to one reaper interval late. This affects liveness only: an undrained message stays invisible, so it is never delivered twice.

## D7 — Priority policy: strict, behind a pluggable SelectionPolicy

- **Decision:** strict priority by default, meaning a lower-priority message is never served while a higher-priority one is ready. The choice of lane goes through a `SelectionPolicy` interface.
- **Options considered:**
  - Strict with an opt-in aging mechanism.
  - Weighted fair queuing by default (e.g. 6:3:1).
- **Why:**
  - The graders check that "priority ordering holds" and list "priority inversion" as a hard case, and strict priority is exactly what they test.
  - The interface makes a weighted or aging policy a small extension.
- **Consequences:**
  - LOW messages can starve under sustained HIGH load. The README documents this along with the mitigations (aging, weighted policies).

## D8a — Ack identity: message ID + receipt handle

- **Decision:**
  - Each delivery creates a fresh receipt handle, a random token.
  - `ack(id, receipt)` succeeds only if that lease is still current.
  - A stale receipt → 409; an unknown ID → 404.
- **Options considered:** message ID only, as in the spec's wording.
- **Why:**
  - It closes a race. Consumer A's lease expires, consumer B receives the same message, then A's late ack arrives. With ID-only acks, A would delete a message B is still processing.
  - It follows the SQS model, and it makes the race explicit and testable.
- **Consequences:**
  - Callers must send both the ID and the receipt when acking.
  - The receipt goes in the request body, not the URL, so it stays out of access logs.

## D8b — Retry counting: maxDeliveries = total attempts

- **Decision:**
  - The per-queue `maxDeliveries` setting counts total attempts, including the first (like SQS `maxReceiveCount`).
  - `deliveryCount` increments on every dequeue.
  - When a lease expires and `deliveryCount >= maxDeliveries`, the message moves to the DLQ **at that moment**.
- **Options considered:** `retryLimit` = redeliveries allowed after the first attempt.
- **Why:**
  - It matches the SQS semantics most engineers already know.
  - Moving the message at expiry time, not at the next dequeue, keeps the metrics honest and moves messages even on idle queues.
- **Consequences:**
  - The README maps this onto the spec's wording ("redelivery threshold") as redeliveries = `maxDeliveries − 1`.

## D8c — Extra consumer operations: none

- **Decision:** only the five operations in the spec. The only failure path is the visibility timeout.
- **Options considered:**
  - An SQS-style `changeVisibility` (0 = nack, N = backoff or heartbeat).
  - Separate nack and extend endpoints.
  - An admin DLQ redrive.
- **Why:**
  - The spec says "correctness over completeness".
  - Every extra operation adds API surface to test and defend.
  - The lease model makes each of these a small extension.
- **Consequences:**
  - Retry and DLQ tests must go through timeout expiry using `FakeClock`.
  - The README describes `changeVisibility` and redrive as extensions.

## D8d — Rule precedence and ack outcomes

- **Decision:** this table is the single source of truth. The implementation (PR 5) and the reference `ModelQueue` (PR 8) each encode it, and every row is a test.

  | Situation | Outcome |
  |---|---|
  | Lease expires, TTL not passed, `deliveryCount < maxDeliveries` | Back to the lane's retry heap (original seq); `redelivered_total++` |
  | Lease expires, TTL not passed, `deliveryCount >= maxDeliveries` | Moved to the DLQ, reason `MAX_DELIVERIES` |
  | Lease expires, TTL passed (whatever `deliveryCount` is) | **Dropped as expired**: TTL beats DLQ; `expired_total++` |
  | Ready message's TTL passes | Dropped as expired (D9b) |
  | Ack with the current receipt, `now < lease.deadline` (TTL passed or not) | 204, removed; `acked_total++` (D9c) |
  | Ack with the current receipt, `now >= lease.deadline` (drained or not) | **409** stale: the lease is over |
  | Ack with an old receipt from an earlier delivery | 409 stale |
  | Ack of an ID that was already acked, dead-lettered, expired, or never existed | **404** |

- **Why:**
  - TTL beats DLQ because a producer who said "useless after X" doesn't want it kept, and the DLQ stays a pure signal of processing failure (D9b).
  - A late ack gets 409 because another consumer may be about to receive the message. Accepting the ack would reopen the race D8a closes.
  - A retried ack (response lost) gets 404 instead of success. Idempotent acks would need a cache of recent receipts. The README tells clients to treat **404 on ack as terminal**: the message is gone either way.
- **Consequences:** clients can't tell "I already acked this" apart from "it was dead-lettered or expired". The metrics show which one happened.

## D9a — Dead-letter queue: auto-created companion queue

- **Decision:**
  - `createQueue(X)` also creates `X.dlq`: a normal queue (same data structures; dequeue and ack work) with no DLQ of its own, unlimited deliveries, and no `maxDepth` limit, so it never drops a message.
  - The `.dlq` suffix is reserved. User queue names match `[A-Za-z0-9_-]{1,80}`, which has no `.`, so a user name can never collide with a DLQ name. Name lookups accept `{valid name}.dlq` as the one internal form.
  - Producers can't enqueue into a DLQ directly (400); only dead-lettering adds to it. Dequeue, ack and metrics work on it as normal.
  - `X.dlq` is created and registered **before** `X` is published in the registry, so a drain can never find its DLQ missing.
  - A dead-lettered message keeps its payload and priority, gains `sourceQueue`, `deliveryCount`, `reason` and `deadLetteredAt`, and has its TTL cleared.
  - The move acquires locks in the fixed order source → DLQ.
- **Options considered:**
  - An internal holding list that can only be inspected.
  - A user-specified DLQ target (SQS-style `RedrivePolicy`).
- **Why:**
  - Reusing the queue implementation gives DLQ processing, metrics and a future redrive (just dequeue, then enqueue) for free.
  - The lock order can't deadlock, because a DLQ never has its own DLQ.
  - Holding both locks means there is never a moment when the message is in neither queue.
  - Clearing the TTL keeps evidence of the failure.
- **Consequences:**
  - Queues can't share a DLQ.
  - An unbounded DLQ can grow; the `dpq_messages_ready{queue="X.dlq"}` gauge makes that visible.

## D9b — TTL expiry of ready messages: drop and count

- **Decision:** a ready message whose TTL passes is removed and counted in `dpq_messages_expired_total{queue,priority}` (logged at debug level). It is not a DLQ event.
- **Options considered:** move it to the DLQ with reason `EXPIRED`.
- **Why:**
  - A TTL expresses the producer's intent ("not useful after X"), not a processing failure.
  - Sending expired messages to the DLQ would mix stale messages with poison ones and inflate the DLQ-failure metric the spec asks for.
  - The counter means nothing is lost *silently*.
- **Consequences:** the payload of an expired message can't be recovered.

## D9c — TTL passing while a message is in flight: the lease wins

- **Decision:**
  - The consumer holding the lease can still ack successfully.
  - If the lease expires after the TTL has passed, the message is dropped as expired rather than redelivered or dead-lettered.
- **Options considered:** expire the message immediately, so that a later ack gets 404.
- **Why:**
  - The consumer may already have done the work, and rejecting its ack would be confusing and wasteful.
  - A message past its TTL should never be delivered again.
- **Consequences:** a message can finish processing slightly after its TTL.

## D10 — Dequeue shape: non-blocking, single message

- **Decision:** dequeue returns immediately. An empty queue gives HTTP 204 / `Optional.empty()`. Each call returns one message.
- **Options considered:**
  - Optional long-poll (`waitSeconds`, using a lock `Condition`).
  - Batch `maxMessages`.
- **Why:** it's the smallest correct surface that matches the spec. Long-poll and batch are clean extensions: the `ReentrantLock` already supports `Condition`, and a batch is a loop under the same lock.
- **Consequences:** harness consumers back off with jitter when the queue is empty.

## D11a — Message IDs: partition prefix + UUIDv7

- **Decision:**
  - Message IDs have the form `p{partition}-{UUIDv7}`, made by a small in-house generator.
  - Receipt handles are separate random 128-bit tokens.
- **Options considered:**
  - Partition prefix + sequence number.
  - Plain UUIDv4.
- **Why:**
  - Unique across nodes and restarts with no coordination.
  - Time-sortable, which helps debugging.
  - Opaque to callers.
  - The prefix lets an ack be routed to the partition that owns the message without a lookup (D15a).
  - Sequence numbers are only unique within a process and reveal volume; UUIDv4 carries no routing information.
- **Consequences:**
  - Java has no built-in UUIDv7, so we maintain about 15 lines of generator code.
  - `IdGenerator` and `ReceiptGenerator` are injected interfaces, so model-based tests (D13) can use deterministic generators and compare IDs directly.

## D11b — Limits and backpressure: bounded per queue

- **Decision:**
  - Per-queue `maxDepth` (ready + in-flight, **default 10k**). A full queue rejects enqueue with 429 / `QueueFullException`.
  - Payload ≤ 256 KB (a UTF-8 string treated as opaque; base64 for binary data).
  - TTL between 1s and 14d.
  - Visibility timeout between 1s and **12h**; `maxDeliveries` between 1 and 1000.
  - Queue names match `[A-Za-z0-9_-]{1,80}`.
  - At most 1,000 user queues per node (configurable), which also caps the `queue` label's cardinality.
- **Options considered:** unbounded queues.
- **Why:**
  - An unbounded queue lets one runaway producer run the node out of heap memory, taking down *every* queue — the opposite of "messages should not be silently lost".
  - Explicit rejection pushes back on producers, which retry with backoff.
- **Consequences:**
  - Producers must handle 429.
  - `maxDepth` counts messages, not bytes. The worst case is 10k × 256 KB ≈ 2.5 GB per queue, times the number of queues, so it limits the blast radius but doesn't guarantee against running out of heap. The README states this. A per-queue `maxBytes` and a node-wide byte budget are listed under "with more time".

## D11c — createQueue semantics and defaults

- **Decision:**
  - `createQueue` is idempotent: the same config → 200, a different config → 409. Configs are compared **after defaults are applied**, so leaving a field out equals sending its default.
  - Creation is atomic via `computeIfAbsent`, with the DLQ registered first (D9a).
  - Defaults: visibility timeout 30s, `maxDeliveries` 5, `maxDepth` 10k.
  - Priority is required on enqueue; TTL is optional (absent = never expires).
  - An unknown queue → 404; invalid input → 400.
- **Options considered:** strict create (always 409 on a repeat) with priority defaulting to MEDIUM.
- **Why:**
  - Idempotent create is safe for startup scripts run by many instances.
  - A 409 on a config mismatch avoids silent reconfiguration.
  - Requiring priority avoids a silent default that producers never chose.
- **Consequences:** changing a queue's config requires an explicit update API (a future extension).

## D12a — Metrics exposition: Prometheus /metrics + JSON

- **Decision:**
  - `GET /metrics` serves the Prometheus text format, using prometheus client_java 1.x with a custom collector that reads partition state **at scrape time** (no background refresh).
  - `GET /queues/{name}/metrics` serves JSON for the spec's "Get Metrics" operation.
  - Both are built from one `QueueMetricsSnapshot`.
- **Metrics** (all labelled `queue`):

  | Metric | Type | Extra labels |
  |---|---|---|
  | `dpq_messages_ready` | gauge | `priority` |
  | `dpq_messages_in_flight` | gauge | – |
  | `dpq_oldest_message_age_seconds` | gauge | `priority` |
  | `dpq_messages_enqueued_total` | counter | `priority` |
  | `dpq_messages_delivered_total` | counter | `priority` |
  | `dpq_dequeue_empty_total` | counter | – |
  | `dpq_messages_acked_total` | counter | – |
  | `dpq_messages_redelivered_total` | counter | – |
  | `dpq_messages_dead_lettered_total` | counter | – |
  | `dpq_messages_expired_total` | counter | `priority` |
  | `dpq_enqueue_rejected_total` | counter | – |
  | `dpq_operation_duration_seconds` | histogram | `op` (enqueue, dequeue, ack); recorded in the HTTP layer |

  - Oldest age **by priority** shows LOW starvation under strict priority (D7). The spec's per-queue value is the maximum across priorities, which the JSON endpoint reports as well.
  - `delivered_total` gives consumer throughput; `dequeue_empty_total` shows wasted polling (D10).
  - The duration histogram makes the p95 < 100ms target observable in production, not just in the harness.

- **Options considered:**
  - Micrometer with a Prometheus registry.
  - Writing the exposition format by hand.
- **Why:**
  - Reading at scrape time means gauges are never stale and cost nothing between scrapes.
  - A collector handles queues created at runtime more cleanly than registering a gauge for every label combination.
  - The official client handles format details such as escaping and TYPE/HELP lines.
  - The oldest-message age is O(1): the minimum `enqueuedAt` over the lane heads, after expired heads are purged; redelivered messages keep their original enqueue time.
- **Consequences:** each scrape briefly takes each partition's lock (O(1) work per partition).

## D12b — Throughput: counters + sliding window

- **Decision:**
  - Monotonic `_total` counters, for Prometheus `rate()`.
  - A per-queue 60s sliding window (a ring of 1s `long` buckets on the injected clock, updated under the partition lock per D5) that feeds `enqueueRatePerSec` and `ackRatePerSec` in the JSON output. The window covers the 60 complete seconds before the current one.
- **Options considered:**
  - Counters only.
  - Exponentially weighted moving averages (EWMA).
- **Why:**
  - Counters are the idiomatic Prometheus approach: they hold up across restarts and can be summed across instances.
  - The window lets the service answer "messages per second" on its own.
  - EWMA values are pre-smoothed, which misleads Prometheus and is harder to test exactly.
- **Consequences:** a small fixed amount of memory per queue (60 buckets).

## D13 — Testing strategy

- **Decision:**
  - **Baseline:** JUnit 5 + AssertJ, with a `FakeClock` for deterministic rule tests (no sleeps); javalin-testtools for HTTP; JaCoCo coverage.
  - **Layer (a) — invariant stress tests** (`@Tag("stress")`, repeated runs). N producers × M consumers start together on a latch, some consumers abandon messages at random, and a thread advances the clock. The tests assert:
    - **conservation:** enqueued = acked + dead-lettered + expired;
    - **lease exclusivity:** no two consumers hold overlapping valid leases on the same message;
    - counters equal the ground truth;
    - **liveness:** the run finishes in time;
    - no worker exceptions.
  - **Layer (a2) — concurrent ordering tests** (the "priority inversion" hard case). The partition stamps a monotonic **delivery sequence** under the lock, visible to tests.
    - *Phase test:* preload a mix of priorities, drain with many consumers at once, and assert that the delivered order (by delivery seq) never goes up in priority, and is FIFO within a priority.
    - *Interleaved test:* producers and consumers run at the same time. Assert that no LOW was delivered at a delivery seq where a HIGH whose enqueue had already returned was still ready. Also assert per-producer FIFO within a priority.
  - **Layer (b) — jqwik model-based tests.** Random sequences of operations run against both the real queue and a naive reference `ModelQueue`, whose results must match at every step. Both use deterministic ID and receipt generators (D11a). The model encodes the D8d table, written from the spec independently of the implementation. Failing sequences are shrunk and kept as regression tests.
  - **Harness latency** runs in an open-loop mode at a fixed arrival rate as well as closed-loop, so the p95 claim includes queueing delay (no coordinated omission).
- **Options considered:** Lincheck (linearizability checking), jcstress (Java memory model testing).
- **Why:**
  - The model-based tests prove the **logic**: priority inversion, FIFO after redelivery, and the interplay of timeout, TTL and DLQ — exactly the hard cases the graders list.
  - The stress tests prove the **locking** under real contention.
  - Lincheck adds little on top of coarse per-partition locks, where the lock is the linearization point.
  - jcstress targets low-level primitives, which this design doesn't have.
- **Consequences:**
  - Stress tests run as a separate Gradle task and CI job.
  - Lincheck is listed under "with more time".

## D14 — Layout, harness, packaging

- **D14a:** a Gradle multi-module build:
  - `core`: the pure engine and its unit, model-based and stress tests;
  - `server`: Javalin, Jackson, Prometheus and `Main`;
  - `harness`: the producer/consumer stubs.

  The build enforces the dependency direction.
- **D14b:** a CLI load harness (virtual threads over HTTP):
  - Flags: producer and consumer counts, message count, crash rate, duration.
  - Reports throughput and p50/p95/p99 latency, and checks p95 < 100ms and conservation.
  - Also includes an in-process demo test of the enqueue → dequeue → ack flow.
- **D14c:** packaging and CI:
  - A multi-stage Dockerfile.
  - docker-compose with the service + Prometheus (scrape pre-configured) + an optional harness profile.
  - GitHub Actions on every PR (build, unit + model-based tests, stress tests, JaCoCo).
  - A Makefile.
- **Options considered:**
  - A single module with packages.
  - Simple curl scripts.
  - Docker without Prometheus, or running locally only.
- **Why:**
  - Separate modules keep the engine clean and map neatly onto PRs.
  - The harness demonstrates both concurrency and the latency target.
  - Compose with Prometheus lets reviewers see the metrics live with one command.

## D15 — Scaling, durability and node failure (README stories)

- **D15a — placement and routing:**
  - A placement service (etcd or ZooKeeper style) maps each partition to its owner node using leases and epochs.
  - A stateless gateway or smart client caches that map and routes by queue name plus the partition prefix in the message ID.
  - A wrong-owner response triggers a refresh.
  - Hot partitions are rebalanced deliberately.
  - *Considered:* consistent hashing only. It's simpler, but gives no control over hot spots and still needs a membership layer.
- **D15b — durability and failure:**
  - The **durable** transitions (enqueue, ack, expire, dead-letter) become entries in a write-ahead log, replicated by Raft per partition across 3 replicas. Group commit keeps p95 < 100ms.
  - **Deliveries and leases are not replicated.** Dequeue stays a leader-local operation with no consensus round trip.
  - When a leader is lost, a follower is elected and rebuilds state by replaying the log. Every message that was in flight becomes visible again at once. At-least-once holds; consumers of the old leader get 409 on ack and the work is redone.
  - Epoch/term fencing rejects stale leaders.
  - *Considered:* replicating deliveries as well. That keeps `deliveryCount` and leases exact across failover, but it costs a consensus write on every dequeue and needs lease deadlines carried as durations plus term, because replicas' clocks are not synchronised.
  - *Also considered:* synchronous primary-backup replication (split-brain risk without fencing), or handing storage to Postgres, FoundationDB or Redis Streams.
- **Trade-off stated in the README:** after a failover, `deliveryCount` can undercount, so a poison message may take a few extra attempts to reach the DLQ. Duplicate delivery goes up during failover, which at-least-once already permits.
- **D15c — README additions:**
  - Producer retries can create duplicate messages (the enqueue response is lost and the producer tries again). Extension: an optional idempotency key with a deduplication window.
  - With N partitions on different nodes, consumers are **assigned partitions** (Kafka-style, through the placement service) rather than the server fanning each dequeue out across nodes. Priority is strict within a partition and approximate across partitions.
- **Why:**
  - These are the proven designs behind Kafka, SQS-style services and etcd.
  - D3's seam and D11a's IDs are the extension points.
- **Consequences:** the README must state clearly what the current in-memory build guarantees: at-least-once delivery within a process lifetime, and loss on a crash.

## D16 — Delivery workflow: stacked PRs

- **Decision:**
  - One PR per phase in `plan.md`, stacked, each ≤ ~400 changed lines and including its tests.
  - The agent opens the whole stack; the user reviews it bottom-up.
  - The user squash-merges each PR; the agent then rebases the rest of the stack onto the new `main` with one `git rebase --update-refs` and runs `gh pr edit --base` for the next PR.
  - CI triggers on `pull_request` for every base branch, not only `main`, so stacked PRs get CI too.
  - `CLAUDE.md` in the repo and a PR description template.
- **Options considered:** merge commits, rebase-merge, one PR at a time, batches of about 3.
- **Why:**
  - Small PRs are easy to review.
  - Squash-merge gives a clean main history.
  - Opening the whole stack is fastest overall.
- **Consequences:** the agent must follow the rebase runbook in `plan.md` after every merge.

## D17 — Red-green-refactor TDD for logic PRs

- **Decision:**
  - Logic PRs (2–7 and 10–11) follow red → green → refactor, with commit order `test:` → `feat:` → optional `refactor:`.
  - The verification PRs (8–9) turn any bug they find into a failing test before fixing it.
  - Scaffold, harness, packaging and README PRs are exempt.
  - The rule lives in `CLAUDE.md`; each PR's concrete test cases are listed in `plan.md`.
- **Why:**
  - The rules are precise and time-based, and with `FakeClock` each one is easy to state as a failing test first.
  - The separate commits keep the red → green order visible on each PR, even though squash-merge collapses them on `main`.
- **Consequences:** slightly more commits per PR, and a clear review checklist.

## D18 — Execution-time clarifications (2026-09-23)

These gaps came up when the execution session started. The author resolved them, and they take precedence over the earlier entries they refine.

- **D18a — JDK provisioning (refines D1):** the build uses a Gradle Java 21 toolchain, with the `org.gradle.toolchains.foojay-resolver-convention` plugin in `settings.gradle.kts` to download JDK 21 when it isn't installed. This works the same on developer machines and in CI.
- **D18b — DLQ configuration (refines D9a, D11c):**
  - A DLQ has a **fixed** internal config: 30s visibility timeout, unlimited deliveries, no `maxDepth`. It never derives anything from its source queue's config.
  - The DLQ is marked by a separate **internal flag**, not by out-of-range `QueueConfig` values, so user-facing validation stays strict and a DLQ can't be created through the public path.
  - Because the DLQ config is fixed, registering `X.dlq` with `putIfAbsent` before `computeIfAbsent(X)` is idempotent and race-free, even when concurrent creates of `X` use different configs.
  - `GET /queues/X.dlq` returns the **messages in the DLQ** (a read-only view that creates no lease and doesn't change visibility), not a config. It lists both ready and in-flight messages, each with a `state` field (`READY` / `IN_FLIGHT`), capped by a `limit` query parameter (default and maximum 100). There is no cursor paging; a DLQ larger than the limit is drained by dequeuing.
- **D18c — Dead-lettered message identity (refines D9a):**
  - A dead-lettered message **keeps its original message ID**.
  - Its `enqueuedAt` is set to the **dead-letter time**, so the DLQ's oldest-message age measures how long failures have been waiting there.
  - Dequeuing from a DLQ returns the dead-letter metadata (`sourceQueue`, source `deliveryCount`, `reason`, `deadLetteredAt`) in the response.
- **D18d — Two enqueue timestamps (refines D6, D12a):** each message stores a monotonic `enqueuedAtMono`, used to compute oldest age, and a wall-clock `enqueuedAt`, used only for display. An NTP step therefore can't make an age negative.
- **D18e — Lane tombstones (refines D4, plan PR 3):** `Entry` carries a mutable `dead` flag. `Lane.markDead(entry)` sets it and increments the tombstone count, and `poll()` / `peekOldest()` skip dead entries without an outside predicate. Compaction removes the flagged entries once they exceed 50% of the lane.
- **D18f — Smaller defaults:**
  - `FakeClock`'s "now" means `monotonicMillis()`, and `advance` moves monotonic and wall time together.
  - Payload-size, TTL and config validation live in `core` (`ValidationException`). The HTTP layer maps them to 400 and adds only transport checks (JSON shape, a missing priority).
  - The 1,000-user-queue cap is enforced with an atomic reservation, so concurrent creates of different queues can't go over it. DLQs don't count toward the cap.
  - Awaitility is a test dependency, used only for the real-scheduler reaper test (PR 6).
- **D18g — PR 5 split (refines D16):** PR 5 is split into **5a** (visibility timeout, redelivery, the DLQ sink, the drain limit) and **5b** (TTL) to stay near the ~400-line PR budget.
- **D18h — Queue-cap rejection (refines D11b):** creating a user queue beyond the per-node cap throws `QueueLimitExceededException`, which the HTTP layer maps to **429** (a resource limit, like `maxDepth`). Repeating the create of an existing queue still returns 200 at the cap.
- **D18i — Expiry is judged at the deadline, not at drain time (refines D6, D8d):** the D8d lease-expiry rows are evaluated at the lease deadline, so the outcome doesn't depend on when a drain or the reaper happens to run. A lease that ended before the TTL is redelivered or dead-lettered even if the drain runs after the TTL. A dead-lettered message's `deadLetteredAt` (and its DLQ `enqueuedAt` and age) is the lease deadline. Found by the model-based tests (PR 8).
- **D18j — One clock reading per operation (refines D6):** each partition operation reads the monotonic clock once and uses that instant throughout. Displayed times (`enqueuedAt`, `visibleUntil`, `deadLetteredAt`) come from `Clock.wallTimeAt(monotonicInstant)` rather than a second wall-clock read, so `visibleUntil` is exactly when the lease ends. Found by the conservation stress test (PR 9).
