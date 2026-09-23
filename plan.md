# Keychain DPQ — Implementation Plan (Stacked PRs)

This plan is written for a **fresh execution session**. Read this file, `Requirements.txt` and `decisions.md` (when present) before starting. Build the service as a stack of reviewable PRs, one per phase below.

## 1. Goal

A thread-safe, in-memory priority queue service in Java: create queue, enqueue, dequeue, ack, visibility timeout, retry → DLQ, TTL and Prometheus-style metrics. It comes with concurrency tests, a producer/consumer harness, Docker packaging and a README explaining the design and trade-offs.

## 2. Decisions summary (full reasoning lives in decisions.md)

| # | Decision |
|---|---|
| D1 | **Java 21**, Gradle Kotlin DSL |
| D2 | Framework-free library core + thin **Javalin** HTTP/JSON layer (Jackson) |
| D3 | In-memory, behind a pluggable store/journal seam; WAL + replication only in the README |
| D4 | Per partition: `EnumMap<Priority, Lane>`; each `Lane` = `retry` min-heap (by seq) served before the main `ArrayDeque`; `messages` id index; `inFlight` lease map; **removable `TreeSet`s** for visibility and TTL deadlines (removed on ack / dequeue, so no leaks); tombstones skipped lazily, lane compacted when tombstones exceed 50%; counts maintained eagerly |
| D5 | `ConcurrentHashMap` queue registry; **one `ReentrantLock` per partition** guards all its state; counters and window buckets are **plain longs under that lock** |
| D5b | Queue→Partition abstraction exists, but always **1 partition** in code |
| D6 | **Hybrid expiry:** every partition op drains **≤ K (256)** expired entries under the lock first; plus one `ScheduledExecutorService` reaper that drains all partitions fully (~100ms, configurable). Injected `Clock` everywhere: monotonic for deadlines, wall time for display. Thread-safe `FakeClock` |
| D7 | **Strict priority** behind a `SelectionPolicy` interface |
| D8a | Ack = message ID + **receipt handle** (new random token per delivery). Stale receipt → 409, unknown id → 404 |
| D8b | `maxDeliveries` = total attempts including the first. `deliveryCount++` on dequeue; on lease expiry, if `deliveryCount >= maxDeliveries` → DLQ immediately |
| D8c | No extra consumer ops (no nack / changeVisibility / redrive) — README extensions only |
| D8d | Precedence table: at lease expiry **TTL beats DLQ**; ack at/after lease deadline → 409 (checked directly, whether or not a drain ran); ack of an acked/DLQ'd/expired/unknown ID → 404 (clients treat 404 on ack as terminal) |
| D9a | `createQueue(X)` auto-creates `X.dlq` (registered before `X`): a normal queue with no DLQ of its own, unlimited deliveries, ignores `maxDepth`, no direct enqueue by producers (400). `.dlq` suffix reserved; user names can't contain `.`. DLQ message keeps payload + priority, adds `sourceQueue, deliveryCount, reason, deadLetteredAt`; TTL cleared. Lock order: source → DLQ (acyclic) |
| D9b | TTL expiry while ready → drop + `dpq_messages_expired_total`; not a DLQ event |
| D9c | TTL passes while in flight → the lease wins (ack still succeeds); if the lease then expires → drop as expired |
| D10 | Non-blocking dequeue (empty → 204), single message |
| D11a | Message ID `p{partition}-{UUIDv7}` (in-house generator); receipt = random 128-bit token; `IdGenerator` / `ReceiptGenerator` injectable for deterministic tests |
| D11b | `maxDepth` (ready + in-flight, default **10k**) → 429; payload ≤ 256 KB UTF-8; TTL 1s–14d; visibility 1s–12h; maxDeliveries 1–1000; queue name `[A-Za-z0-9_-]{1,80}`; ≤ 1,000 user queues per node. Byte budget = README "with more time" |
| D11c | Idempotent create (same **resolved** config → 200, different → 409), atomic via `computeIfAbsent`. Defaults: visibility 30s, maxDeliveries 5, maxDepth 10k. Priority required, TTL optional |
| D12a | `GET /metrics` Prometheus text (client_java 1.x custom collector, computed at scrape time) + `GET /queues/{name}/metrics` JSON, both from one `QueueMetricsSnapshot`. Adds delivered/empty-dequeue counters, oldest age by priority, op-duration histogram |
| D12b | `_total` counters + a 60s sliding-window rate (ring of 1s `long` buckets, under the partition lock) for JSON |
| D13 | JUnit 5 + AssertJ + `FakeClock`, javalin-testtools, JaCoCo; **invariant stress tests** + **concurrent ordering tests** (phase + interleaved, via delivery seq) + **jqwik model-based tests** (deterministic IDs, independent D8d model); open-loop harness latency |
| D14 | Modules `core`, `server`, `harness`; CLI load harness; Dockerfile + compose (service + Prometheus); GitHub Actions; Makefile |
| D15 | README stories: placement service + consumer partition assignment; WAL + Raft per partition for enqueue/ack/expire/DLQ only (**leases not replicated**); epoch fencing; idempotency key as an extension |
| D16 | Stacked PRs, squash-merge, agent restacks with `--update-refs`; CI on every PR base; ≤ ~400 changed lines per PR |
| D17 | Red-green-refactor TDD for logic PRs |
| D18 | Execution clarifications: foojay JDK 21 provisioning; fixed DLQ config + internal DLQ flag; `GET /queues/X.dlq` lists DLQ messages; DLQ keeps message ID, `enqueuedAt` = dead-letter time; monotonic + wall `enqueuedAt`; `dead` flag on lane entries |

## 3. Prerequisites

**Already done:** `git init` (branch `main`) and `origin` = `https://github.com/anshulgera/DPQ.git`.

**User (before execution):**
- Make sure the GitHub repo `anshulgera/DPQ` exists and is empty.
- Run `gh auth login`.

**Agent (first step):**
- Commit `Requirements.txt`, `decisions.md`, `plan.md` as "chore: initial requirements, decisions and plan".
- Push `main`.

## 4. Conventions (PR 1 copies these into `CLAUDE.md`)

### Branches & PRs
- Branch per PR: `pr/NN-slug` (e.g. `pr/03-lane`).
- PR N's base branch = PR N-1's branch; PR 1's base is `main`.
- Every PR must:
  - compile and pass `./gradlew check`;
  - include tests with the code;
  - stay at **≤ ~400 changed lines**, excluding generated files;
  - use the PR template: **What** / **Why** (with D# refs) / **How to test** / **Next in stack**.
- Workflow: build and open the **whole stack** in order, each PR green in CI, then iterate on review feedback bottom-up.
- PR bodies end with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

### Restack runbook (after the user squash-merges PR N)
```bash
git fetch origin
# Replay everything above pr/NN onto the new main; --update-refs moves every
# intermediate pr/* branch along with the top branch (git ≥ 2.38).
git rebase --update-refs --onto origin/main pr/NN pr/<top>
git push --force-with-lease origin $(git for-each-ref --format='%(refname:short)' 'refs/heads/pr/*')
gh pr edit <N+1 number> --base main
```
**Review fixes:** commit on the owning branch, then `git rebase --update-refs <owning branch> pr/<top>` and force-push with lease.

CI (`.github/workflows/ci.yml`) triggers on `pull_request` **without** a `branches: [main]` filter, so stacked PRs are tested too.

### TDD (D17)
- **Applies to PRs 2–7 and 10–11.** For each test listed under the PR:
  - **Red:** write the test and run it. It must fail *for the expected reason* (an assertion failure, not a compile error in unrelated code).
  - **Green:** write the minimum code to make it pass.
  - **Refactor:** tidy up with all tests green.
- **Commit order inside a PR:** `test: …` (red) → `feat: …` (green) → optional `refactor: …`. Push only when green.
- **PRs 8–9 verify existing code.** Any bug they find becomes a failing unit test first, then the fix.
- **PRs 1, 12–14 are exempt.**

### Code style
- Keep `core` free of HTTP, JSON and metrics-library dependencies.
- Never call `System.currentTimeMillis()`, `System.nanoTime()` or `Instant.now()` directly — use the injected `Clock` (`monotonicMillis()` for deadlines, `wallTime()` for displayed timestamps).
- No `Thread.sleep` in unit tests; advance `FakeClock` instead.
- Every mutable field of a Partition is guarded by its lock. Document the lock ordering (source → DLQ) in code comments.
- Prefer records for immutable value types.

## 5. HTTP API (implemented in PR 10)

| Method | Path | Body | Responses |
|---|---|---|---|
| PUT | `/queues/{name}` | `{visibilityTimeoutSeconds?, maxDeliveries?, maxDepth?}` | 201 created / 200 identical config / 409 different config / 400 |
| GET | `/queues/{name}` | – | 200 config / 404. For `{name}.dlq`: 200 read-only list of DLQ messages (D18b; shape settled before PR 10) |
| POST | `/queues/{name}/messages` | `{payload, priority, ttlSeconds?}` | 201 `{messageId}` / 400 (incl. enqueue into a `.dlq`) / 404 / 429 |
| POST | `/queues/{name}/dequeue` | – | 200 `{messageId, receiptHandle, payload, priority, deliveryCount, enqueuedAt, visibleUntil}` plus dead-letter metadata when dequeued from a DLQ (D18c) / 204 / 404 |
| POST | `/queues/{name}/messages/{id}/ack` | `{receiptHandle}` | 204 / 404 gone or unknown / 409 stale receipt or lease expired (see D8d) |
| GET | `/queues/{name}/metrics` | – | 200 JSON snapshot / 404 |
| GET | `/metrics` | – | Prometheus text exposition |
| GET | `/health` | – | 200 |

The receipt goes in the request body, never the URL, so it doesn't leak into access logs. Errors use the shape `{error: CODE, message}`.

## 6. Metrics (built in PR 7, exposed in PR 11)

All metrics carry the `queue` label.

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
| `dpq_operation_duration_seconds` | histogram | `op` (HTTP layer, PR 11) |

- **Oldest message age** is O(1) per lane: the lane head's monotonic `enqueuedAtMono` (D18d), taken after purging dead heads lazily. The per-queue value (JSON) is the maximum across priorities. Redelivered messages use their original enqueue time; dead-lettered messages use the dead-letter time (D18c).
- **JSON adds** `enqueueRatePerSec` and `ackRatePerSec`, computed over the 60s sliding window.

## 7. PR stack

Each PR lists its scope and then its **tests**, which are also its acceptance criteria. For TDD PRs, write the tests first.

### PR 1 — Scaffold (`pr/01-scaffold`) · TDD exempt
- Gradle multi-module setup: `settings.gradle.kts` plus `core`, `server`, `harness` modules.
- Java 21 toolchain (foojay resolver plugin auto-provisions JDK 21, D18a) and a version catalog (`gradle/libs.versions.toml`) with JUnit 5, AssertJ and jqwik.
- `.gitignore`, `.github/workflows/ci.yml` (runs `./gradlew check` on PRs and on main), `.github/pull_request_template.md`.
- `CLAUDE.md` containing the conventions from §4, the build/test commands, and pointers to `decisions.md` and `plan.md`.
- Makefile skeleton (`build`, `test`).
- **Accept:** one smoke test per module passes, and CI is green.

### PR 2 — Domain primitives (`pr/02-domain`) · D8a, D11
- `Priority` enum (HIGH, MEDIUM, LOW; ordinal = rank).
- `QueueConfig` record with defaults and validation.
- `Clock` interface (`monotonicMillis()`, `wallTime()`) and `SystemClock`.
- `FakeClock` in `core` testFixtures, via the `java-test-fixtures` plugin. Thread-safe (`AtomicLong`).
- `UuidV7`, `MessageId` (format `p{n}-{uuid}`, with parse and format), `ReceiptHandle` (random 128-bit, hex).
- `IdGenerator` and `ReceiptGenerator` interfaces, with random implementations and deterministic sequential test implementations.
- `Message` record(s).
- Exceptions:
  - `QueueNotFoundException`
  - `QueueAlreadyExistsException` (config mismatch)
  - `QueueFullException`
  - `StaleReceiptException`
  - `MessageNotFoundException`
  - `ValidationException`
- **Tests:**
  - `QueueConfig` defaults are 30s / 5 / 10k.
  - `QueueConfig` rejects visibility < 1s or > 12h, `maxDeliveries` < 1 or > 1000, and `maxDepth` < 1.
  - Two configs are equal when one omits fields and the other sends the defaults explicitly.
  - Queue name validation: accepts valid names, and rejects empty names, bad characters (including `.`, which makes `x.dlq` invalid as a user name) and names longer than 80 characters.
  - `FakeClock` advanced from 8 threads at once ends at exactly the sum of the advances.
  - UUIDv7: version/variant bits are correct, IDs are strictly increasing within the same millisecond (10k generated), and the embedded timestamp matches the clock.
  - `MessageId` round-trips through format/parse; parsing rejects malformed IDs.
  - `ReceiptHandle` values are unique across 100k generations.
  - `FakeClock.advance` moves `now()` forward exactly.

### PR 3 — Lane (`pr/03-lane`) · D4
- `Lane`: a `PriorityQueue<Entry>` retry heap keyed by seq, plus an `ArrayDeque<Entry>` main FIFO.
  - Operations: `offer`, `offerRetry`, `poll()`, `peekOldest()`, `markDead(entry)`, `isEmpty` (D18e).
  - Dead entries (tombstones) are skipped and discarded when they reach the head.
  - `markDead(entry)` sets the entry's `dead` flag and counts tombstones. When they exceed 50% of the lane's size, the lane is compacted (dead entries removed from the deque and the retry heap).
- **Tests:**
  - Messages come out in FIFO order.
  - A retry entry is served before any main-deque entry.
  - Multiple retries come out in seq order even when offered out of order (offer seq 5, then seq 3 → 3 comes first).
  - Tombstoned entries are skipped by both `poll` and `peekOldest`.
  - `peekOldest` returns the minimum over the retry heap and the deque.
  - Polling an empty lane returns empty.
  - Tombstoning 60% of a lane's middle entries triggers compaction: the physical size drops to the live count, and FIFO order of the live entries is preserved.

### PR 4 — Partition core (`pr/04-partition`) · D4, D5, D7, D8a, D11b
- `Partition`: a `ReentrantLock`, `EnumMap<Priority, Lane>`, a `messages` map, an `inFlight` map (`MessageId` → `Lease{receipt, deadline}`), and eager counts (ready per priority, in-flight).
- Operations: `enqueue`, `dequeue` (via `SelectionPolicy`; `StrictPriorityPolicy` is the only implementation), `ack(id, receipt)`.
- Enforces `maxDepth`.
- Stamps a monotonic `deliverySeq` on every delivery, under the lock. It is returned on the delivered message (for the ordering tests in PR 9) and not exposed over HTTP.
- **Tests:**
  - Dequeue returns HIGH before an older MEDIUM, and MEDIUM before an older LOW.
  - Messages of the same priority come out FIFO.
  - Dequeue on an empty partition returns empty.
  - A dequeued message is invisible to a second dequeue.
  - Ack with a valid receipt removes the message; a second ack throws `MessageNotFound`.
  - Ack with the wrong receipt throws `StaleReceipt`.
  - Ack of an unknown id throws `MessageNotFound`.
  - Enqueue at `maxDepth` throws `QueueFull`, and in-flight messages count toward the depth.
  - After an ack frees space, enqueue succeeds again.
  - Counts (ready per priority, in-flight) are correct after each operation.
  - `deliveryCount` is 1 on the first dequeue.
  - `deliverySeq` goes up strictly across deliveries.

### PR 5 — Timeouts & TTL (`pr/05-timeouts`) · D6, D8b, D9b, D9c
- Visibility-deadline and TTL-deadline sets: `TreeSet<Deadline(at, seq)>`, removed eagerly (the visibility entry on ack; the TTL entry on dequeue, re-added on redelivery).
- `drainExpired(now, limit)` runs at the start of every operation with `limit = K` (default 256); the reaper (PR 6) calls it with no limit.
- Lease-expiry outcome follows the **D8d table**, in order: TTL passed → drop as expired; else `deliveryCount >= maxDeliveries` → `DeadLetterSink`; else → the lane's retry heap (original seq).
- An expired TTL on a ready message drops it (tombstone + counter).
- `ack` rejects with `StaleReceipt` when `now >= lease.deadline`, whether or not the drain has processed that lease yet.
- **Tests (all with `FakeClock`; one per D8d row plus the following):**
  - At deadline − 1ms the message is still invisible; at the deadline it becomes visible again.
  - A redelivered message comes before a newer message of the same priority.
  - `deliveryCount` increments on each redelivery.
  - An old receipt after redelivery throws `StaleReceipt`.
  - With `maxDeliveries=2`: the 1st expiry → redelivery; the 2nd expiry → DLQ sink called with reason `MAX_DELIVERIES`; the message is not visible in the source.
  - With `maxDeliveries=1`: the first expiry sends the message straight to the DLQ.
  - A ready message past its TTL is never dequeued and the expired counter increments.
  - A TTL that passes while in flight still allows the ack to succeed.
  - A TTL that passes while in flight, followed by lease expiry, drops the message as expired (no redelivery, no DLQ).
  - A lease expiring with `deliveryCount >= maxDeliveries` **and** the TTL passed → dropped as expired, not dead-lettered.
  - Ack at exactly the lease deadline, before any drain, → `StaleReceipt`.
  - Ack of a message that was dead-lettered or expired → `MessageNotFound`.
  - Acking before the deadline, then advancing past it, causes no phantom redelivery.
  - An expired entry deep in the deque (not at the head) is still removed from the counts.
  - After enqueue → dequeue → ack of many TTL'd messages, both deadline sets are empty (no leak).
  - With K+1 leases expiring at the same instant, one op drains exactly K; a full drain handles the rest.

### PR 6 — QueueService, DLQ & Reaper (`pr/06-service`) · D5, D9a, D11c, D6
- `QueueService`: a `ConcurrentHashMap` registry.
  - `createQueue(name, config)`: idempotent (compares resolved configs); creates and registers `name.dlq` first, then publishes `name` via `computeIfAbsent`. Enforces the queue-count cap.
  - Enqueue into a `.dlq` queue is rejected (`ValidationException`).
  - `enqueue`, `dequeue`, `ack`, `getQueue`.
- DLQ wiring: the dead-letter sink enqueues into the DLQ partition while holding the source lock (lock order source → DLQ). DLQ metadata is attached and the TTL cleared.
- `Reaper` (implements `AutoCloseable`): a scheduled sweep calling `drainExpired` on every partition, with a configurable interval.
- `QueueService` implements `AutoCloseable` so it can stop the reaper.
- **Tests:**
  - Creating a queue twice with the same config is fine; creating it with a different config throws `QueueAlreadyExists`.
  - Creating `foo` makes `foo.dlq` exist.
  - Creating a user queue named `x.dlq` is rejected.
  - Enqueue into `foo.dlq` is rejected; dequeue and ack on it work.
  - 16 threads concurrently creating the same queue: all succeed, and exactly one `foo` and one `foo.dlq` exist.
  - Creating one queue over the cap is rejected.
  - Operations on an unknown queue throw `QueueNotFound`.
  - End to end: a message that exhausts its deliveries appears in `foo.dlq` with the right metadata and original priority, and can be dequeued and acked there.
  - A DLQ accepts messages past `maxDepth`, and the DLQ itself has no DLQ.
  - The reaper expires leases on an idle queue with no API calls: a test reaper driven manually with `FakeClock`, plus one real-scheduler test with a short interval and Awaitility-style polling.
  - After `close()` the reaper stops.

### PR 7 — Metrics core (`pr/07-metrics`) · D12
- Per-queue counters: plain `long` fields, updated under the partition lock (D5).
- `SlidingWindowRate`: a ring of 60 one-second `long` buckets keyed by clock second, updated under the partition lock; not thread-safe on its own, by design.
- New counters: `delivered` (by priority) and `dequeueEmpty`. Oldest age by priority.
- `QueueMetricsSnapshot` (built under the partition lock, O(1) per partition).
- `QueueService.metrics(name)` and `metricsAll()`.
- **Tests:**
  - Ready counts per priority match after a mixed enqueue.
  - The in-flight count is correct.
  - Oldest age = now − the oldest ready message's `enqueuedAt`.
  - Oldest age ignores in-flight and expired messages.
  - A redelivered message uses its original `enqueuedAt`.
  - Oldest age is 0 when the queue is empty.
  - Oldest age by priority: with LOW starved behind a stream of HIGH, the LOW age grows while the HIGH age stays small.
  - The enqueued/delivered/empty-dequeue/acked/redelivered/dead-lettered/expired/rejected counters each increment exactly once per event.
  - Sliding window: 120 enqueues over 60s gives ≈ 2/s; buckets older than 60s are evicted; after 60s idle the rate is 0.
  - Snapshot values are internally consistent (no partial updates).

### PR 8 — Model-based tests (`pr/08-model-tests`) · D13b · verification
- `ModelQueue`: a naive single-threaded reference implementation using a `List<Message>` sorted on every dequeue and a plain `long now`. It encodes the rules as written in Requirements.txt and the **D8d table**, written from those documents rather than copied from the implementation.
- Both sides use the deterministic `IdGenerator` and `ReceiptGenerator`, so IDs and receipts compare directly. The real service runs with drain limit = ∞ and no reaper, so the results are deterministic.
- A jqwik `ActionSequence` over the actions `Enqueue(priority, ttl?)`, `Dequeue`, `Ack(valid|stale|unknown)`, `AdvanceClock(0..60s)` and `CheckMetrics`.
- Every action runs against both the real `QueueService` (with `FakeClock`) and `ModelQueue`, and the results must be equal.
- **Accept:**
  - 1,000 tries pass.
  - Any bug found gets a minimal regression unit test (TDD) and its fix, both in this PR.
  - The PR description lists the bugs found, or "none".

### PR 9 — Stress tests (`pr/09-stress`) · D13a · verification
- Stress suite tagged `@Tag("stress")`, with `@RepeatedTest(20)`:
  - 8 producers × 5k messages with random priorities; 8 consumers that ack 90% of messages and abandon 10%.
  - A clock-advancer thread moves the clock forward; all workers start together on a start latch.
- **Concurrent ordering tests** (priority inversion), using `deliverySeq`:
  - *Phase:* preload 30k mixed messages, then 16 consumers drain at once. Sorted by `deliverySeq`, the priority never goes up, and each priority is FIFO by enqueue seq.
  - *Interleaved:* 8 producers and 8 consumers run at once. Producers record when each enqueue returned (a global `AtomicLong` tick), consumers record the tick at dequeue start. Assert that no LOW or MEDIUM was delivered while a higher-priority message was still ready whose enqueue had returned before that dequeue started (i.e. it was delivered later, by `deliverySeq`). Also assert per-producer FIFO within a priority.
- A Gradle `stressTest` task; `check` excludes the stress tag. A CI job runs `stressTest`.
- **Invariants asserted:**
  - **Conservation:** enqueued = acked + in DLQ + expired, with no duplicate terminal states and no missing IDs.
  - **Lease exclusivity:** no two consumers held valid, overlapping leases on the same ID.
  - Counters equal the ground truth; ready = 0 and in-flight = 0 at the end.
  - The run completes within 30s (liveness).
  - No worker exceptions.

### PR 10 — HTTP API (`pr/10-http`) · D2, D10
- Javalin app factory taking a `QueueService`.
- DTO records, Jackson config, and an exception → status mapper (§5).
- Validation: payload size, required priority, TTL range.
- `Main` with config from env vars/args (port, reaper interval), and graceful shutdown.
- **Tests (javalin-testtools):** every row in §5, including:
  - PUT gives 201, then 200 on repeat, then 409 on a changed config.
  - Enqueue with a missing priority returns 400.
  - A payload over 256 KB returns 400.
  - Enqueue to an unknown queue returns 404.
  - A full queue returns 429.
  - Dequeue on an empty queue returns 204.
  - Ack with a stale receipt returns 409.
  - The full enqueue → dequeue → ack flow works.

### PR 11 — Prometheus endpoint (`pr/11-prometheus`) · D12a
- A prometheus client_java 1.x custom `MultiCollector` that reads `metricsAll()` at scrape time.
- A `dpq_operation_duration_seconds{queue, op}` histogram recorded by a Javalin before/after handler for the enqueue, dequeue and ack routes.
- Routes: `/metrics` and the JSON `/queues/{name}/metrics`.
- **Tests:**
  - The `/metrics` output parses as a valid exposition, and every metric from §6 is present with the correct labels and types.
  - Values match the service state after a scripted scenario.
  - The JSON endpoint matches the snapshot, including the rates.
  - The DLQ queue appears as its own `queue` label.

### PR 12 — Harness (`pr/12-harness`) · D14b · TDD exempt
- A `harness` CLI (picocli or plain arg parsing) with flags `--base-url --queue --producers --consumers --messages --crash-rate --duration --rate`.
- `--rate N` switches to **open-loop** mode: requests are scheduled at a fixed arrival rate, and latency is measured from the *scheduled* start time, so queueing delay counts (no coordinated omission). Without `--rate`, the harness runs closed-loop.
- Uses virtual threads with `java.net.http.HttpClient`.
- Records latency in HdrHistogram (or an equivalent).
- Prints throughput and p50/p95/p99 for enqueue and dequeue, PASS/FAIL against p95 < 100ms, and the conservation check result.
- Includes an in-process demo test that starts the server on a random port and runs a small harness to verify conservation.

### PR 13 — Packaging (`pr/13-packaging`) · D14c · TDD exempt
- Multi-stage `Dockerfile` (Gradle build → JRE 21 slim).
- `docker-compose.yml`: `dpq` service, `prometheus` with `prometheus.yml` scraping `dpq:8080/metrics`, and a `harness` service under a profile.
- Makefile targets: `run`, `docker-up`, `docker-down`, `harness`, `stress`.
- CI adds a docker build step.

### PR 14 — README (`pr/14-readme`) · D15 · TDD exempt
- Overview and a quick start (local and compose).
- API reference.
- Data-model diagram and a concurrency-model explanation: lock per partition, lock ordering, lazy expiry + reaper.
- Delivery guarantees: at-least-once, stale receipts, the D8d precedence table (404 on ack is terminal), what is lost on a crash.
- The metrics design.
- Testing methodology: unit + FakeClock, model-based, stress + concurrent ordering, harness (closed- and open-loop).
- Horizontal scaling (D15a, consumer partition assignment), durability and node failure (D15b: leases not replicated), producer dedup (D15c).
- Memory limits: `maxDepth` 10k × 256 KB worst case per queue; the queue cap.
- Extensions: changeVisibility/nack, DLQ redrive, long-poll, batch, N partitions, aging policy, WAL, delayed delivery (released into the retry heap by seq), numeric priorities (`TreeMap<Integer, Lane>`), delete queue.
- "With more time": Lincheck, persistence, `maxBytes` / node-wide byte budget, idempotency keys.
- A link to `decisions.md`.

## 8. Final verification (after the whole stack is merged)
- `./gradlew check` and `./gradlew stressTest` pass locally and in CI.
- `docker compose up` works: curl create → enqueue → dequeue → ack, and the Prometheus UI (`localhost:9090`) shows the `dpq_*` series.
- `make harness` against compose reports conservation OK and p95 < 100ms.
