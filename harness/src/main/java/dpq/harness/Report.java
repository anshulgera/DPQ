package dpq.harness;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;
import org.HdrHistogram.Histogram;

/** The outcome of a harness run: throughput, latency percentiles, and the conservation check. */
record Report(HarnessConfig config, double producingSeconds, double elapsedSeconds, Set<String> enqueued,
        Set<String> acked, Set<String> deadLettered, long duplicates, long deliveries, long crashed, long errors,
        Histogram enqueueLatency, Histogram dequeueLatency) {

    static final double P95_TARGET_MILLIS = 100;

    /** Enqueued messages that were neither acked nor dead-lettered. */
    Set<String> missing() {
        Set<String> missing = new HashSet<>(enqueued);
        missing.removeAll(acked);
        missing.removeAll(deadLettered);
        return missing;
    }

    /** Every enqueued message was acked or dead-lettered, exactly once, and nothing unknown turned up. */
    boolean conservationOk() {
        Set<String> both = new HashSet<>(acked);
        both.retainAll(deadLettered);
        Set<String> unknown = new HashSet<>(acked);
        unknown.addAll(deadLettered);
        unknown.removeAll(enqueued);
        return missing().isEmpty() && both.isEmpty() && unknown.isEmpty() && duplicates == 0;
    }

    boolean latencyOk() {
        return p95Millis(enqueueLatency) < P95_TARGET_MILLIS && p95Millis(dequeueLatency) < P95_TARGET_MILLIS;
    }

    boolean passed() {
        return conservationOk() && latencyOk() && errors == 0;
    }

    void print(PrintStream out) {
        String mode = config.openLoop()
                ? "open-loop at " + config.rate() + " enqueues/s for " + config.duration() + "s"
                : "closed-loop";
        out.printf("DPQ harness: %s, queue %s, %d producers / %d consumers, crash rate %.0f%%%n", mode,
                config.queue(), config.producers(), config.consumers(), config.crashRate() * 100);
        out.printf("  throughput   %d enqueued in %.1fs (%.0f/s); %d deliveries (%d crashed); %d acked in %.1fs"
                        + " (%.0f/s)%n",
                enqueued.size(), producingSeconds, enqueued.size() / producingSeconds, deliveries, crashed,
                acked.size(), elapsedSeconds, acked.size() / elapsedSeconds);
        printLatency(out, "enqueue", enqueueLatency);
        printLatency(out, "dequeue", dequeueLatency);
        out.printf("  conservation %s: %d enqueued = %d acked + %d dead-lettered (%d missing, %d duplicates)%n",
                conservationOk() ? "OK" : "FAIL", enqueued.size(), acked.size(), deadLettered.size(),
                missing().size(), duplicates);
        out.printf("  errors       %d%n", errors);
        out.println("RESULT: " + (passed() ? "PASS" : "FAIL"));
    }

    private static void printLatency(PrintStream out, String op, Histogram h) {
        out.printf("  %-12s p50 %.2fms  p95 %.2fms  p99 %.2fms  (n=%d)  %s%n", op + " latency",
                h.getValueAtPercentile(50) / 1000.0, p95Millis(h), h.getValueAtPercentile(99) / 1000.0,
                h.getTotalCount(), p95Millis(h) < P95_TARGET_MILLIS ? "PASS (p95 < 100ms)" : "FAIL (p95 >= 100ms)");
    }

    private static double p95Millis(Histogram h) {
        return h.getValueAtPercentile(95) / 1000.0;
    }
}
