package dpq.server;

import dpq.core.Priority;
import dpq.core.QueueMetricsSnapshot;
import dpq.core.QueueService;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

/**
 * Builds every per-queue {@code dpq_*} family from {@link QueueService#metricsAll()} at scrape time (D12a).
 * Nothing is stored between scrapes, so gauges are never stale and queues created at runtime (DLQs included,
 * under their own {@code queue} label) appear on their own.
 */
final class QueueMetricsCollector implements MultiCollector {

    private final QueueService service;

    QueueMetricsCollector(QueueService service) {
        this.service = service;
    }

    @Override
    public MetricSnapshots collect() {
        List<QueueMetricsSnapshot> all = service.metricsAll();
        return MetricSnapshots.of(
                gaugeByPriority("dpq_messages_ready", "Messages ready to be dequeued.", all,
                        QueueMetricsSnapshot::ready),
                gauge("dpq_messages_in_flight", "Messages dequeued but not yet acked or expired.", all,
                        QueueMetricsSnapshot::inFlight),
                gaugeByPriority("dpq_oldest_message_age_seconds",
                        "Age of the oldest ready message; 0 when none is ready.", all,
                        QueueMetricsSnapshot::oldestAgeSecondsByPriority),
                counterByPriority("dpq_messages_enqueued", "Messages accepted by enqueue (or dead-lettered in).",
                        all, QueueMetricsSnapshot::enqueued),
                counterByPriority("dpq_messages_delivered", "Deliveries, redeliveries included.", all,
                        QueueMetricsSnapshot::delivered),
                counter("dpq_dequeue_empty", "Dequeues that found no ready message.", all,
                        QueueMetricsSnapshot::dequeueEmpty),
                counter("dpq_messages_acked", "Messages acknowledged and removed.", all, QueueMetricsSnapshot::acked),
                counter("dpq_messages_redelivered", "Leases that expired and returned the message to the queue.",
                        all, QueueMetricsSnapshot::redelivered),
                counter("dpq_messages_dead_lettered", "Messages moved to this queue's DLQ.", all,
                        QueueMetricsSnapshot::deadLettered),
                counterByPriority("dpq_messages_expired", "Messages dropped because their TTL passed.", all,
                        QueueMetricsSnapshot::expired),
                counter("dpq_enqueue_rejected", "Enqueues rejected because the queue was full.", all,
                        QueueMetricsSnapshot::enqueueRejected));
    }

    private static GaugeSnapshot gauge(String name, String help, List<QueueMetricsSnapshot> all,
            ToDoubleFunction<QueueMetricsSnapshot> value) {
        GaugeSnapshot.Builder b = GaugeSnapshot.builder().name(name).help(help);
        for (QueueMetricsSnapshot m : all) {
            b.dataPoint(GaugeDataPointSnapshot.builder().labels(Labels.of("queue", m.queue()))
                    .value(value.applyAsDouble(m)).build());
        }
        return b.build();
    }

    private static GaugeSnapshot gaugeByPriority(String name, String help, List<QueueMetricsSnapshot> all,
            Function<QueueMetricsSnapshot, ? extends Map<Priority, ? extends Number>> values) {
        GaugeSnapshot.Builder b = GaugeSnapshot.builder().name(name).help(help);
        for (QueueMetricsSnapshot m : all) {
            values.apply(m).forEach((p, v) -> b.dataPoint(GaugeDataPointSnapshot.builder()
                    .labels(Labels.of("queue", m.queue(), "priority", p.name())).value(v.doubleValue()).build()));
        }
        return b.build();
    }

    /** {@code name} without {@code _total}; the text format adds it. */
    private static CounterSnapshot counter(String name, String help, List<QueueMetricsSnapshot> all,
            ToDoubleFunction<QueueMetricsSnapshot> value) {
        CounterSnapshot.Builder b = CounterSnapshot.builder().name(name).help(help);
        for (QueueMetricsSnapshot m : all) {
            b.dataPoint(CounterDataPointSnapshot.builder().labels(Labels.of("queue", m.queue()))
                    .value(value.applyAsDouble(m)).build());
        }
        return b.build();
    }

    private static CounterSnapshot counterByPriority(String name, String help, List<QueueMetricsSnapshot> all,
            Function<QueueMetricsSnapshot, Map<Priority, Long>> values) {
        CounterSnapshot.Builder b = CounterSnapshot.builder().name(name).help(help);
        for (QueueMetricsSnapshot m : all) {
            values.apply(m).forEach((p, v) -> b.dataPoint(CounterDataPointSnapshot.builder()
                    .labels(Labels.of("queue", m.queue(), "priority", p.name())).value(v).build()));
        }
        return b.build();
    }
}
