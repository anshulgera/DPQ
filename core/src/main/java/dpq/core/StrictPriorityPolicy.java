package dpq.core;

import dpq.core.model.Priority;
import java.util.EnumMap;
import java.util.Optional;

/** Serves the highest non-empty priority; a lower priority is never served while a higher one is ready (D7). */
final class StrictPriorityPolicy implements SelectionPolicy {

    @Override
    public Optional<Lane> select(EnumMap<Priority, Lane> lanes) {
        // EnumMap iterates in declaration order: HIGH, MEDIUM, LOW.
        return lanes.values().stream().filter(lane -> !lane.isEmpty()).findFirst();
    }
}
