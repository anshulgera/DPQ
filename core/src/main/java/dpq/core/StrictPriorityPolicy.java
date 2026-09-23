package dpq.core;

import java.util.EnumMap;
import java.util.Optional;

final class StrictPriorityPolicy implements SelectionPolicy {

    @Override
    public Optional<Lane> select(EnumMap<Priority, Lane> lanes) {
        throw new UnsupportedOperationException("not implemented");
    }
}
