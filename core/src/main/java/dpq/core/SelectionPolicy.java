package dpq.core;

import dpq.core.model.Priority;
import java.util.EnumMap;
import java.util.Optional;

/** Chooses which priority lane serves the next dequeue (D7). Called under the partition lock. */
interface SelectionPolicy {

    /** The lane to poll next, or empty when every lane is empty. */
    Optional<Lane> select(EnumMap<Priority, Lane> lanes);
}
