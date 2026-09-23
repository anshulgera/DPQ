package dpq.core.model;


/** Message priority. Declaration order is rank: {@code HIGH} is served first under strict priority (D7). */
public enum Priority {
    HIGH,
    MEDIUM,
    LOW
}
