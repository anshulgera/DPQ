package dpq.core;

/** A queue's name and settings. A DLQ has a fixed config and no limits of its own (D18b). */
public record QueueDescription(String name, QueueConfig config, boolean deadLetterQueue) {}
