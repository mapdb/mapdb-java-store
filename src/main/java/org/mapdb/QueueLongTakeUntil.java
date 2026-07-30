package org.mapdb;

/** Callback used by {@link QueueLong#takeUntil}. Return true to consume the node. */
@FunctionalInterface
public interface QueueLongTakeUntil {
    boolean take(long nodeRecid, QueueLong.Node node);
}
