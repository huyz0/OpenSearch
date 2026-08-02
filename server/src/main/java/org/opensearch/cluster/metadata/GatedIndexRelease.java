/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.Index;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Tells a node that a gated index is gone, so it can close the shards it opened for one.
 *
 * <h2>Why this seam exists</h2>
 *
 * Every other index closure is driven by a cluster state diff. A gated index never appears in one, so a
 * delete tells nobody: the cluster manager tombstones the descriptor and returns, and a node holding the
 * shard has no way to hear about it. Left alone the shard stays open forever, holding its
 * {@code NodeEnvironment} lock, its memory and its files, and still able to serve reads for an index the
 * cluster says is gone.
 *
 * <p>D2 closed that with a per-node sweep that re-resolves every on-demand index on a timer. The sweep
 * works and its own comment says what it is: an honest stand-in, trading latency bounded by the interval
 * for needing nothing that does not already exist, with the push version belonging on the change feed. This
 * is that push. The feed knows the moment a descriptor is tombstoned; this is how it says so.
 *
 * <h2>What it does not replace</h2>
 *
 * The sweep stays as the backstop, and that is deliberate rather than leftover. A push arrives once and can
 * be missed: a node down during the change, a change log entry that failed to append, a tailer that threw.
 * The sweep re-derives the answer from scratch every interval and so converges regardless. Keeping both
 * means the common case is fast and the uncommon case is still correct, which is the same reason a cache
 * has a TTL as well as invalidation.
 *
 * <p>Unset by default, so a node with no plugin closes nothing and behaves exactly as it did.
 */
public final class GatedIndexRelease {

    private static final Logger logger = LogManager.getLogger(GatedIndexRelease.class);

    private static final AtomicReference<Consumer<Index>> RELEASER = new AtomicReference<>();

    private GatedIndexRelease() {}

    /** Installs the releaser. Registering null clears it, restoring the previous behaviour exactly. */
    public static void register(Consumer<Index> releaser) {
        RELEASER.set(releaser);
    }

    public static boolean isRegistered() {
        return RELEASER.get() != null;
    }

    /**
     * Releases this index if this node holds it on demand, or does nothing.
     *
     * <p>Takes an {@link Index} rather than a name because a name alone cannot tell a deleted index from a
     * new one created with the same name moments later, and closing the wrong one would take down a live
     * shard. The change feed carries the uuid for exactly this reason.
     *
     * <p>Never throws. This runs from a feed consumer that must keep going: one index that cannot be closed
     * is a leaked shard, and abandoning the rest of the batch would leak more.
     */
    public static void release(Index index) {
        Consumer<Index> releaser = RELEASER.get();
        if (releaser == null || index == null) {
            return;
        }
        try {
            releaser.accept(index);
        } catch (Exception e) {
            logger.warn("could not release the gated index [{}]; the sweep will retry", index, e);
        }
    }
}
