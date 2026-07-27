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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Node-level hook for recording an index's descriptor when the index is created.
 *
 * <p>This is the dual-write half of Area H's second phase. While the cluster state entry is still being
 * written, the descriptor is written alongside it, deliberately redundant, so the two resolution paths can
 * be compared against each other while the old structure is still there to be right. Once that comparison
 * holds, H3 stops writing the cluster state entry and only this remains.
 *
 * <p><b>The descriptor is derived, not constructed.</b> {@link IndexDescriptor#from} reduces the same
 * {@link IndexMetadata} that is going into cluster state, so the two cannot drift. Building it from
 * separate inputs would make agreement between the paths prove only that they shared a mistake.
 *
 * <p><b>This runs on the cluster manager's state update thread.</b> That thread is single and every
 * cluster state change queues behind it, so an implementation that writes to an index synchronously here
 * would add a round trip to every index creation and serialise it against everything else. Implementations
 * must hand the descriptor off and return. The contract is fire and forget, and it is the caller's
 * problem to make the write durable, not this thread's.
 *
 * <p><b>A failure here must not fail index creation.</b> During dual write the descriptor is redundant, so
 * losing one costs a comparison rather than an index. That inverts in H3, where the descriptor becomes the
 * only record and its write has to be the thing that succeeds or fails the request. The two phases have
 * opposite failure semantics and this is the forgiving one.
 */
public final class IndexDescriptorPublisher {

    private static final Logger logger = LogManager.getLogger(IndexDescriptorPublisher.class);

    private static final AtomicReference<Consumer<IndexDescriptor>> PUBLISHER = new AtomicReference<>();

    private IndexDescriptorPublisher() {}

    /** Installs the publisher. Registering null clears it, which is how a test restores the default. */
    public static void register(Consumer<IndexDescriptor> publisher) {
        PUBLISHER.set(publisher);
    }

    public static boolean isRegistered() {
        return PUBLISHER.get() != null;
    }

    /**
     * Records that an index was deleted, as a tombstoned descriptor rather than an absence.
     *
     * <p>Absence cannot be distinguished from not having looked, which is why {@link IndexGraveyard}
     * exists at all: a node partitioned during a delete would otherwise adopt its dangling shard data on
     * rejoin. A tombstoned descriptor answers that question durably, and unlike the graveyard it does not
     * forget, since the graveyard keeps a bounded list and purges the oldest entries.
     *
     * <p>The uuid and shard count survive the tombstone deliberately. They are what identifies the
     * dangling data that has to be reclaimed, so a tombstone that carried only the name would say an
     * index is gone without saying what to delete.
     *
     * @return whether a publisher was invoked, so a caller can tell "nothing listening" from "recorded"
     */
    public static boolean publishTombstone(IndexMetadata indexMetadata) {
        Consumer<IndexDescriptor> publisher = PUBLISHER.get();
        if (publisher == null || indexMetadata == null) {
            return false;
        }
        try {
            publisher.accept(IndexDescriptor.from(indexMetadata).tombstoned());
            return true;
        } catch (Exception e) {
            logger.warn("failed to publish tombstone for [" + indexMetadata.getIndex().getName() + "]", e);
            return true;
        }
    }

    /**
     * Records the descriptor for a newly created index, if anything is listening.
     *
     * @return whether a publisher was invoked, which is what lets a test tell "nothing installed" apart
     *         from "installed and did nothing", since both leave no descriptor behind
     */
    public static boolean publish(IndexMetadata indexMetadata) {
        Consumer<IndexDescriptor> publisher = PUBLISHER.get();
        if (publisher == null || indexMetadata == null) {
            return false;
        }
        try {
            publisher.accept(IndexDescriptor.from(indexMetadata));
            return true;
        } catch (Exception e) {
            // Redundant during dual write, so a failure here costs a comparison rather than an index.
            logger.warn("failed to publish descriptor for [" + indexMetadata.getIndex().getName() + "]", e);
            return true;
        }
    }
}
