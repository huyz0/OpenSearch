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
 * <p>This is the dual-write half of moving index metadata off cluster state. While the cluster state entry
 * is still being
 * written, the descriptor is written alongside it, deliberately redundant, so the two resolution paths can
 * be compared against each other while the old structure is still there to be right. Once that comparison
 * holds, the follow-up step stops writing the cluster state entry and only this remains.
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
 * losing one costs a comparison rather than an index. That inverts once the cluster state entry stops being
 * written, where the descriptor becomes the
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

    /**
     * Creates a gated index's descriptor, which for a gated index <em>is</em> the creation.
     *
     * <p>Separate from {@link #register}'s publisher because the two have opposite failure semantics, and
     * lost creations and duplicate-name acknowledgements are what happens when one stands in for the
     * other. A publisher records an index that
     * already exists in cluster state, so losing the write costs a comparison and fire and forget is
     * right. A creator is the only record the index will ever have, so it must be atomic against a
     * competing creation and its outcome must reach the client.
     *
     * <p>Returns a future rather than a boolean so the caller can defer the acknowledgement without
     * blocking. Blocking on the cluster state thread is an established deadlock, and the thread does
     * not need to wait; the acknowledgement does.
     */
    private static final AtomicReference<
        java.util.function.Function<IndexDescriptor, java.util.concurrent.CompletableFuture<Boolean>>> CREATOR = new AtomicReference<>();

    /**
     * Changes a gated index's stored descriptor, against whatever the store currently holds.
     *
     * <p><b>Why this takes a mutation rather than a descriptor.</b>
     *
     * It used to take the finished descriptor, and every caller built that descriptor the same way: resolve
     * the current one through {@code AbsentIndexDescriptorSuppliers}, apply a change to it, hand the result
     * back. Two things are wrong with that and neither is visible at the call site.
     *
     * <p>The resolved descriptor is a <em>cached</em> one, up to the descriptor cache's freshness window old
     * -- a minute, by the object-store backend's own default -- and on the cluster manager's update thread
     * it is the cached one or nothing at all, because the resolution seam refuses to do I/O there. So the
     * base of the read-modify-write was routinely stale.
     *
     * <p>And writing the result back is unconditional, so everything that changed on the real descriptor in
     * the meantime is silently reverted. A close issued while a dynamic field was being added rolls the
     * mapping back to whatever the closer's cached copy said, and the document carrying that field is then
     * unqueryable on it -- a wrong answer that looks like a correct one.
     *
     * <p>Taking the mutation instead makes both impossible to express: the implementation reads the
     * descriptor authoritatively, applies this function to <em>that</em>, and writes it conditionally on the
     * generation it read, retrying the whole cycle if it loses. The same shape {@code MappingGenerationStore}
     * uses, and for the same reason -- a caller that cannot name the base value cannot clobber it.
     */
    @FunctionalInterface
    public interface DescriptorMutator {
        /**
         * @param name     the index whose descriptor is to change
         * @param mutation applied to the descriptor as the store currently holds it; returning the argument
         *                 unchanged means "nothing to write", which is how an idempotent request (adding an
         *                 alias that is already there) says so without a write
         * @return a future completing {@code true} once the change is durable, {@code false} if it could not
         *         be applied, and exceptionally if the write failed. Never null.
         */
        java.util.concurrent.CompletableFuture<Boolean> update(String name, java.util.function.UnaryOperator<IndexDescriptor> mutation);
    }

    private static final AtomicReference<DescriptorMutator> UPDATER = new AtomicReference<>();

    /** Installs the creator. Registering null clears it. */
    public static void registerCreator(
        java.util.function.Function<IndexDescriptor, java.util.concurrent.CompletableFuture<Boolean>> creator
    ) {
        CREATOR.set(creator);
    }

    /** Installs the updater. Registering null clears it. */
    public static void registerUpdater(DescriptorMutator updater) {
        UPDATER.set(updater);
    }

    /** Whether anything can record a descriptor change, which a caller must not read as "it worked". */
    public static boolean isUpdaterRegistered() {
        return UPDATER.get() != null;
    }

    /**
     * Writes the descriptor that constitutes a gated index's creation, or null when nothing is installed.
     *
     * <p>The future completes {@code true} when this call created the name, {@code false} when a competing
     * creation won, and exceptionally when the write could not be made. Null means no creator is
     * registered, which the caller must treat as a failure rather than as success: an index with no record
     * anywhere is the outcome this whole path exists to prevent.
     */
    public static java.util.concurrent.CompletableFuture<Boolean> createGated(IndexMetadata indexMetadata) {
        java.util.function.Function<IndexDescriptor, java.util.concurrent.CompletableFuture<Boolean>> creator = CREATOR.get();
        if (creator == null || indexMetadata == null) {
            return null;
        }
        try {
            return creator.apply(IndexDescriptor.from(indexMetadata));
        } catch (Exception e) {
            return java.util.concurrent.CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Changes an existing gated index's descriptor in the store, off the calling thread.
     *
     * <p>Never null and never silently nothing: with no updater installed the future fails, because for a
     * gated index this write <em>is</em> the operation, and there is no cluster state entry behind it that
     * would still carry the change. A null return was the old shape and it was indistinguishable from
     * success at every one of its call sites, all four of which discarded the future entirely.
     *
     * @return a future completing {@code true} when the change is durable
     */
    public static java.util.concurrent.CompletableFuture<Boolean> updateGated(
        String name,
        java.util.function.UnaryOperator<IndexDescriptor> mutation
    ) {
        DescriptorMutator updater = UPDATER.get();
        if (updater == null || name == null || mutation == null) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                new IllegalStateException(
                    "no descriptor updater is installed, so the change to [" + name + "] has nowhere to be recorded; the index is unchanged"
                )
            );
        }
        try {
            java.util.concurrent.CompletableFuture<Boolean> future = updater.update(name, mutation);
            if (future == null) {
                return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("the descriptor updater returned no future for [" + name + "], so nothing was written")
                );
            }
            return future;
        } catch (Exception e) {
            return java.util.concurrent.CompletableFuture.failedFuture(e);
        }
    }

    /**
     * {@link #updateGated(String, java.util.function.UnaryOperator)} reported to a listener, which is what
     * a request handler deferring its acknowledgement actually wants.
     *
     * <p>Nothing blocks: the listener is completed from whichever thread completes the write. That is the
     * same arrangement {@link ClaimedIndexLifecycle} uses to make a deletion's acknowledgement wait for its
     * tombstone, and it is the only way a caller on a thread that must not block can still refuse to
     * acknowledge a change that did not happen.
     *
     * <p>A {@code false} outcome is a failure here rather than a quiet success. The updater returns false
     * only when the change could not be applied at all, and a request told "acknowledged" for that is the
     * silent-success failure this area has produced repeatedly.
     */
    public static void updateGated(
        String name,
        java.util.function.UnaryOperator<IndexDescriptor> mutation,
        org.opensearch.core.action.ActionListener<Void> whenWritten
    ) {
        updateGated(name, mutation).whenComplete((applied, failure) -> {
            if (failure != null) {
                Throwable cause = failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null
                    ? failure.getCause()
                    : failure;
                whenWritten.onFailure(
                    cause instanceof Exception ? (Exception) cause : new IllegalStateException("could not update [" + name + "]", cause)
                );
            } else if (Boolean.TRUE.equals(applied) == false) {
                whenWritten.onFailure(new IllegalStateException("the descriptor for [" + name + "] could not be updated"));
            } else {
                whenWritten.onResponse(null);
            }
        });
    }

    public static boolean isRegistered() {
        return PUBLISHER.get() != null;
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
