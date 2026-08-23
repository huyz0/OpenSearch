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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The node-level holder for the single {@link ClaimedIndexLifecycle} a {@link
 * org.opensearch.plugins.ClusterPlugin} may supply, mirroring {@link IndexCatalogRegistry}'s shape
 * deliberately -- the safest way to introduce a seam is one that looks structurally identical to a mechanism
 * already proven in this codebase.
 *
 * <p><b>Why a holder exists at all, when the lifecycle is also injected.</b> Most of core reaches the plane
 * as a constructor argument, which is better: no static to keep in step, nothing to leak between tests. Two
 * call sites cannot. {@code Metadata.Builder.put} is a method on a data-structure builder, and {@code
 * MetadataCreateIndexService.clusterStateCreateIndex} is static and called from static contexts. Both are
 * places an index's record has to be written from, and neither has anywhere to put an injected field. This
 * holds the same instance {@code ClusterModule} binds, selected once by {@code Node}, so there is one plugin
 * hook and one object however it is reached.
 *
 * <p><b>Core's defensive policy lives here, not in the interface.</b> The two methods below are the ones
 * called from inside cluster state construction, where core -- not an implementer -- decides what a
 * misbehaving plane may do to the thread it is standing on: {@link #recordChange} swallows, because a
 * redundant record is worth less than the state change it accompanies, and {@link #createIndex} converts a
 * synchronous throw into a failed stage, so one creation fails rather than the transform that carries it.
 * These policies were {@code IndexDescriptorPublisher}'s, and they are the only part of that class that was
 * ever core's business.
 */
public final class ClaimedIndexLifecycleRegistry {

    private static final Logger logger = LogManager.getLogger(ClaimedIndexLifecycleRegistry.class);

    private static final AtomicReference<ClaimedIndexLifecycle> LIFECYCLE = new AtomicReference<>();

    private ClaimedIndexLifecycleRegistry() {}

    /** Installs the lifecycle. Registering {@code null} clears it, which is how a test restores the default. */
    public static void register(ClaimedIndexLifecycle lifecycle) {
        LIFECYCLE.set(lifecycle);
    }

    /**
     * Whether any plugin supplied one on this node. Not the same question as "would it do anything": an
     * installed plane whose backing store is not armed answers every operation the way an absent one does,
     * and it is the plane's business to say so rather than this holder's to guess.
     */
    public static boolean isRegistered() {
        return LIFECYCLE.get() != null;
    }

    /** The installed lifecycle, or {@link ClaimedIndexLifecycle#NOOP} when none is. Never null. */
    public static ClaimedIndexLifecycle get() {
        ClaimedIndexLifecycle lifecycle = LIFECYCLE.get();
        return lifecycle == null ? ClaimedIndexLifecycle.NOOP : lifecycle;
    }

    /**
     * {@link ClaimedIndexLifecycle#recordChange} with core's failure policy applied.
     *
     * @return whether anything was told, which is what lets a test tell "nothing installed" apart from
     *         "installed and did nothing", since both leave no record behind
     */
    public static boolean recordChange(IndexMetadata indexMetadata) {
        ClaimedIndexLifecycle lifecycle = LIFECYCLE.get();
        if (lifecycle == null || indexMetadata == null) {
            return false;
        }
        try {
            lifecycle.recordChange(indexMetadata);
        } catch (Exception e) {
            // Swallowed on purpose: this runs inside cluster state construction, and during dual write the
            // record is redundant, so a failure here costs a comparison rather than an index. Letting it out
            // would fail the state change that carries it.
            logger.warn("failed to record the change to [" + indexMetadata.getIndex().getName() + "]", e);
        }
        return true;
    }

    /**
     * {@link ClaimedIndexLifecycle#createIndex} with core's failure policy applied.
     *
     * <p>Refuses rather than degrading when nothing is installed. An index configured to skip its cluster
     * state entry and recorded nowhere else is an index with no record anywhere, which is the outcome this
     * whole path exists to prevent -- so the message is core's own, stated in terms of the decision core
     * made, rather than the interface default's.
     */
    public static CompletionStage<Boolean> createIndex(IndexMetadata indexMetadata) {
        ClaimedIndexLifecycle lifecycle = LIFECYCLE.get();
        if (lifecycle == null) {
            return CompletableFuture.failedFuture(
                new IllegalStateException(
                    "index ["
                        + indexMetadata.getIndex().getName()
                        + "] is configured to skip its cluster state entry, but no plugin records indices "
                        + "held outside it, so creating it would leave no record of it anywhere"
                )
            );
        }
        try {
            CompletionStage<Boolean> write = lifecycle.createIndex(indexMetadata);
            if (write == null) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException(
                        "the claimed-index lifecycle returned no stage for ["
                            + indexMetadata.getIndex().getName()
                            + "], so nothing was written"
                    )
                );
            }
            return write;
        } catch (Exception e) {
            // A plane that throws on the way in has written nothing, and that is this creation's failure
            // rather than the cluster state transform's.
            return CompletableFuture.failedFuture(e);
        }
    }
}
