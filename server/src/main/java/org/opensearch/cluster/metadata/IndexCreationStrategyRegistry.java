/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Phase D2 of {@code core-pluggability-refactor-plan.md}: the node-level holder for the single {@link
 * IndexCreationStrategy} a {@link org.opensearch.plugins.ClusterPlugin} may supply, mirroring {@link
 * DescriptorOnlyCreation}'s own static-registry shape deliberately -- the safest possible way to introduce
 * this seam is one that looks structurally identical to the mechanism already proven in this exact spot,
 * changing only <em>who decides</em> a name is claimed, not <em>how</em> that decision is discovered.
 *
 * <p>Registered once, at node startup, by {@code Node.java} from whichever single {@code ClusterPlugin}
 * supplies {@link org.opensearch.plugins.ClusterPlugin#getIndexCreationStrategy()} (see that method's own
 * javadoc). With nothing registered -- the case for any node without such a plugin installed -- {@link
 * #claims} answers {@code false} unconditionally, so a node without the plugin behaves exactly as it did
 * before this class existed.
 *
 * <p>A predicate that throws is treated as "does not claim this," matching {@link
 * DescriptorOnlyCreation#skipsClusterState}'s own failure direction: the failure mode of wrongly answering
 * {@code true} (an index treated as claimed when it should not have been) is worse than the failure mode of
 * wrongly answering {@code false} (an index that falls through to the ordinary path it would have taken
 * anyway).
 */
public final class IndexCreationStrategyRegistry {

    private static final AtomicReference<IndexCreationStrategy> STRATEGY = new AtomicReference<>();

    private IndexCreationStrategyRegistry() {}

    /** Installs the strategy. Registering {@code null} clears it, which is how a test restores the default. */
    public static void register(IndexCreationStrategy strategy) {
        STRATEGY.set(strategy);
    }

    public static boolean isRegistered() {
        return STRATEGY.get() != null;
    }

    /**
     * Whether the registered strategy (if any) claims the given index name/request. Answers {@code false}
     * when nothing is registered, or when the registered strategy throws.
     */
    public static boolean claims(String indexName, CreateIndexClusterStateUpdateRequest request) {
        IndexCreationStrategy strategy = STRATEGY.get();
        if (strategy == null) {
            return false;
        }
        try {
            return strategy.claims(indexName, request);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Whether the registered strategy (if any) claims the given index name, with no request in scope --
     * see {@link IndexCreationStrategy#claims(String)}. Answers {@code false} when nothing is registered,
     * or when the registered strategy throws.
     */
    public static boolean claims(String indexName) {
        IndexCreationStrategy strategy = STRATEGY.get();
        if (strategy == null) {
            return false;
        }
        try {
            return strategy.claims(indexName);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Whether the registered strategy (if any) says this finished {@link IndexMetadata} should skip its
     * cluster-state entry -- see {@link IndexCreationStrategy#skipsClusterState(IndexMetadata)}. Answers
     * {@code false} when nothing is registered, when {@code indexMetadata} is {@code null}, or when the
     * registered strategy throws -- the same "wrongly answering false costs what it always cost, wrongly
     * answering true costs the index's only record" direction {@code DescriptorOnlyCreation.skipsClusterState}
     * already established for this exact predicate.
     */
    public static boolean skipsClusterState(IndexMetadata indexMetadata) {
        IndexCreationStrategy strategy = STRATEGY.get();
        if (strategy == null || indexMetadata == null) {
            return false;
        }
        try {
            return strategy.skipsClusterState(indexMetadata);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * The registered strategy's description of the namespace it claims -- see {@link
     * IndexCreationStrategy#describeClaimedNamespace()}. Only meaningful (and only ever called by a caller)
     * after {@link #claims} has already answered {@code true}, so this falls back to the interface's own
     * generic default if nothing is registered or the registered strategy throws, rather than exposing that
     * as a separate failure mode callers need to handle.
     */
    public static String describeClaimedNamespace() {
        IndexCreationStrategy strategy = STRATEGY.get();
        if (strategy == null) {
            return DEFAULT_DESCRIPTION;
        }
        try {
            return strategy.describeClaimedNamespace();
        } catch (Exception e) {
            return DEFAULT_DESCRIPTION;
        }
    }

    private static final String DEFAULT_DESCRIPTION = new IndexCreationStrategy() {
    }.describeClaimedNamespace();
}
