/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.common.Nullable;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The node-level holder for the single {@link IndexCatalog} a {@link org.opensearch.plugins.ClusterPlugin}
 * may supply, mirroring {@link IndexCreationStrategyRegistry}'s shape deliberately -- the safest way to
 * introduce a seam is one that looks structurally identical to a mechanism already proven in this codebase.
 *
 * <p>Registered once, at node startup, by {@code Node.java} from whichever single {@code ClusterPlugin}
 * supplies {@link org.opensearch.plugins.ClusterPlugin#getIndexCatalog()}. With nothing registered -- the
 * case for any node without such a plugin installed -- every method here answers exactly what core answered
 * before this seam existed: no metadata, no routing, publish routing, not active.
 *
 * <p><b>This replaced per-cluster-state attachment.</b> Its predecessors ({@code IndexMetadataResolver} on
 * {@code Metadata}, {@code IndexRoutingResolver} on {@code RoutingTable}) were stored on each cluster state
 * instance, propagated through every {@code Builder} copy-constructor and diff application, and seeded onto
 * from-scratch instances by a high-priority {@code ClusterStateApplier}. All of that machinery existed to
 * make a per-node callback reachable from a per-state object -- and a {@code ClusterState} never leaves the
 * node that built it, so the two scopes were always the same scope. Holding it here instead deletes the
 * propagation, the applier and the two transient fields, and makes {@link #isActive()} -- a question two
 * core call sites must ask before any {@code ClusterState} exists at all -- expressible for the first time.
 *
 * <p><b>Exception policy, deliberately not uniform.</b> {@link #shouldPublishRouting} and {@link #isActive}
 * are predicates whose safe direction is known ("publish, as an ordinary cluster always has" and "the
 * feature is off"), so a throwing catalog is treated as answering that way. The two {@code resolve} methods
 * do <em>not</em> swallow: a catalog that fails to resolve an index it owns signals that with an exception
 * (e.g. {@link DescriptorUnavailableException}) which callers already handle and must keep seeing --
 * collapsing it into {@code null} would turn "temporarily unavailable" into "does not exist", which is the
 * one confusion this whole area is built to avoid.
 *
 * <p>The thread guard is <em>not</em> here either: it stays at the two consultation points ({@link
 * Metadata#indexOrResolved(String)} and {@link ClusterState#getIndexRoutingTable(String)}), which is where
 * the decision to consult a catalog is actually made. See {@link
 * org.opensearch.cluster.ClusterStateMutationThreads} for what it prevents.
 */
public final class IndexCatalogRegistry {

    private static final AtomicReference<IndexCatalog> CATALOG = new AtomicReference<>();

    private IndexCatalogRegistry() {}

    /** Installs the catalog. Registering {@code null} clears it, which is how a test restores the default. */
    public static void register(IndexCatalog catalog) {
        CATALOG.set(catalog);
    }

    /**
     * Whether any catalog is installed on this node. A node-lifetime fact -- <b>not</b> the same question as
     * {@link #isActive()}, which is whether the feature behind an installed catalog is currently switched on;
     * see {@link IndexCatalog#isActive()} for why the distinction is load-bearing. Used for the "does this
     * read path have anything to fall back to at all" checks, where the answer with a registered-but-inactive
     * catalog is the same either way (it declines) and the cheaper check is the honest one.
     */
    public static boolean isRegistered() {
        return CATALOG.get() != null;
    }

    /** The installed catalog, or {@code null} if none is. */
    @Nullable
    public static IndexCatalog get() {
        return CATALOG.get();
    }

    /**
     * The installed catalog's metadata answer for a name that had no published entry, or {@code null} if
     * nothing is installed. See {@link IndexCatalog#resolveMetadata} -- exceptions propagate, by design.
     */
    @Nullable
    public static IndexMetadata resolveMetadata(Metadata metadata, String indexName) {
        IndexCatalog catalog = CATALOG.get();
        return catalog == null ? null : catalog.resolveMetadata(metadata, indexName);
    }

    /**
     * The installed catalog's routing answer for an index that had no published entry, or {@code null} if
     * nothing is installed. See {@link IndexCatalog#resolveRouting} -- exceptions propagate, by design.
     */
    @Nullable
    public static IndexRoutingTable resolveRouting(ClusterState state, IndexMetadata indexMetadata) {
        IndexCatalog catalog = CATALOG.get();
        return catalog == null ? null : catalog.resolveRouting(state, indexMetadata);
    }

    /**
     * Whether the installed catalog (if any) says this index should have routing published for it. {@code
     * true} when nothing is installed, when {@code indexMetadata} is {@code null}, or when the catalog
     * throws -- so a node without the feature always publishes routing, exactly today's behavior.
     */
    public static boolean shouldPublishRouting(IndexMetadata indexMetadata) {
        IndexCatalog catalog = CATALOG.get();
        if (catalog == null || indexMetadata == null) {
            return true;
        }
        try {
            return catalog.shouldPublishRouting(indexMetadata);
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * Whether the feature behind the installed catalog is currently switched on -- see {@link
     * IndexCatalog#isActive()} for the four call sites that need exactly this and why nothing else answers
     * them. {@code false} when nothing is installed or the catalog throws.
     */
    public static boolean isActive() {
        IndexCatalog catalog = CATALOG.get();
        if (catalog == null) {
            return false;
        }
        try {
            return catalog.isActive();
        } catch (Exception e) {
            return false;
        }
    }
}
