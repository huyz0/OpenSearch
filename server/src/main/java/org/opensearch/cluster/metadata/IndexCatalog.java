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
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * A plugin-supplied answer to the question core actually asks about an index that has no published entry:
 * <em>does this index exist, and where do its shards live?</em>
 *
 * <p><b>Why one interface and not two.</b> This replaces the pair {@code IndexMetadataResolver} +
 * {@code IndexRoutingResolver}, which were separate SPIs, separately registered, separately attached, and
 * -- in every core call site that mattered -- separately consulted four lines apart. {@link
 * ClusterState#getIndexRoutingTable(String)} is the clearest case: one method, behind one thread guard,
 * asking the metadata half and then the routing half of a single compound question, because routing
 * resolution <em>needs</em> the metadata half's answer as its input. The same pair recurs in {@code
 * IndicesClusterStateService}, {@code TransportReplicationAction}, {@code SnapshotsService} and {@code
 * ActiveShardCount}. Two SPIs for one question meant two of everything -- two {@code ClusterPlugin}
 * getters, two adapters over the two static registries, and a cluster-state applier whose only job was to
 * attach both -- to deliver five core invocation lines.
 *
 * <p><b>Why two methods rather than one compound return.</b> A compound {@code resolve(state, name)}
 * returning a pair (or a lazy holder) would allocate on every miss and could not actually save any work:
 * the two halves are inherently sequential, since {@link #resolveRouting} takes {@link #resolveMetadata}'s
 * result as an argument -- a bare {@code RoutingTable} does not know an index's shard count, only its
 * {@link IndexMetadata} does. So the "laziness" a pair would buy is already free here: a caller that wants
 * only metadata calls only {@link #resolveMetadata} and pays nothing for routing, and a caller that wants
 * routing must compute metadata first anyway. What matters for the merge is that both halves are one
 * interface, one registration and one implementation object -- not that they arrive in one return value.
 *
 * <p><b>Node-scoped, not cluster-state-scoped.</b> An implementation is registered once at node startup
 * via {@link org.opensearch.plugins.ClusterPlugin#getIndexCatalog()} and held by {@link
 * IndexCatalogRegistry}; it is not attached to individual {@link Metadata}/{@link
 * org.opensearch.cluster.routing.RoutingTable} instances. The predecessor SPIs were attached per cluster
 * state (with copy-constructor and diff propagation, plus a high-priority applier to seed from-scratch
 * instances) specifically to avoid node-level global state. That cost was paid for nothing: a {@code
 * ClusterState} never leaves the node that built it, so a per-state callback and a per-node one are the
 * same callback -- and {@link #isActive()}, which four core call sites genuinely need, cannot be answered
 * from a per-state attachment at all (see its own javadoc). Node scope makes that question expressible and
 * deletes the propagation machinery outright.
 *
 * <p><b>Consulted from {@link Metadata#indexOrResolved(String)}, a separate, explicitly-named method --
 * not from {@link Metadata#index(String)} itself.</b> This is a correction made mid-session after an
 * earlier version of this design, which folded catalog consultation directly into {@code index(String)}
 * for every caller, caused a real, {@code internalClusterTest}-confirmed regression: {@code
 * MetadataDeleteIndexService#deleteIndices} relies on {@code index(String)}'s null-ness as a
 * <em>distinguishing signal</em> ("is this index gated, and does it need the durable tombstone-write path")
 * rather than a plain existence check, and auto-resolving broke that distinction silently.
 *
 * <p>The practical effect: a caller that wants the fallback (the call sites migrating off the pre-existing
 * static registries, {@code AbsentIndexDescriptorSuppliers}/{@code AbsentIndexRoutingSuppliers}) must call
 * {@code indexOrResolved(String)} by name instead of the plain accessor. This is one extra word at each of
 * those call sites, in exchange for leaving every other caller of {@code index(String)} -- which is nearly
 * every read path in the codebase, the overwhelming majority never audited for the distinguishing-signal
 * pattern -- completely untouched, with no risk and no auditing required. A catalog implementation itself
 * does not need to know or care about this distinction; it only ever sees calls that already want the
 * fallback.
 *
 * <p>Absent by default, so an ordinary cluster with no such plugin installed resolves exactly as it always
 * has -- and every read path checks its published map <em>first</em>, reaching a catalog only on a miss, so
 * an ordinary index costs exactly what it always cost even on a node that does have one registered.
 *
 * <p><b>Never invoked on a cluster-state-mutation thread, by construction.</b> Both {@link
 * Metadata#indexOrResolved(String)} and {@link ClusterState#getIndexRoutingTable(String)} check {@link
 * org.opensearch.cluster.ClusterStateMutationThreads#blockingIsUnsafeOnCurrentThread()} before consulting a
 * catalog at all -- see that class's own javadoc for the deadlock this exists to prevent. This means a
 * {@code resolve*} implementation does <em>not</em> need to detect or guard against being called from one
 * of those threads itself; core already guarantees it will not be. It is, however, still on the hook for
 * every other caller, so {@code resolve*} must still answer quickly (e.g. from a catalog-owned local cache)
 * rather than perform unbounded blocking I/O on an arbitrary request thread.
 *
 * <p>A third method, {@code localShardsFor}, existed on the predecessor routing SPI for a "computed shards
 * spliced into a node's local shard list" design that production never used -- data nodes materialize
 * computed shards through the on-demand opener in {@code IndicesClusterStateService} instead -- and is not
 * carried over here.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexCatalog {

    /**
     * The metadata half. Called only when {@code metadata.getIndices().get(indexName)} already returned
     * nothing, and never from a thread where blocking would be unsafe -- see this interface's own javadoc.
     *
     * <p>Defaults to "no answer" so a catalog that only computes shard placement for indices that <em>do</em>
     * have published metadata need not stub it. That was one of the two things the SPI split used to buy,
     * and it costs one default method to keep.
     *
     * @param metadata  the {@link Metadata} the lookup missed against, for a catalog that needs additional
     *                  cluster-state context (e.g. to check whether the name collides with something
     *                  metadata already knows about).
     * @param indexName the index name that had no entry.
     * @return the resolved {@link IndexMetadata}, or {@code null} if this catalog has no answer either --
     *         which callers must treat identically to "the index does not exist," the same as any other
     *         {@link Metadata#indexOrResolved(String)} miss.
     */
    @Nullable
    default IndexMetadata resolveMetadata(Metadata metadata, String indexName) {
        return null;
    }

    /**
     * The routing half. Called only when {@code state.routingTable().index(name)} already returned nothing
     * <em>and</em> the metadata half has already produced an {@code indexMetadata} for the same name (from
     * the published map or from {@link #resolveMetadata}), and never from a thread where blocking would be
     * unsafe -- see this interface's own javadoc.
     *
     * <p>Takes a whole {@link ClusterState} rather than a {@link org.opensearch.cluster.routing.RoutingTable}
     * because real routing resolution needs the index's shard count and often the live node list, neither of
     * which a bare routing table holds -- which is also why {@link ClusterState#getIndexRoutingTable(String)},
     * not {@code RoutingTable#index(String)}, is core's consultation point.
     *
     * @return the resolved {@link IndexRoutingTable}, or {@code null} if this catalog has no answer either --
     *         callers must then fall back to their own default (typically: no shards).
     */
    @Nullable
    default IndexRoutingTable resolveRouting(ClusterState state, IndexMetadata indexMetadata) {
        return null;
    }

    /**
     * Whether {@code indexMetadata} should have a routing table published for it at all. {@code true} by
     * default (the ordinary case for every ordinary index) -- a catalog only needs to override this to say
     * {@code false} for an index whose shard placement it computes rather than publishes (e.g. a
     * scale-to-zero-style index), which is a distinct question from whether {@link #resolveRouting} can
     * currently answer for the index right now.
     *
     * <p>Dispatched from {@code RoutingTable#shouldPublishRouting(IndexMetadata)}, which lives there rather
     * than on {@code ClusterState} (unlike {@link ClusterState#getIndexRoutingTable(String)}): this question
     * only ever needs an {@link IndexMetadata}, which every real call site already has in hand, so there is
     * no reason to require a full {@code ClusterState} the way resolving an actual routing table does.
     */
    default boolean shouldPublishRouting(IndexMetadata indexMetadata) {
        return true;
    }

    /**
     * Whether the feature behind this catalog is <em>currently switched on</em> for this node. Mandatory:
     * there is no sensible default, and four core call sites depend on the answer.
     *
     * <p><b>This is deliberately not "is a catalog registered".</b> Registration is a node-lifetime fact,
     * true for as long as a plugin implements the SPI at all; this is a dynamically-toggled fact that
     * changes as the underlying feature is installed and uninstalled at runtime. An implementation must
     * therefore read live state on every call rather than return a flag captured at construction. An
     * earlier attempt to answer this by checking "is a resolver attached to this cluster state" was caught
     * by a real test: the guard fired even when the machinery the resolver wrapped was empty.
     *
     * <p>The four call sites, and why nothing else can answer them:
     * <ul>
     *   <li>{@code TransportCatShardsAction} and {@code RestAllocationAction} ask <em>before any {@link
     *       ClusterState} exists</em>, to decide what to request: metadata is the expensive part of a
     *       cluster state response, and it is needed only when an index whose routing is computed could be
     *       in the answer. A per-state attachment is unreachable at that point by definition; a node-scoped
     *       catalog is not.</li>
     *   <li>{@code BroadcastEmptiness#check} uses it to decide whether "an open index contributed no
     *       shards" is a bug worth logging or the ordinary shape of a computed-placement cluster.</li>
     *   <li>{@code ActiveShardCount#enoughShardsActive} uses it in an assertion that has always meant "an
     *       open index without a routing entry is impossible" -- true unless this feature is on.</li>
     * </ul>
     */
    boolean isActive();
}
