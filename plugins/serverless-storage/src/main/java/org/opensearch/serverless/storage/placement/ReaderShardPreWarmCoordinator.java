/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.metadata.GatedIndexPrewarmer;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ComputedPlacementMembership;
import org.opensearch.cluster.routing.ComputedPlacementMembershipService;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.serverless.storage.readerengine.action.PollNowAction;
import org.opensearch.serverless.storage.readerengine.action.PollNowRequest;
import org.opensearch.serverless.storage.readerengine.action.PollNowResponse;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plan item D1 (plan-100m-index-implementation.md, Area D): "when the node set is about to change,
 * fetch the shards that will move onto their new candidates before shifting traffic."
 *
 * <p>D3 and D2 turned out already done, found rather than needing to be built: {@link WarmCandidates}
 * (via {@code ComputedRoutingTable#warmthOrderedAfterThePrimary}, a real production caller) already
 * prefers a previously-warm node among equally-eligible search-replica candidates, and
 * {@code ReaderReplicaExpansionCoordinator} already has real sustained-duration hysteresis via
 * {@code SustainedCandidateTracker} for the search-replica-count scale-up decision. Neither is what
 * this class does.
 *
 * <p>What was still missing: everything above only <em>prefers</em> a node that happens to already be
 * warm, or decides <em>how many</em> replicas an index should have -- nothing actually causes a newly
 * eligible node's local cache to warm up before a real request arrives there. This class is that
 * missing half: reacting to a {@link ComputedPlacementMembership} epoch change (the same custom-data
 * transition {@link WarmCandidates} reads {@code previousNodeIds()} from), it finds every
 * {@code (index, shard)} whose rendezvous candidate set gained a node this epoch that it didn't have
 * last epoch, and proactively polls that node for that shard via the existing {@link PollNowAction}
 * -- the same "answer only from the receiving node, fetch a newer manifest and materialize" primitive
 * {@code TransportPollNowAction} already implements for a different caller (generation-lag polling).
 * No new fetch/materialize mechanism was built; this only decides when to call the one that exists.
 *
 * <h2>Why only the elected cluster-manager acts</h2>
 *
 * Every node runs this applier (it is registered once per node, like {@link GatedIndexPrewarmer}),
 * but the pre-warm dispatch itself only happens on whichever node is currently the elected
 * cluster-manager. Without that restriction, every node in the cluster would independently compute
 * the identical newly-eligible set and each send its own {@link PollNowRequest} to the same targets --
 * a self-inflicted request storm of exactly the kind D1's own "bound the concurrency" requirement
 * exists to prevent, multiplied by cluster size before the per-tick budget below even applies.
 *
 * <h2>The per-invocation budget</h2>
 *
 * A single membership epoch change (e.g. a fleet doubling) can make many shards newly eligible for
 * many nodes at once -- S12's own 12.4%-of-shards-at-fleet-doubling figure, the number D1 exists to
 * act on. Unbounded, that is an unbounded burst of concurrent object-store reads in one moment,
 * which is precisely the "does not itself cause an object-store storm" D1 warns against. {@link
 * #maxPreWarmsPerEvent} caps how many {@link PollNowRequest}s one cluster-state event dispatches;
 * anything past the cap is simply not pre-warmed for this event and pays the ordinary cold-read cost
 * on first real access instead, which is the safe degrade this whole mechanism already tolerates
 * (see {@link PollNowRequest}'s own "best-effort" framing).
 *
 * <h2>Gated indices needed a second, differently-shaped pass</h2>
 *
 * The original version of this class enumerated only {@code event.state().metadata().indices()} --
 * every ordinary, cluster-state-visible index. A gated index has no entry there at all; that is what
 * gating means. So every gated index -- the target population this whole project exists for -- was
 * silently pre-warmed for never, at zero cost and zero benefit, which is a correctness gap wearing
 * the shape of "working fine" until someone measures it. Enumerating cluster state cannot be fixed by
 * looking harder at cluster state: there is nothing there to find, by design, and reading the
 * descriptor store to reconstruct the missing list on every membership change would be exactly the
 * population-proportional cost gating exists to avoid paying on this path.
 *
 * <p>{@link IndicesClusterStateService#onDemandOpenIndices()} is the bounded answer: every gated index
 * this node itself currently holds shards for, the identical per-node working set {@link
 * GatedIndexPrewarmer} already established as the right scope for a gated-index signal (that class
 * bounds to indices already locally open for the same reason). Because that set is inherently local
 * and small -- bounded by however many gated indices actually landed on this node, not by cluster
 * population -- the gated pass below runs on <em>every</em> node, not only the cluster manager, and
 * for the same reason does not need the manager's storm-avoidance restriction the ordinary pass
 * above still does. What it needs instead, since more than one node can hold the same gated shard: a
 * different node acting for the same shard would each independently compute the identical
 * newly-eligible target and each dispatch to it. Restricting to "only the node rendezvous currently
 * names the shard's primary candidate" ({@link #isLocalNodePrimaryCandidate}) makes that exactly one
 * dispatcher per shard, deterministically, rather than merely bounding the duplication.
 */
public final class ReaderShardPreWarmCoordinator implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(ReaderShardPreWarmCoordinator.class);

    private final Supplier<TransportService> transportServiceSupplier;
    private final Supplier<List<IndicesClusterStateService.OnDemandOpenIndex>> onDemandOpenGatedIndicesSupplier;
    private final int maxPreWarmsPerEvent;
    private volatile boolean enabled;

    /**
     * Counts real pre-warm dispatches -- incremented once per {@link PollNowRequest} actually sent,
     * not per shard merely considered. A real cluster's search results stay correct whether or not
     * this coordinator ever runs (see this class's own javadoc: it only affects latency), so a test
     * needs this counter to prove pre-warming genuinely happened, the same reasoning
     * {@code AffinityForwardingActionFilter#forwardCountForTesting} already established for the same
     * shape of problem.
     */
    private static final java.util.concurrent.atomic.AtomicLong PRE_WARM_COUNT = new java.util.concurrent.atomic.AtomicLong();

    /** @return how many times this JVM's coordinator has dispatched a pre-warm request. */
    public static long preWarmCountForTesting() {
        return PRE_WARM_COUNT.get();
    }

    /** Resets the counter between tests sharing one JVM. */
    public static void resetPreWarmCountForTesting() {
        PRE_WARM_COUNT.set(0);
    }

    /**
     * Creates a coordinator with no dependencies wired yet -- see {@link org.opensearch.serverless.storage.ServerlessStoragePlugin#setTransportService}
     * for why {@code transportServiceSupplier} is a supplier rather than a direct reference.
     *
     * @param transportServiceSupplier resolves this node's {@link TransportService} lazily.
     * @param onDemandOpenGatedIndicesSupplier resolves this node's own currently-open gated indices
     *                                         lazily -- see this class's own "gated indices needed a
     *                                         second pass" javadoc for why.
     * @param maxPreWarmsPerEvent the most {@link PollNowRequest}s dispatched per cluster-state event
     *                            this coordinator reacts to -- see this class's own "per-invocation
     *                            budget" javadoc. Values {@code <= 0} mean unlimited.
     * @param enabled whether this coordinator does anything at all -- off by default like every other
     *                mechanism in this plugin, since the real cost of this budget against a real
     *                object store has not been measured (the same discipline WAL batching's own
     *                Phase 3 default is still waiting on).
     */
    public ReaderShardPreWarmCoordinator(
        Supplier<TransportService> transportServiceSupplier,
        Supplier<List<IndicesClusterStateService.OnDemandOpenIndex>> onDemandOpenGatedIndicesSupplier,
        int maxPreWarmsPerEvent,
        boolean enabled
    ) {
        this.transportServiceSupplier = transportServiceSupplier;
        this.onDemandOpenGatedIndicesSupplier = onDemandOpenGatedIndicesSupplier;
        this.maxPreWarmsPerEvent = maxPreWarmsPerEvent;
        this.enabled = enabled;
    }

    /** Updates the enabled flag from a dynamic settings update -- see the plugin's own wiring. */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * One index this coordinator will check, from either pass -- see this class's own "gated indices
     * needed a second, differently-shaped pass" javadoc for what {@code requirePrimaryDispatcher}
     * means and why only the gated pass sets it.
     */
    private record IndexToConsider(String indexUuid, int numberOfShards, boolean requirePrimaryDispatcher) {}

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (enabled == false) {
            return;
        }

        ComputedPlacementMembership membership = ComputedPlacementMembershipService.get(event.state());
        if (membership.isEmpty() || membership.previousNodeIds().isEmpty()) {
            // No epoch to compare against yet (fresh cluster, or computed placement not installed) --
            // matches WarmCandidates#forShard's own "no previous epoch" convention: a real answer,
            // not a missing one. Nothing is "newly eligible" without a prior epoch to be new against.
            return;
        }
        // A membership object with an unchanged previousNodeIds() compared to the last time this
        // applier ran real work would mean the same epoch transition was already acted on -- but
        // ClusterStateApplier only fires on a real state transition, and previousNodeIds() only
        // changes when withNodes shifts the epoch, so reaching here with a non-empty previous epoch
        // already means something changed since the last transition. No separate dedup needed.

        TransportService transportService = transportServiceSupplier.get();
        if (transportService == null) {
            return;
        }

        List<IndexToConsider> toConsider = new ArrayList<>();
        // Ordinary indices: only from the cluster manager, and only that node contributes them to
        // this list -- see this class's own "why only the elected cluster-manager acts" javadoc.
        // Every other node's list simply omits these, rather than filtering dispatch after the fact.
        if (event.state().nodes().isLocalNodeElectedClusterManager()) {
            for (IndexMetadata indexMetadata : event.state().metadata().indices().values()) {
                if (ComputedPlacementGate.ownsIndex(indexMetadata) == false) {
                    continue;
                }
                toConsider.add(new IndexToConsider(indexMetadata.getIndexUUID(), indexMetadata.getNumberOfShards(), false));
            }
        }
        // Gated indices: every node contributes its own on-demand-opened set -- see this class's own
        // "gated indices needed a second, differently-shaped pass" javadoc.
        for (IndicesClusterStateService.OnDemandOpenIndex openIndex : onDemandOpenGatedIndicesSupplier.get()) {
            toConsider.add(new IndexToConsider(openIndex.indexUuid(), openIndex.numberOfShards(), true));
        }

        String localNodeId = event.state().nodes().getLocalNodeId();
        int dispatched = 0;
        for (IndexToConsider index : toConsider) {
            for (int shardId = 0; shardId < index.numberOfShards(); shardId++) {
                if (maxPreWarmsPerEvent > 0 && dispatched >= maxPreWarmsPerEvent) {
                    logger.debug(
                        "reader shard pre-warm budget ({}) exhausted for this cluster-state event; "
                            + "remaining newly-eligible shards will pay an ordinary cold read on first access",
                        maxPreWarmsPerEvent
                    );
                    return;
                }
                if (index.requirePrimaryDispatcher()
                    && isLocalNodePrimaryCandidate(membership, index.indexUuid(), shardId, localNodeId) == false) {
                    // Some other current candidate for this shard owns dispatching for it this epoch --
                    // see this class's own javadoc on why the gated pass needs exactly one dispatcher.
                    continue;
                }
                for (String newlyEligibleNodeId : newlyEligibleCandidates(membership, index.indexUuid(), shardId)) {
                    if (newlyEligibleNodeId.equals(localNodeId)) {
                        // The dispatcher itself, newly eligible for its own shard -- only reachable
                        // from the gated pass (the ordinary pass's dispatcher, the cluster manager, is
                        // never also its own target in any topology this plugin runs in production or
                        // test, but the gated pass's dispatcher is whichever node holds the shard, and
                        // a node opening a shard for the first time is simultaneously the primary
                        // candidate and newly eligible for it). Sending a request to yourself here
                        // would take TransportService's local fast path, which runs the receiving
                        // handler synchronously on this same thread -- still inside
                        // ClusterApplierService's own callback -- and that handler's own ActionFilters
                        // call clusterService.state(), which asserts it is never reentered from an
                        // applier callback. It is also simply unnecessary: a node that just opened this
                        // shard itself has nothing to warm from a poll to itself.
                        continue;
                    }
                    if (maxPreWarmsPerEvent > 0 && dispatched >= maxPreWarmsPerEvent) {
                        break;
                    }
                    dispatchPreWarm(transportService, event, newlyEligibleNodeId, index.indexUuid(), shardId);
                    dispatched++;
                }
            }
        }
    }

    /**
     * The nodes this epoch's rendezvous candidates include for {@code (indexUuid, shardId)} that last
     * epoch's candidates did not -- the inverse of {@link WarmCandidates#forShard}, which finds nodes
     * warm from the <em>previous</em> epoch; this finds nodes that need warming <em>for</em> the
     * current one.
     */
    static List<String> newlyEligibleCandidates(ComputedPlacementMembership membership, String indexUuid, int shardId) {
        List<String> current = RendezvousShardPlacement.candidates(membership.nodeIds(), indexUuid, shardId);
        List<String> previous = RendezvousShardPlacement.candidates(membership.previousNodeIds(), indexUuid, shardId);
        List<String> newlyEligible = new ArrayList<>();
        for (String nodeId : current) {
            if (previous.contains(nodeId) == false) {
                newlyEligible.add(nodeId);
            }
        }
        return newlyEligible;
    }

    /**
     * Whether {@code localNodeId} is this epoch's rendezvous-chosen primary for {@code (indexUuid,
     * shardId)} -- {@link ComputedRoutingTable}'s own definition of primary (candidate index zero,
     * never reordered by warmth). Only the gated pass consults this; the ordinary pass already has a
     * single dispatcher by construction (the cluster manager) and does not need a second restriction.
     */
    static boolean isLocalNodePrimaryCandidate(
        ComputedPlacementMembership membership,
        String indexUuid,
        int shardId,
        String localNodeId
    ) {
        if (localNodeId == null) {
            return false;
        }
        List<String> current = RendezvousShardPlacement.candidates(membership.nodeIds(), indexUuid, shardId);
        return current.isEmpty() == false && localNodeId.equals(current.get(0));
    }

    private static void dispatchPreWarm(
        TransportService transportService,
        ClusterChangedEvent event,
        String targetNodeId,
        String indexUuid,
        int shardId
    ) {
        org.opensearch.cluster.node.DiscoveryNode targetNode = event.state().nodes().get(targetNodeId);
        if (targetNode == null) {
            return;
        }
        PRE_WARM_COUNT.incrementAndGet();
        transportService.sendRequest(
            targetNode,
            PollNowAction.NAME,
            new PollNowRequest(indexUuid, shardId),
            new TransportResponseHandler<PollNowResponse>() {
                @Override
                public PollNowResponse read(org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
                    return new PollNowResponse(in);
                }

                @Override
                public void handleResponse(PollNowResponse response) {
                    logger.debug(
                        "pre-warmed [{}][{}] on newly-eligible node [{}]: polled={}",
                        indexUuid,
                        shardId,
                        targetNodeId,
                        response.polled()
                    );
                }

                @Override
                public void handleException(TransportException exp) {
                    // Best-effort, same as every other consumer of this signal (ReaderCacheAffinityRecorder's
                    // own "deliberately fire-and-forget" framing) -- a failed pre-warm just means the first
                    // real request pays an ordinary cold read, not a new failure mode.
                    logger.debug(
                        "pre-warm request for [{}][{}] to node [{}] failed, falling back to an ordinary cold read on first access: {}",
                        indexUuid,
                        shardId,
                        targetNodeId,
                        exp.toString()
                    );
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }
}
