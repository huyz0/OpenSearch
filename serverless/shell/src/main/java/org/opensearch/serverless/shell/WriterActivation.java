/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ShardAssignment;

/**
 * Activates writers for {@link ServerlessNode}: applies the metadata plane's truth, takes a shard's head and opens
 * it, and opens frozen views.
 */
public final class WriterActivation {

    private final ServerlessNode node;

    WriterActivation(ServerlessNode node) {
        this.node = node;
    }

    /**
     * Opens one shard of a frozen view, so a search can read the commit it froze.
     *
     * <p>The view is opened as an index of its own — see
     * {@link org.opensearch.serverless.shard.ShardReconciler#openFrozenReader} for why it has to be — which
     * means a node can serve a frozen view of a shard it is at the same time writing to.
     *
     * @param plane the metadata plane
     * @param pit the frozen view
     * @param shardNumber the shard
     * @return the shard id the view was opened under
     * @throws Exception if the shard cannot be opened
     */
    public org.opensearch.core.index.shard.ShardId openFrozenView(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        org.opensearch.serverless.metadata.PointInTime pit,
        int shardNumber
    ) throws Exception {
        node.ensureStarted();
        node.adopt(plane);
        node.reconciler.setSegmentPublishers(NodePackWiring.publishers(node, plane));
        node.reconciler.setBeforePublish(ServerlessNode.widensRollupsOf(plane));
        node.reconciler.setWalStores(
            id -> plane.walStore(id.getIndexName(), id.getIndex().getUUID(), id.id()).readingWith(node.threadPool().generic())
        );
        node.reconciler.setOwnershipCheck((id, term) -> node.leases.ownsAtTerm(plane, id, term));
        final IndexDescriptor descriptor = plane.describe(pit.index())
            .orElseThrow(() -> new IllegalArgumentException("no such index: " + pit.index()));
        // Every shard's term, not just this one's: the view's IndexService is created once and updated as
        // each shard opens, and metadata that knew only one shard's term would push the others back to
        // zero.
        final java.util.Map<Integer, Long> terms = new java.util.HashMap<>();
        pit.shards().forEach((shard, commit) -> terms.put(shard, commit.term()));
        if (node.reconciler.heldShards().size() >= node.nodeCapacity().admission.maxShardsHeld()
            && node.nodeCapacity().admission.makeRoom() == false) {
            throw new IllegalStateException(
                "this node holds " + node.reconciler.heldShards().size() + " shards, the cap; the view was not opened here"
            );
        }
        // The reconciler checks that the view is of this incarnation of the index and that it froze this
        // shard; an IllegalArgumentException saying "no longer held" is the REST layer's 404.
        return node.reconciler.openFrozenReader(
            descriptor.toIndexMetadata(terms),
            pit,
            shardNumber,
            DiscoveryNodes.builder().add(node.localNode).localNodeId(node.localNode.getId()).build()
        );
    }

    /**
     * Projects the given truth into this node's local view, applies it, and opens every shard this node
     * owns. Idempotent.
     *
     * <p>This is the whole shell loop in one call: descriptors and shard-heads in, serving shards out.
     *
     * @param descriptors the indices this node should know about
     * @param owned the shards this node owns, per shard-heads
     * @return the shards opened by this call
     * @throws Exception if projection, application or shard opening fails
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> applyTruth(
        java.util.Collection<IndexDescriptor> descriptors,
        java.util.Collection<ShardAssignment> owned
    ) throws Exception {
        node.ensureStarted();
        // What this node hosts as a writer is exactly what it was handed; the view is that plus whatever
        // it serves as a reader, so a dual-role node's readers are not projected away by a writer sync
        // or its writers by a reader open. That used to happen: serveAsReader projected the reader set
        // alone, and the writer indices vanished from the applied state.
        node.hosted.clear();
        for (IndexDescriptor descriptor : descriptors) {
            node.hosted.put(descriptor.name(), descriptor);
        }
        for (ShardAssignment assignment : owned) {
            node.writerAssignments.put(ServerlessNode.shardKey(assignment.indexName(), assignment.shardId()), assignment);
        }
        final ClusterState view = node.projectAndApply(java.util.List.of(), owned);
        return node.reconciler.ensureOpen(view, owned);
    }

    /**
     * Reads this node's truth from the metadata plane and applies it.
     *
     * <p>This is the whole steady-state loop, and it replaces cluster-state publication: no node tells
     * this one what to serve, it reads the object store and works it out. The background reconciler runs
     * this on a timer; tests call it explicitly.
     *
     * @param plane the metadata plane to read from
     * @return the shards opened by this call
     * @throws Exception if reading, projection or shard opening fails
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> syncFrom(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        node.ensureStarted();
        node.adopt(plane);
        node.reconciler.setSegmentPublishers(NodePackWiring.publishers(node, plane));
        node.reconciler.setBeforePublish(ServerlessNode.widensRollupsOf(plane));
        node.reconciler.setWalStores(
            id -> plane.walStore(id.getIndexName(), id.getIndex().getUUID(), id.id()).readingWith(node.threadPool().generic())
        );
        node.reconciler.setOwnershipCheck((id, term) -> node.leases.ownsAtTerm(plane, id, term));
        final org.opensearch.serverless.metadata.Truth truth = plane.truthFor(node.localNode.getId(), node.localNode.getEphemeralId());

        // A node that does not accept writer activation must not open a writer shard. A search-only
        // node opens reader shards instead -- so this gate is on the role, not on "has any role".
        final java.util.Collection<ShardAssignment> assignable = node.roles.contains(ServerlessNode.ROLE_INGEST)
            ? truth.assignments()
            : java.util.List.of();
        final java.util.Set<org.opensearch.core.index.shard.ShardId> opened = applyTruth(truth.descriptors(), assignable);

        // Closing here does NOT treat absence from a view as removal, but only because of what the close
        // is keyed on -- and it used to be keyed on the wrong thing. Truth's assignment list is derived
        // from a listing of this node's claim blobs, and RegisterMap says of that listing what holds for every
        // listing: a hint, not truth. A claim blob whose write failed after the head was won, or a
        // listing that lagged, made truth omit a shard whose head still named this node, and the old
        // loop released it on that evidence alone. So a shard the listing does not vouch for is judged
        // by its head, read directly: only "the head no longer names this node" closes it. That is the
        // rule as written. Readers are skipped outright -- they hold no head and no claim, and on a
        // dual-role node every writer activation used to evict every warm reader through this loop.
        final java.util.Set<String> stillOwned = new java.util.HashSet<>();
        for (ShardAssignment assignment : assignable) {
            stillOwned.add(ServerlessNode.shardKey(assignment.indexName(), assignment.shardId()));
        }
        for (org.opensearch.core.index.shard.ShardId held : node.reconciler.openShards()) {
            if (node.reconciler.isReader(held) || stillOwned.contains(ServerlessNode.shardKey(held.getIndexName(), held.id()))) {
                continue;
            }
            final java.util.Optional<org.opensearch.serverless.metadata.ShardHead> head = plane.heads()
                .read(held.getIndexName(), held.id());
            node.sightings().noteHead(held.getIndexName(), held.id(), head.orElse(null));
            if (node.leases.namesThisNode(head, held)) {
                // The listing lagged, or the claim was never written. The head is what decides, and it
                // says this shard is ours; keep serving it.
                continue;
            }
            node.leases.releaseLost(plane, held, "the shard-head no longer names " + node.nodeName, true);
        }
        return opened;
    }

    /**
     * Takes ownership of a shard and opens it for writing.
     *
     * <p>One compare-and-swap, then the ordinary sync. A node that loses the race gets an empty result
     * rather than an exception, because losing is not an error: the winner's head is in the metadata
     * plane and the correct response is to route there, never to retry activation elsewhere.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shardNumber the shard
     * @return the shard, or empty if another node holds a live lease on it
     * @throws Exception if activation or opening fails
     */
    public java.util.Optional<org.opensearch.core.index.shard.ShardId> activateWriter(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber
    ) throws Exception {
        node.ensureStarted();
        if (node.roles.contains(ServerlessNode.ROLE_INGEST) == false) {
            throw new IllegalStateException("node " + node.nodeName + " does not accept writer activation; its roles are " + node.roles);
        }
        node.adopt(plane);
        node.reconciler.setSegmentPublishers(NodePackWiring.publishers(node, plane));
        node.reconciler.setBeforePublish(ServerlessNode.widensRollupsOf(plane));
        node.reconciler.setWalStores(
            id -> plane.walStore(id.getIndexName(), id.getIndex().getUUID(), id.id()).readingWith(node.threadPool().generic())
        );
        node.reconciler.setOwnershipCheck((id, term) -> node.leases.ownsAtTerm(plane, id, term));
        {
            // The lease first, and it has to be first. Under batched liveness a shard-head is not
            // self-describing: it names an owner, and whether that ownership is live is a fact stored in
            // the owner's node lease. A node that acquired a shard before publishing a lease would hold a
            // head that every peer reads as dead, and the shard would be stolen out from under it
            // immediately -- with both nodes believing they had won it fairly.
            //
            // The head's own stamp is a floor that covers this window, but only just: publishing the
            // lease first is what makes the invariant hold rather than merely usually hold.
            //
            // And with the lapsed-lease guard, the same one the heartbeat applies: a node whose lease
            // lapsed lets go of every writer shard before it renews, or this renewal would reopen the
            // write path on shards a successor may already have sealed.
            //
            // Unless the lease was renewed recently enough that it will outlive this activation by a wide
            // margin: renewing then costs a write on every activation and proves nothing the last renewal did
            // not. Half the TTL left, less the skew margin, is the bar -- the renewal timer runs at a third.
            final long now = plane.clock().getAsLong();
            if (now + plane.leaseTtlMillis() / 2 >= node.ownLeaseValidUntilMillis - plane.membership().skewMarginMillis()) {
                node.leases().renewLease(plane, false);
            }
            if (node.leases().ownershipUnverified) {
                // The lease lapsed, or was revoked, and another thread is re-reading the heads this node holds. Writes are
                // refused until it finishes, so a shard taken now could not be written; and waiting for it put every
                // activation thread behind that scan, on one lock, for as long as the store took to answer four hundred
                // reads -- a store slowed to seconds a request stopped a whole fleet taking any shard for an hour. The
                // activation is asked again once the scan is done.
                ServerlessNode.logger.debug(
                    "not taking {}[{}]: this node's held heads are being re-read after its lease lapsed",
                    indexName,
                    shardNumber
                );
                return java.util.Optional.empty();
            }
        }
        // The incarnation being activated, so a head left by a deleted index of the same name is not
        // mistaken for a live owner of this one. The read confirms the incarnation too, for the fence the
        // first write after the open would otherwise pay again.
        final long describedAt = System.nanoTime();
        final java.util.Optional<IndexDescriptor> described = plane.describe(indexName);
        if (described.isEmpty()) {
            return java.util.Optional.empty();
        }
        if (described.get().writableSlot(shardNumber) == false) {
            // A unit a split retired, reached by a coordinator still routing with the layout from before it: taking it
            // back would let it take writes beside the halves that replaced it. The coordinator re-reads and retries.
            ServerlessNode.logger.debug("not taking {}[{}]: it is not a live unit of the index", indexName, shardNumber);
            return java.util.Optional.empty();
        }
        final long acquiringAt = System.nanoTime();
        node.nodeCapacity().activationPhases[0].addAndGet(acquiringAt - describedAt);
        final org.opensearch.serverless.metadata.Acquisition acquisition = plane.activate(
            indexName,
            shardNumber,
            node.localNode.getId(),
            node.localNode.getEphemeralId(),
            described.get().uuid(),
            node.nodeLogEnabled
        );
        node.nodeCapacity().activationPhases[1].addAndGet(System.nanoTime() - acquiringAt);
        if (acquisition.acquired() == false) {
            // The winner's head is the freshest routing fact this node has, and it was paid for: a
            // write for this shard forwards there without another read.
            node.sightings().noteHead(indexName, shardNumber, acquisition.head());
            return java.util.Optional.empty();
        }
        // Open the one shard just won, from the head and descriptor already in hand. This used to run
        // the full sync -- a listing of this node's claims and one head read per claim -- so taking one
        // shard cost a node holding a thousand a thousand reads, and a failover storm cost the square of
        // that. The full sync still runs from the backstop, rarely, for what only it can find.
        final ShardAssignment assignment = new ShardAssignment(indexName, shardNumber, acquisition.head().term());
        // The head just won is the freshest there is: the first publish checks ownership against it rather than
        // reading it back.
        node.sightings().noteHead(indexName, shardNumber, acquisition.head());
        node.hosted.put(indexName, described.get());
        final String openingKey = ServerlessNode.shardKey(indexName, shardNumber);
        node.writerAssignments.put(openingKey, assignment);
        node.openingShards.add(openingKey);
        // The takeover fenced every older term at the swap, so the open need not list them again.
        node.reconciler.noteOlderTermsFenced(indexName, shardNumber, acquisition.head().term());
        if (node.nodeLogEnabled) {
            // Whose node logs the open replays: every node the head names as holding what was not published.
            final NodeLogWriter nodeLog = node.startup().nodeLog(plane);
            if (nodeLog != null) {
                nodeLog.noteHeadMoved(indexName, shardNumber);
            }
            node.reconciler.noteNodeLogWriters(indexName, shardNumber, acquisition.head().writers());
            // And whether the shard's own log has anything for the open to close or replay.
            node.reconciler.noteShardLogQuiet(indexName, shardNumber, acquisition.head().term(), acquisition.head().shardLogQuiet());
        }
        try {
            // Marked before the shard opens, so no coordinator rules it out while it can hold refreshed
            // documents its published digest does not describe.
            //
            // In turn, not alongside the view. Run on GENERIC beside it, the mark can wait behind the writes that are
            // themselves waiting on activations: in a fleet test a dead node's shards stopped being taken at all. And the view it
            // would have overlapped averages 14 ms of a 460 ms open.
            final long markingAt = System.nanoTime();
            plane.rollups().markOwned(indexName, described.get().uuid(), described.get().numberOfShards(), shardNumber, assignment.term());
            final long openingAt = System.nanoTime();
            node.nodeCapacity().activationPhases[2].addAndGet(openingAt - markingAt);
            final ClusterState view = node.projectAndApply(java.util.List.of(described.get()), java.util.List.of(assignment));
            node.nodeCapacity().activationPhases[4].addAndGet(System.nanoTime() - openingAt);
            node.reconciler.ensureOpen(view, java.util.List.of(assignment));
            node.nodeCapacity().activationPhases[3].addAndGet(System.nanoTime() - openingAt);
            node.nodeCapacity().activationsOpened.incrementAndGet();
            node.fences().confirmIncarnation(described.get().uuid(), describedAt);
        } catch (Exception e) {
            // The head was won and the shard could not be opened. Held and unserved, that head would
            // refuse every other node's acquisition for as long as this node's lease lives, and nothing
            // on this node would try again: a shard nobody serves and nobody is looking for. Give the
            // head back, so the next node to ask -- possibly this one -- acquires cleanly.
            node.writerAssignments.remove(ServerlessNode.shardKey(indexName, shardNumber));
            node.sightings().noteHead(indexName, shardNumber, null);
            try {
                // The head goes on naming every log it named, and this node's own: an open that failed published nothing.
                plane.heads()
                    .release(
                        indexName,
                        shardNumber,
                        node.localNode.getId(),
                        assignment.term(),
                        described.get().uuid(),
                        node.localNode.getEphemeralId()
                    );
                plane.forgetAssignment(node.localNode.getId(), indexName, shardNumber);
            } catch (Exception giveBack) {
                e.addSuppressed(giveBack);
            }
            throw e;
        } finally {
            node.openingShards.remove(openingKey);
        }
        final IndexMetadata metadata = node.clusterService.state().metadata().index(indexName);
        return metadata == null
            ? java.util.Optional.empty()
            : java.util.Optional.of(new org.opensearch.core.index.shard.ShardId(metadata.getIndex(), shardNumber));
    }
}
