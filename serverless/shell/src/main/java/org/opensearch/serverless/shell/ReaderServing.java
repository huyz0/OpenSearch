/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.cluster.ClusterState;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ShardAssignment;

import java.util.Map;

/**
 * Serves shards as readers for {@link ServerlessNode}: opens or shares a reader, checks it is still current, and
 * forgets it.
 */
public final class ReaderServing {

    private final ServerlessNode node;

    ReaderServing(ServerlessNode node) {
        this.node = node;
    }

    /**
     * Serves a shard as a reader: opens the published commit without owning the shard.
     *
     * <p>Reader activation is the cheap half of the design. There is no compare-and-swap and no entry in
     * the shard-head, because a reader cannot conflict with anything — it does not write, and the node's
     * engine is read-only when it does not accept writer activation. Any number of search nodes may
     * serve the same shard at once.
     *
     * <p>Accumulating rather than replacing: a search node typically serves many shards, and each call
     * re-projects the full set so the view keeps describing everything this node holds.
     *
     * @param plane the metadata plane to read from
     * @param indexName the index to serve
     * @param shardNumber the shard to serve
     * @return the shard id now open as a reader
     * @throws Exception if the index does not exist, nothing has been published, or opening fails
     */
    public org.opensearch.core.index.shard.ShardId serveAsReader(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber
    ) throws Exception {
        return serveAsReader(plane, indexName, shardNumber, null);
    }

    /**
     * Opens a reader only for the incarnation this request resolved.
     *
     * @param plane the metadata plane
     * @param indexName the index name
     * @param shardNumber the shard number
     * @param expectedUuid the resolved incarnation, or null for an unbound control-plane caller
     * @return the shard to serve
     * @throws Exception if opening fails or the name now resolves to another incarnation
     */
    public org.opensearch.core.index.shard.ShardId serveAsReader(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber,
        String expectedUuid
    ) throws Exception {
        node.ensureStarted();
        node.adopt(plane);
        node.reconciler.setSegmentPublishers(NodePackWiring.publishers(node, plane));
        node.reconciler.setBeforePublish(ServerlessNode.widensRollupsOf(plane));
        node.reconciler.setWalStores(
            id -> plane.walStore(id.getIndexName(), id.getIndex().getUUID(), id.id()).readingWith(node.threadPool().generic())
        );
        node.reconciler.setOwnershipCheck((id, term) -> node.leases.ownsAtTerm(plane, id, term));
        final String openingKey = indexName + "#" + shardNumber;
        final org.opensearch.core.index.shard.ShardId served = openOrShareReader(plane, indexName, shardNumber, openingKey);
        requireReaderIncarnation(served, expectedUuid);
        try {
            validateReader(plane, indexName, shardNumber, served);
        } catch (ShardBehindLogException e) {
            if (e.superseded() == false || node.reconciler.isReader(served)) {
                throw e;
            }
            // The copy was on a replaced commit and has been let go: one open now is onto the current one.
            final org.opensearch.core.index.shard.ShardId reopened = openOrShareReader(plane, indexName, shardNumber, openingKey);
            requireReaderIncarnation(reopened, expectedUuid);
            validateReader(plane, indexName, shardNumber, reopened);
            return reopened;
        }
        return served;
    }

    static void requireReaderIncarnation(org.opensearch.core.index.shard.ShardId actual, String expectedUuid)
        throws StaleIncarnationException {
        if (expectedUuid != null && expectedUuid.equals(actual.getIndex().getUUID()) == false) {
            throw new StaleIncarnationException(
                new org.opensearch.core.index.shard.ShardId(actual.getIndexName(), expectedUuid, actual.id())
            );
        }
    }

    /** Refuses to let a reader serve a commit behind an acknowledged write; see {@link ReaderValidation#validateReader}. */
    void validateReader(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber,
        org.opensearch.core.index.shard.ShardId shardId
    ) throws java.io.IOException {
        node.readerValidation.validateReader(plane, indexName, shardNumber, shardId);
    }

    /**
     * Checks a shard open here before it answers, if it is open as a reader; see {@link #validateReader}.
     *
     * <p>For the paths that find a reader already open and query it directly, without going through
     * {@link #serveAsReader}: they would otherwise answer from a commit behind the log the moment its writer died.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shardNumber the shard
     * @param shardId the shard open here
     * @throws java.io.IOException {@link ShardBehindLogException} if it may not answer, or if the check could not run
     */
    public void ensureReaderCurrent(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber,
        org.opensearch.core.index.shard.ShardId shardId
    ) throws java.io.IOException {
        validateReader(plane, indexName, shardNumber, shardId);
    }

    org.opensearch.core.index.shard.ShardId openOrShareReader(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber,
        String openingKey
    ) throws Exception {
        final java.util.concurrent.CompletableFuture<org.opensearch.core.index.shard.ShardId> mine =
            new java.util.concurrent.CompletableFuture<>();
        final java.util.concurrent.CompletableFuture<org.opensearch.core.index.shard.ShardId> running = node.readerOpens.putIfAbsent(
            openingKey,
            mine
        );
        if (running != null) {
            // Someone is opening it already: their answer is this one's, success or failure.
            try {
                return running.get();
            } catch (java.util.concurrent.ExecutionException e) {
                throw e.getCause() instanceof Exception cause ? cause : e;
            }
        }
        final long describedAt = System.nanoTime();
        node.openingShards.add(openingKey);
        try {
            final IndexDescriptor descriptor = plane.describe(indexName)
                .orElseThrow(() -> new IllegalArgumentException("no such index: " + indexName));
            final org.opensearch.core.index.shard.ShardId opened = openReader(plane, indexName, shardNumber, descriptor, openingKey);
            // The read above confirmed the incarnation, so the query this open serves need not read it again.
            node.fences().confirmIncarnation(descriptor.uuid(), describedAt);
            mine.complete(opened);
            return opened;
        } catch (Exception e) {
            mine.completeExceptionally(e);
            throw e;
        } finally {
            node.openingShards.remove(openingKey);
            node.readerOpens.remove(openingKey, mine);
        }
    }

    org.opensearch.core.index.shard.ShardId openReader(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber,
        IndexDescriptor descriptor,
        String openingKey
    ) throws Exception {
        // Readers this node has since let go of are dropped from the record before this one is added,
        // or a search node that cycles through many distinct readers carries every one it ever opened
        // into every reader open for the rest of its life: the N-th open cost O(N) and descriptors of
        // deleted indices stayed projected forever.
        pruneReaderBookkeeping();
        node.served.put(indexName, descriptor);
        node.readerShards.add(new java.util.AbstractMap.SimpleEntry<>(indexName, shardNumber));

        if (node.shards().localShardOf(indexName, shardNumber) == null
            && node.reconciler.heldShards().size() >= node.nodeCapacity().admission.maxShardsHeld()
            && node.nodeCapacity().admission.makeRoom() == false) {
            // Readers used to be uncapped: a wide search burst could open thousands of shards on one
            // node. The same ceiling demand-driven writers have applies -- the configured one, counting
            // views -- and the same eviction makes room before this refuses.
            node.nodeCapacity().readerOpensRefusedAtCap.incrementAndGet();
            node.nodeCapacity().noteRefusedRecently(indexName, shardNumber);
            throw new IllegalStateException(
                "this node holds "
                    + node.reconciler.openShards().size()
                    + ServerlessNode.AT_THE_CAP
                    + indexName
                    + "["
                    + shardNumber
                    + "] was not opened"
            );
        }
        // One manifest read: the shard being opened. Every other reader's term is what was read when it
        // was opened. This used to re-read every held reader's manifest on every open -- the 500th reader
        // cost 500 reads before its own.
        //
        // Read once, and handed on: the reconciler and the directory factory each used to read the same
        // register again, so one reader open was three to four reads of one manifest. By the uuid already in
        // hand: the two-argument form resolves it with another descriptor read.
        // The register, or the newer commit its writer staged after it, asked of the writer rather than the store.
        // A commit the writer pushed a moment ago is the one to open, and it costs no read at all.
        final java.util.Optional<org.opensearch.serverless.store.CommitManifest> pushed = node.router() == null
            ? java.util.Optional.empty()
            : node.router().commitPush().takePushed(indexName, descriptor.uuid(), shardNumber);
        final java.util.Optional<org.opensearch.serverless.store.CommitManifest> manifest;
        if (pushed.isPresent()) {
            manifest = pushed;
        } else {
            final java.util.Optional<org.opensearch.serverless.store.CommitManifest> register = plane.segmentPublisher(
                indexName,
                descriptor.uuid(),
                shardNumber
            ).readManifest();
            manifest = node.router() == null
                ? register
                : org.opensearch.serverless.transport.NewestCommits.of(node).newerThan(register, indexName, descriptor.uuid(), shardNumber);
        }
        node.readerTerms.put(openingKey, manifest.map(org.opensearch.serverless.store.CommitManifest::term).orElse(1L));
        // The union of what this node serves and what it hosts, plus the reader being opened: a view
        // that described only the readers used to drop every writer index from the applied state on a
        // dual-role node.
        final ShardAssignment opening = new ShardAssignment(indexName, shardNumber, node.readerTerms.getOrDefault(openingKey, 1L));
        final ClusterState view = node.projectAndApply(java.util.List.of(descriptor), java.util.List.of(opening));
        return node.reconciler.openReader(view, indexName, shardNumber, manifest.orElse(null));
    }

    /**
     * Forgets that this node served a shard as a reader, once it no longer does.
     *
     * @param shardId the reader shard that was closed
     */
    public void forgetReader(org.opensearch.core.index.shard.ShardId shardId) {
        node.readerShards.remove(new java.util.AbstractMap.SimpleEntry<>(shardId.getIndexName(), shardId.id()));
        node.readerTerms.remove(ServerlessNode.shardKey(shardId.getIndexName(), shardId.id()));
        pruneServedIndices();
    }

    /** Drops reader records for shards no longer open here as readers, and the descriptors nothing refers to. */
    void pruneReaderBookkeeping() {
        final java.util.Set<org.opensearch.core.index.shard.ShardId> readers = node.reconciler.readerShards();
        final java.util.Set<String> openReaderKeys = new java.util.HashSet<>();
        for (org.opensearch.core.index.shard.ShardId reader : readers) {
            openReaderKeys.add(ServerlessNode.shardKey(reader.getIndexName(), reader.id()));
        }
        for (Map.Entry<String, Integer> entry : new java.util.ArrayList<>(node.readerShards)) {
            final String key = ServerlessNode.shardKey(entry.getKey(), entry.getValue());
            if (openReaderKeys.contains(key) == false && node.openingShards.contains(key) == false) {
                node.readerShards.remove(entry);
                node.readerTerms.remove(key);
            }
        }
        pruneServedIndices();
    }

    void pruneServedIndices() {
        final java.util.Set<String> referenced = new java.util.HashSet<>();
        for (Map.Entry<String, Integer> entry : node.readerShards) {
            referenced.add(entry.getKey());
        }
        node.served.keySet().removeIf(index -> referenced.contains(index) == false);
    }
}
