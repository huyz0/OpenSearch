/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

/**
 * A node's per-shard bookkeeping around its reconciler: marking a shard used, publishing it, releasing it, and
 * forgetting what the node remembered about it once it is gone.
 */
public final class NodeShards {

    private final ServerlessNode node;

    NodeShards(ServerlessNode node) {
        this.node = node;
    }

    /**
     * Records that a request touched this shard, on the clock the reconciler will compare against.
     *
     * <p><b>The plane's clock, not the wall clock, and that is not a detail.</b> Idle release compares a
     * stamp made here against the time a reconcile pass was given, and a test drives that pass from a
     * clock it controls. Stamping {@code System.currentTimeMillis()} instead makes every shard look as
     * though it were used far in the future, so nothing is ever idle — which is exactly how the first run
     * of these tests failed, with a controller that was working and a stamp that was not comparable.
     *
     * @param shardId the shard a request used
     */
    public void markUsed(org.opensearch.core.index.shard.ShardId shardId) {
        final org.opensearch.serverless.metadata.MetadataPlane plane = node.metadataPlane;
        node.reconciler.markUsed(shardId, plane == null ? System.currentTimeMillis() : plane.clock().getAsLong());
    }

    /**
     * Publishes a shard's current commit to the object store, fenced by the owning term.
     *
     * @param shardId the shard to publish
     * @param term the term this node owns the shard at
     * @return the manifest published
     * @throws java.io.IOException if the shard is not held here, or a newer term already published
     */
    public org.opensearch.serverless.store.CommitManifest publishShard(org.opensearch.core.index.shard.ShardId shardId, long term)
        throws java.io.IOException {
        node.ensureStarted();
        return node.reconciler.publish(shardId, term);
    }

    /**
     * Publishes a shard's current commit as a routine publish, which a node staging commits may stage rather than write
     * to the register.
     *
     * @param shardId the shard to publish
     * @param term the term this node owns the shard at
     * @return the manifest published
     * @throws java.io.IOException if the shard is not held here, or a newer term already published
     */
    public org.opensearch.serverless.store.CommitManifest publishRoutine(org.opensearch.core.index.shard.ShardId shardId, long term)
        throws java.io.IOException {
        node.ensureStarted();
        return node.reconciler.publish(shardId, term, true);
    }

    /**
     * Closes a shard this node has stopped owning.
     *
     * <p>Deliberately separate from {@link WriterActivation#applyTruth}: a shard is never closed because a view omitted
     * it, only because truth says ownership is gone. See {@code ShardReconciler}.
     *
     * @param shardId the shard to release
     * @param reason why, for the log
     */
    public void releaseShard(org.opensearch.core.index.shard.ShardId shardId, String reason) {
        node.ensureStarted();
        node.reconciler.releaseShard(shardId, reason);
        forgetShardRecords(shardId);
    }

    /**
     * Drops what this node remembered about a shard once it is no longer open here: the reader records
     * that would otherwise be re-projected on every open, the head it last read, and -- once no shard of
     * the index remains -- the descriptor versions it had applied.
     */
    void forgetShardRecords(org.opensearch.core.index.shard.ShardId shardId) {
        if (node.reconciler.shard(shardId) != null) {
            return;
        }
        node.readerServing.forgetReader(shardId);
        node.sightings.forgetOwner(shardId.getIndexName(), shardId.id());
        node.writerAssignments.remove(ServerlessNode.shardKey(shardId.getIndexName(), shardId.id()));
        if (node.reconciler.openShards().stream().noneMatch(open -> open.getIndexName().equals(shardId.getIndexName()))) {
            node.appliedDescriptorVersions.remove(shardId.getIndexName());
            node.hosted.remove(shardId.getIndexName());
        }
    }

    org.opensearch.core.index.shard.ShardId localShardOf(String indexName, int shardNumber) {
        return node.reconciler.openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(indexName) && s.id() == shardNumber)
            .findFirst()
            .orElse(null);
    }
}
