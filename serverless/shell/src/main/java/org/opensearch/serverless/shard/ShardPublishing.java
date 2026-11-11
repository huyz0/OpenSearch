/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shard;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.serverless.store.CommitManifest;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Publishes a writer's commit for {@link ShardReconciler}: flushes it, writes its segments and manifest, and trims
 * the log it now covers.
 */
final class ShardPublishing {

    private final ShardReconciler reconciler;

    ShardPublishing(ShardReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Publishes a shard's current commit to the object store.
     *
     * @param shardId the shard
     * @param term the owning writer's term, which fences a zombie
     * @return the manifest published
     * @throws IOException if the shard is not held here, or publication fails
     */
    public CommitManifest publish(ShardId shardId, long term) throws IOException {
        final IndexShard shard = reconciler.open.get(shardId);
        if (shard == null) {
            throw new IOException("cannot publish " + shardId + ": not open on " + reconciler.localNode.getId());
        }
        if (reconciler.publishers == null) {
            throw new IOException("cannot publish " + shardId + ": no object store configured");
        }
        if (reconciler.readers.contains(shardId)) {
            // The read-only engine already makes this impossible; this refuses earlier and says why,
            // rather than surfacing as some Lucene-level complaint about a closed writer.
            throw new IOException("cannot publish " + shardId + ": it is open as a reader and owns no head");
        }
        // A commit must exist before there is anything to publish; an unflushed shard has its data only
        // in the translog, which this does not upload. Under the write guard, so the commit cannot split
        // an apply from its append -- see writeGuard for the lie that would tell.
        final ReentrantReadWriteLock.WriteLock exclusive = reconciler.guardFor(shardId).writeLock();
        final var log = reconciler.wal(shardId);
        final long walOrdinal;
        final ShardReconciler.NodeLogHooks nodeLogHooks = reconciler.nodeLog;
        final org.opensearch.serverless.store.NodeLogCommitter.Position nodeLogAt;
        exclusive.lock();
        try {
            // Where no write is between its apply and its append, as for the ordinal below.
            nodeLogAt = nodeLogHooks == null ? null : nodeLogHooks.position(shardId);
            // How far into the log this commit reaches, read where no write is between its apply and its append:
            // every record at or below it holds operations the flush below commits. A reader of a shard nobody is
            // writing compares it with what has landed, so a commit behind an acknowledged write is never served
            // as though it were current.
            walOrdinal = log == null ? -1L : log.appendedThrough(term);
            advanceGlobalCheckpoint(shard);
            shard.flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true).waitIfOngoing(true));
        } finally {
            exclusive.unlock();
        }
        final CommitManifest manifest;
        // The commit is pinned for the length of the upload. Without the pin, a periodic flush landing
        // mid-upload let the deletion policy remove a file only the older commit referenced, and the
        // upload failed on a file that had been there a moment ago.
        try (GatedCloseable<IndexCommit> commit = shard.acquireLastIndexCommit(false)) {
            // Named, so a manifest says who wrote it. A term is not an identity: the publish fence refuses
            // a newer term and cannot tell two nodes holding the same one apart.
            final org.opensearch.serverless.store.PruningDigest digest = digestOf(shard, commit.get());
            // Recorded where a coordinator looks for it before the commit is published, never after: a publish
            // that fails in between leaves the record wider than what is searchable, never narrower. The hook may
            // skip the record while the writer's own mark keeps the shard from being ruled out; clearing the mark
            // then widens it.
            final ShardReconciler.BeforePublish hook = reconciler.beforePublish;
            // Even for an empty digest -- no digested fields, or one that could not be computed. Skipping it left an
            // entry holding an earlier, narrower digest while the commit grew past it; an empty one widens the entry
            // to rule nothing out.
            if (hook != null) {
                hook.accept(shardId, shard.indexSettings().getNumberOfShards(), term, digest);
            }
            manifest = reconciler.publisherCache.computeIfAbsent(shardId, reconciler.publishers)
                .publish(shard.store(), commit.get(), term, reconciler.localNode.getId(), digest, walOrdinal);
        }
        final var walForPublish = log;
        if (walForPublish != null) {
            // Only after the commit is durable in the object store. Truncation drops what the previous
            // publish saw, never what this one did -- see WalStore for why that gap is load-bearing.
            walForPublish.onPublished(term);
        }
        if (nodeLogHooks != null) {
            nodeLogHooks.published(shardId, nodeLogAt);
            // What the open replayed from the shard's own log is in this commit: the head can say that log is quiet.
            if (reconciler.shardLogQuietOnPublish.remove(shardId, term)) {
                try {
                    nodeLogHooks.shardLogQuiet(shardId, term);
                } catch (IOException | RuntimeException e) {
                    // The next owner closes the shard's log again, which costs listings, never records.
                    ShardReconciler.logger.debug("could not record that " + shardId + "'s own log is quiet", e);
                }
            }
        }
        return manifest;
    }

    /**
     * Moves a writer's global checkpoint to its local checkpoint, before a flush.
     *
     * <p>A shard here has no replicas: every acknowledged operation is in the object store's log before it is
     * acknowledged, and that log, not a replica, is what recovery reads. So everything this copy has processed is as
     * safe as it will ever be, which is what the global checkpoint means -- but nothing moved it, because nothing here
     * runs the replication actions that would. It stayed where the shard opened, and the engine's deletion policy,
     * which keeps every commit from the newest one at or below the global checkpoint, kept every commit since the
     * shard opened: local disk that never came back, and the opening commit's list of files inherited from the
     * previous term, which it read back on every new commit. When the newest manifest stopped naming those files the
     * collector deleted them, as it should, and the writer's next commit failed its engine.
     *
     * <p>Called with the write guard held, where no operation sits between apply and append, so the local
     * checkpoint is every operation the flush that follows commits.
     */
    static void advanceGlobalCheckpoint(IndexShard shard) {
        shard.updateLocalCheckpointForShard(shard.routingEntry().allocationId().getId(), shard.getLocalCheckpoint());
    }

    /**
     * The pruning digest of exactly the commit being published -- not of the searcher, which may lag or lead
     * it -- read from the commit's own segments. A digest that cannot be computed is empty, which prunes
     * nothing: the manifest is published either way.
     */
    static org.opensearch.serverless.store.PruningDigest digestOf(IndexShard shard, IndexCommit commit) {
        shard.store().incRef();
        try (org.apache.lucene.index.DirectoryReader reader = org.apache.lucene.index.DirectoryReader.open(commit)) {
            return org.opensearch.serverless.store.PruningDigest.compute(reader, name -> shard.mapperService().fieldType(name));
        } catch (Exception e) {
            ShardReconciler.logger.warn("could not compute the pruning digest of " + shard.shardId() + "; publishing without one", e);
            return org.opensearch.serverless.store.PruningDigest.EMPTY;
        } finally {
            shard.store().decRef();
        }
    }
}
