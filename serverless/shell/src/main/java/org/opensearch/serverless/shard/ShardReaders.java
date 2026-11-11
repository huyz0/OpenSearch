/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shard;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.store.CommitManifest;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/**
 * Opens and closes {@link ShardReconciler}'s readers and frozen views: shards that serve a published commit and
 * own nothing.
 */
final class ShardReaders {

    private final ShardReconciler reconciler;

    ShardReaders(ShardReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Opens a shard as a reader: serving the published commit, owning nothing.
     *
     * <p>No compare-and-swap, no term bump, no entry in the shard-head. That is deliberate: readers are
     * interchangeable caches — any search node can serve any shard by reading its manifest, and no coordination is involved because
     * nothing can go wrong if two of them do it at once.
     *
     * <p>The term recorded on the shard is the manifest's — the term of the writer whose commit this is
     * serving. It is not a claim of ownership; the shard needs a positive term to start at all
     * (the data plane refuses term 0), and the commit's own term is the only honest number available.
     *
     * @param view the node-local view, which must describe the index
     * @param indexName the index
     * @param shardNumber the shard
     * @return the shard id now open as a reader
     * @throws IOException if nothing has been published for the shard, or opening fails
     */
    public ShardId openReader(ClusterState view, String indexName, int shardNumber) throws IOException {
        return openReader(view, indexName, shardNumber, null);
    }

    /**
     * Opens a shard as a reader at a commit the caller has already read.
     *
     * <p>The caller that projects the view needs the manifest's term before it can project, so it has
     * read the register already; passing what it read is what makes a reader open cost one register read
     * rather than one per layer that needs it.
     *
     * @param view the node-local view, which must describe the index
     * @param indexName the index
     * @param shardNumber the shard
     * @param known the currently published commit, if the caller has read it; null to read it here
     * @return the shard id now open as a reader
     * @throws IOException if nothing has been published for the shard, or opening fails
     */
    public ShardId openReader(ClusterState view, String indexName, int shardNumber, CommitManifest known) throws IOException {
        final IndexMetadata indexMetadata = view.metadata().index(indexName);
        if (indexMetadata == null) {
            throw new IllegalStateException("cannot open a reader for " + indexName + ": the view does not describe it");
        }
        if (reconciler.publishers == null) {
            throw new IOException("cannot open a reader for " + indexName + ": no object store configured");
        }
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), shardNumber);
        if (reconciler.open.containsKey(shardId)) {
            return shardId;
        }
        final CommitManifest manifest = known != null
            ? known
            : reconciler.publishers.apply(shardId)
                .readManifest()
                .orElseThrow(
                    () -> new IOException(
                        "cannot serve "
                            + shardId
                            + " as a reader: nothing has been published for it. "
                            + "An empty result here would be indistinguishable from an empty index."
                    )
                );
        // Readers read blocks; they do not download segments. The store type is set here rather than in
        // the descriptor because it is a property of how this node is serving the shard, not of the index.
        final IndexShard opened = reconciler.opener.openAndStart(
            ShardReconciler.onBlockCache(indexMetadata),
            shardId,
            manifest.term(),
            view.nodes(),
            false,
            manifest
        );
        synchronized (reconciler.lifecycle(shardId)) {
            // Recorded as a reader only if it is still the shard open under this id. A write can take the shard
            // for this node's writer between the open and here, and a writer recorded as a reader is closed as
            // one -- by a pass that believes it holds no head -- while its head stays behind.
            if (reconciler.open.get(shardId) == opened) {
                reconciler.readers.add(shardId);
                // What it is a cache of. A reader never changes commit while it is open -- a ReadOnlyEngine is
                // opened on one and core offers no way to move it -- so the only way to notice the commit has
                // moved on is to remember which one this is and compare.
                reconciler.readerCommits.put(shardId, manifest);
            }
        }
        return shardId;
    }

    /**
     * Closes a shard only if it is still the reader the caller judged it to be.
     *
     * <p><b>Why a reader has a release of its own.</b> A reader and the writer that replaces it on this node share
     * a {@code ShardId}. A pass that decides a shard is a reader, goes to the store, and then releases by id closes
     * whatever is open under that id by then -- and a write in between makes that the writer. A writer closed as a
     * reader keeps its head: the fleet run found one answering "acquiring, retry" for as long as the node lived,
     * with an acknowledged write in its log that nobody would replay. So the check and the close are one step,
     * under the lock that registering a reader and every other release take.
     *
     * @param shardId the shard
     * @param servingCommit the commit the caller saw the reader serving, or null for any reader
     * @param reason why, for the log
     * @return what happened
     */
    public ShardReconciler.ReaderRelease releaseReader(ShardId shardId, CommitManifest servingCommit, String reason) {
        synchronized (reconciler.lifecycle(shardId)) {
            if (reconciler.readers.contains(shardId) == false
                || (servingCommit != null && reconciler.readerCommits.get(shardId) != servingCommit)) {
                return ShardReconciler.ReaderRelease.NOT_A_READER;
            }
            return reconciler.releaseShardLocked(shardId, reason, false)
                ? ShardReconciler.ReaderRelease.RELEASED
                : ShardReconciler.ReaderRelease.BUSY;
        }
    }

    /**
     * Returns the commit a reader is serving.
     *
     * @param shardId the shard
     * @return the commit it was opened at, or empty if it is not a reader
     */
    public Optional<CommitManifest> readerCommit(ShardId shardId) {
        return Optional.ofNullable(reconciler.readerCommits.get(shardId));
    }

    /**
     * Opens one shard of a frozen view, refusing a view taken over an earlier index of the same name.
     *
     * <p>A view records the uuid of the index it froze. The name it was taken under may since have been
     * deleted and created again, and the new index's first segments have the same names as the old one's
     * did — so a view opened by name against the new index would serve the new index's bytes under the old
     * view's id, or fail on a length mismatch if it was lucky. The uuid is what the view is a view of; a
     * view whose uuid is not the index's is no longer held, and is refused as such.
     *
     * @param indexMetadata the index the view was taken over, as it stands now
     * @param pit the view
     * @param shardNumber the shard
     * @param nodes the node view the shard is started against
     * @return the shard id the view was opened under
     * @throws IOException if the shard cannot be opened
     * @throws IllegalArgumentException if the view is of a different incarnation of the name, or did not
     *     freeze this shard
     */
    public ShardId openFrozenReader(IndexMetadata indexMetadata, PointInTime pit, int shardNumber, DiscoveryNodes nodes)
        throws IOException {
        if (pit.indexUuid() != null && pit.indexUuid().equals(indexMetadata.getIndexUUID()) == false) {
            throw new IllegalArgumentException(
                "the point in time ["
                    + pit.id()
                    + "] was taken over an earlier index named ["
                    + pit.index()
                    + "] that has since been deleted; it is no longer held"
            );
        }
        final CommitManifest manifest = pit.shards().get(shardNumber);
        if (manifest == null) {
            throw new IllegalArgumentException("the point in time [" + pit.id() + "] did not freeze shard " + shardNumber);
        }
        return openFrozenReader(indexMetadata, pit.id(), shardNumber, manifest, nodes);
    }

    /**
     * Opens a shard at a commit that is not the current one, under an identity of its own.
     *
     * <p><b>Why a separate identity.</b> A node holds at most one shard per {@code ShardId}, and it may
     * well already be serving the live one — the whole point of a frozen view is to read it while writing
     * continues. So the view is opened as an index whose uuid is the view's identifier, which makes it a
     * different shard as far as everything below here is concerned. The bytes still live under the real
     * index's uuid, which is why the shard carries
     * {@link org.opensearch.serverless.shell.ServerlessNode#STORAGE_UUID_SETTING}.
     *
     * <p><b>And why the manifest is passed in rather than read.</b> Reading it here would read the current
     * one, which is the thing this exists not to do.
     *
     * @param indexMetadata the index this is a view of
     * @param viewId the view's identifier, which becomes the synthetic index uuid
     * @param shardNumber the shard
     * @param manifest the commit to open
     * @param nodes the node view the shard is started against
     * @return the shard id the view was opened under
     * @throws IOException if the shard cannot be opened
     */
    public ShardId openFrozenReader(
        IndexMetadata indexMetadata,
        String viewId,
        int shardNumber,
        CommitManifest manifest,
        DiscoveryNodes nodes
    ) throws IOException {
        final org.opensearch.core.index.Index viewIndex = new org.opensearch.core.index.Index(indexMetadata.getIndex().getName(), viewId);
        final ShardId shardId = new ShardId(viewIndex, shardNumber);
        if (reconciler.open.containsKey(shardId)) {
            return shardId;
        }
        // Built from the given metadata rather than from a fresh builder, so the per-shard primary terms
        // survive. A fresh builder gave every shard but this one a term of zero, and opening the second
        // shard of a view then rewrote the first one's term downwards -- "term is only increased as part
        // of primary promotion", from a code path that never promotes anything.
        final IndexMetadata frozen = IndexMetadata.builder(indexMetadata)
            .settings(
                org.opensearch.common.settings.Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put(IndexMetadata.SETTING_INDEX_UUID, viewId)
                    .put("index.store.type", org.opensearch.serverless.shell.ServerlessNode.BLOCK_CACHE_STORE_TYPE)
                    .put(org.opensearch.serverless.shell.ServerlessNode.STORAGE_UUID_SETTING, indexMetadata.getIndexUUID())
                    .build()
            )
            .build();
        reconciler.opener.openAndStart(frozen, shardId, manifest.term(), nodes, false, manifest);
        reconciler.readers.add(shardId);
        // A view serves a commit like any other reader, so it records which one. It is exempt from the
        // staleness pass because it is a view, not because it forgot -- and the difference matters: the
        // exemption was at first protected only by this line's absence, which is an accident rather than a
        // guarantee and the canary for it could not fail.
        reconciler.readerCommits.put(shardId, manifest);
        reconciler.frozenViews.add(shardId);
        return shardId;
    }

    /**
     * Closes a frozen view, releasing the shards it opened and deleting what they left on disk.
     *
     * <p>A view's shards live under the view's own uuid, so nothing shares their directory and nothing
     * will ever open it again: the record it served is gone or expired. Left behind, every point in time
     * ever taken cost a directory — index and translog — for the life of the node's disk.
     *
     * @param viewId the view's identifier
     * @return how many shards were closed
     */
    public int closeFrozenReader(String viewId) {
        int closed = 0;
        for (ShardId shardId : Set.copyOf(reconciler.frozenViews)) {
            if (shardId.getIndex().getUUID().equals(viewId) == false) {
                continue;
            }
            // A shard a query is still paging through stays a view, so the next pass finds it and closes
            // it then; forgetting it here would leave it open and uncounted for the life of the process.
            if (reconciler.releaseShard(shardId, "the frozen view it served was released", true)) {
                reconciler.frozenViews.remove(shardId);
                closed++;
            }
        }
        return closed;
    }
}
