/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.directory;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Recovers a {@link ShardDirectory} instance that has lost all its state (process restart,
 * partition reshuffle, cold start of a brand-new directory node) by walking every shard's
 * {@link ShardStateStore} rather than waiting for activity to lazily repopulate it entry by entry
 * (rfc-serverless-metadata-plane.md &sect;8/&sect;9/&sect;11's "rebuild-from-listing recovery"
 * gap). This is the one operation that legitimately needs to enumerate storage wholesale -- unlike
 * every other path in this package, which is careful never to (see {@link ShardDirectory}'s
 * javadoc) -- because there is no other way to answer "what shards exist" after total state loss.
 * It is expected to be slow and run rarely (at directory-node startup, or on operator demand), not
 * on any request path.
 *
 * <p>Only recovers <strong>writer</strong> hints, and only for shards with a currently
 * unexpired lease: {@link ShardHead} (rfc-serverless-metadata-plane.md &sect;4) only ever records
 * who holds the writer/compactor lease, not which nodes have opened the shard read-only --
 * reader-shard placement is nowhere durable, by design (readers are disposable local caches of a
 * manifest, not identified anywhere central). A rebuild can therefore only ever recover the writer
 * side of the directory; reader entries repopulate themselves the ordinary way, from the next
 * activation on whichever node picks the shard up next.
 *
 * <p>Requires a {@link BlobContainer} rooted <em>above</em> the per-shard split (i.e. the same
 * base path {@code ServerlessStoragePlugin} resolves {@code serverless_storage.base_path} to,
 * with an empty {@link org.opensearch.common.blobstore.BlobPath}) so {@link
 * BlobContainer#children()} can walk index-uuid, then shard-id, directories -- a per-shard
 * container (what every other class in this plugin is handed) has nothing left to enumerate.
 */
public final class DirectoryRebuildService {

    private final ShardDirectory shardDirectory;
    private final LongSupplier nowMillisSupplier;
    private final long directoryEntryTtlMillis;

    /**
     * Creates a rebuild service that reports recovered entries into {@code shardDirectory}.
     *
     * @param shardDirectory the directory to populate with recovered writer hints
     * @param directoryEntryTtlMillis the TTL to assign to each recovered entry
     */
    public DirectoryRebuildService(ShardDirectory shardDirectory, long directoryEntryTtlMillis) {
        this(shardDirectory, directoryEntryTtlMillis, System::currentTimeMillis);
    }

    DirectoryRebuildService(ShardDirectory shardDirectory, long directoryEntryTtlMillis, LongSupplier nowMillisSupplier) {
        this.shardDirectory = shardDirectory;
        this.directoryEntryTtlMillis = directoryEntryTtlMillis;
        this.nowMillisSupplier = nowMillisSupplier;
    }

    /**
     * Walks every {@code <indexUuid>/<shardId>} child of {@code rootContainer}, reads each
     * shard's head, and reports a {@link ShardRole#WRITER} hint into the target directory for
     * every shard whose lease is currently held and unexpired. Returns the number of entries
     * recovered. A shard whose head can't be read (corrupt register, transient I/O error) is
     * skipped rather than aborting the whole walk -- one bad shard shouldn't block recovering
     * every other one.
     *
     * @param rootContainer a blob container rooted above the per-shard split, with index-uuid
     *                      then shard-id children to walk
     * @return the number of entries recovered
     * @throws IOException if listing the root container's children fails
     */
    public int rebuildFrom(BlobContainer rootContainer) throws IOException {
        int recovered = 0;
        for (Map.Entry<String, BlobContainer> indexEntry : rootContainer.children().entrySet()) {
            String indexUuid = indexEntry.getKey();
            for (Map.Entry<String, BlobContainer> shardEntry : indexEntry.getValue().children().entrySet()) {
                Integer shardId = parseShardId(shardEntry.getKey());
                if (shardId == null) {
                    continue;
                }
                if (recoverShard(indexUuid, shardId, shardEntry.getValue())) {
                    recovered++;
                }
            }
        }
        return recovered;
    }

    private boolean recoverShard(String indexUuid, int shardId, BlobContainer shardContainer) {
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(shardContainer);
        try {
            Optional<org.opensearch.serverless.storage.shardstate.VersionedShardHead> versionedHead = shardStateStore.get(
                indexUuid,
                shardId
            );
            if (versionedHead.isEmpty()) {
                return false;
            }
            ShardHead head = versionedHead.get().head();
            long now = nowMillisSupplier.getAsLong();
            if (head.isLeaseHeldAt(now) == false) {
                return false;
            }
            shardDirectory.report(
                indexUuid,
                shardId,
                new ShardDirectoryEntry(
                    head.leaseHolderNodeId(),
                    ShardRole.WRITER,
                    head.primaryTerm(),
                    head.latestManifestGeneration(),
                    now + directoryEntryTtlMillis
                )
            );
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static Integer parseShardId(String name) {
        try {
            return Integer.valueOf(name);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
