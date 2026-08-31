/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Reclaims segment blobs no longer referenced by a shard's published commit.
 *
 * <p>Most of what this collects is a zombie's work. A writer that lost its lease and kept going wrote
 * real files under its own term container, could never publish them, and left them there — inert, but
 * not free. Merges leave the same kind of residue: a segment superseded by a later commit stops being
 * referenced the moment the manifest moves on.
 *
 * <p><b>The rule, and it is the whole safety argument:</b> a blob is deleted only when both hold —
 *
 * <ol>
 *   <li>it lives in a term container <em>strictly older</em> than the published manifest's term, and</li>
 *   <li>the manifest does not name it.</li>
 * </ol>
 *
 * <p>Condition 1 exists because publishing is not atomic: a writer uploads files and <em>then</em>
 * swaps the manifest, so at the current term there is a window in which live files are not yet
 * referenced. Deleting them would break an in-flight publish. Older terms have no such window, because
 * a writer at an older term can no longer publish at all — that is what the fence in
 * {@link SegmentPublisher} guarantees.
 *
 * <p>Condition 2 exists because a failover <em>inherits</em> files rather than re-uploading them
 * (phase 4), so files a live commit depends on routinely live in older term containers. Collecting by
 * term alone would delete exactly the data the current writer is serving. That is the mistake this
 * class is shaped to make impossible, and the one its tests are pointed at.
 */
public final class GarbageCollector {

    private final BlobStore blobStore;
    private final org.opensearch.common.blobstore.BlobPath base;

    /**
     * Creates a collector.
     *
     * @param blobStore the backing store
     * @param base the deployment's base path
     */
    public GarbageCollector(BlobStore blobStore, org.opensearch.common.blobstore.BlobPath base) {
        this.blobStore = blobStore;
        this.base = base;
    }

    /**
     * Collects unreferenced segment blobs for one shard.
     *
     * @param plane the metadata plane, for the shard's manifest
     * @param indexName the index
     * @param shardId the shard number
     * @return the blob names deleted, qualified by their term container
     * @throws IOException if listing or deleting fails
     */
    public List<String> collectShard(MetadataPlane plane, String indexName, int shardId) throws IOException {
        final SegmentPublisher publisher = plane.segmentPublisher(indexName, shardId);
        final Optional<CommitManifest> manifest = publisher.readManifest();
        if (manifest.isEmpty()) {
            // Nothing published means nothing is safe to judge: a writer may be mid-first-publish, and
            // every file present is a candidate for the commit it is about to make.
            return List.of();
        }
        final long liveTerm = manifest.get().term();

        // Referenced as (term container, file name) pairs. A name alone is not enough: the same segment
        // name can exist in two term containers after a history bootstrap, and only one is referenced.
        final Set<String> referenced = new HashSet<>();
        for (Map.Entry<String, String> file : manifest.get().files().entrySet()) {
            referenced.add(file.getValue() + "/" + file.getKey());
        }

        final List<String> deleted = new ArrayList<>();
        final BlobContainer shardContainer = blobStore.blobContainer(plane.shardData(indexName, shardId));
        for (Map.Entry<String, BlobContainer> child : shardContainer.children().entrySet()) {
            final String termDir = child.getKey();
            final Long term = parseTerm(termDir);
            if (term == null || term >= liveTerm) {
                // Not a term container, or the live term, or a higher one. A higher term container means
                // a newer writer is mid-publish and about to become the truth; leave it entirely alone.
                continue;
            }
            final List<String> orphans = new ArrayList<>();
            for (String blobName : child.getValue().listBlobs().keySet()) {
                if (referenced.contains(termDir + "/" + blobName) == false) {
                    orphans.add(blobName);
                }
            }
            if (orphans.isEmpty() == false) {
                child.getValue().deleteBlobsIgnoringIfNotExists(orphans);
                for (String orphan : orphans) {
                    deleted.add(termDir + "/" + orphan);
                }
            }
        }
        return deleted;
    }

    /**
     * Sweeps every shard of every index.
     *
     * <p>Walks the whole deployment one page at a time via {@link #collectPage}. A sweep genuinely does
     * intend to visit everything, so being proportional to the population is correct here in a way it
     * never is on a request path — but it must still be resumable and sliceable, which is why the paged
     * form is the real one and this is a convenience over it.
     *
     * @param plane the metadata plane
     * @return blob names deleted, keyed by {@code index#shard}
     * @throws IOException if listing or deleting fails
     */
    public Map<String, List<String>> collectAll(MetadataPlane plane) throws IOException {
        final Map<String, List<String>> deleted = new java.util.LinkedHashMap<>();
        String after = null;
        do {
            after = collectPage(plane, after, 500, deleted);
        } while (after != null);
        return deleted;
    }

    /**
     * Collects one bounded slice of the deployment.
     *
     * <p>This is the shape a sweep at target scale has to have, and {@link #collectAll} is a loop over
     * it kept for small deployments and tests. A real fleet runs many workers each taking a slice, which
     * is what {@code rfc-serverless-metadata-plane.md} §6 means by control logic being many small
     * idempotent loops: nothing here is privileged, and two workers collecting the same slice at once is
     * wasteful rather than wrong.
     *
     * @param plane the metadata plane
     * @param after resume point, or null to start
     * @param limit how many indices this slice covers
     * @param into accumulates deletions, keyed by {@code index#shard}
     * @return the cursor to resume from, or null when the sweep is complete
     * @throws IOException if listing or deleting fails
     */
    public String collectPage(MetadataPlane plane, String after, int limit, Map<String, List<String>> into) throws IOException {
        final var page = plane.descriptors().listPage(after, limit);
        for (Map.Entry<String, org.opensearch.serverless.cluster.IndexDescriptor> index : page.descriptors().entrySet()) {
            for (int shard = 0; shard < index.getValue().numberOfShards(); shard++) {
                final List<String> orphans = collectShard(plane, index.getKey(), shard);
                if (orphans.isEmpty() == false) {
                    into.put(index.getKey() + "#" + shard, orphans);
                }
            }
        }
        return page.nextAfter();
    }

    /**
     * Deletes the storage of shards no index owns any more.
     *
     * <p><b>What leaves an orphan.</b> Publishing is fenced by the manifest register's term, not by the
     * shard-head, so a writer that has lost its head can still finish a publish it had already begun — and
     * land bytes after the delete that removed the index swept its container. Those bytes are under the
     * dead index's uuid, so nothing will ever read them and nothing will ever reuse that path. Without this
     * they are paid for forever.
     *
     * <p><b>Absent means absent.</b> A container is deleted only when the register for its index says the
     * index is not there, or is there under a different uuid. A register read that fails throws rather than
     * answering "absent", so a store having a bad minute cannot be mistaken for an index having been
     * deleted — which is the mistake that would turn a garbage collector into data loss.
     *
     * <p><b>A container this does not recognise is left alone.</b> Anything whose name is not
     * {@code index#uuid#shard} was not written by this system, and a sweep that deletes what it cannot
     * parse is a sweep that eventually deletes somebody else's bucket.
     *
     * <p><b>This lists every shard container in the deployment</b>, which is proportional to the population
     * and is the thing §6.3 forbids on a request path. It is allowed here for the same reason
     * {@link #collectAll} is: a sweep genuinely intends to visit everything, and nothing waits on it. What
     * it is not is resumable, unlike the per-shard sweep — the object-store listing API this is built on
     * offers children, not pages of them, so a deployment large enough for that to matter needs a listing
     * that can be sliced before this can be.
     *
     * @param plane the metadata plane
     * @return the container names deleted
     * @throws IOException if listing fails
     */
    public List<String> collectOrphanedShards(MetadataPlane plane) throws IOException {
        final BlobContainer segments = blobStore.blobContainer(base.add("segments"));
        final List<String> deleted = new ArrayList<>();
        for (Map.Entry<String, BlobContainer> child : segments.children().entrySet()) {
            final String container = child.getKey();
            // index#uuid#shard, split from the right, because an index name may contain the separator in
            // no version of this system but the uuid and shard cannot.
            final int lastSeparator = container.lastIndexOf(RegisterMap.SHARD_SEPARATOR);
            final int uuidSeparator = lastSeparator < 0 ? -1 : container.lastIndexOf(RegisterMap.SHARD_SEPARATOR, lastSeparator - 1);
            if (uuidSeparator <= 0) {
                continue;
            }
            final String indexName = container.substring(0, uuidSeparator);
            final String uuid = container.substring(uuidSeparator + 1, lastSeparator);
            try {
                Integer.parseInt(container.substring(lastSeparator + 1));
            } catch (NumberFormatException e) {
                continue;
            }

            final Optional<org.opensearch.serverless.cluster.IndexDescriptor> descriptor = plane.describe(indexName);
            if (descriptor.isPresent() && descriptor.get().uuid().equals(uuid)) {
                continue;
            }
            child.getValue().delete();
            deleted.add(container);
        }
        return deleted;
    }

    /**
     * Removes node leases that have expired.
     *
     * <p>Tidiness rather than correctness: an expired lease is already filtered out on read, so leaving
     * it changes no answer. It is collected because a deployment that has cycled through nodes for a
     * year should not have to list a year of dead ones to find the live few.
     *
     * @param plane the metadata plane
     * @param nowMillis the observer's clock
     * @return the node ids whose leases were removed
     * @throws IOException if listing or deleting fails
     */
    public List<String> collectExpiredLeases(MetadataPlane plane, long nowMillis) throws IOException {
        final BlobContainer members = blobStore.blobContainer(RegisterMap.members(base));
        final List<String> removed = new ArrayList<>();
        for (String blobName : members.listBlobsByPrefix(org.opensearch.serverless.membership.BlobLeaseMembership.LEASE_PREFIX).keySet()) {
            final String nodeId = blobName.substring(org.opensearch.serverless.membership.BlobLeaseMembership.LEASE_PREFIX.length());
            final var lease = plane.membership().read(nodeId);
            if (lease.isPresent() && lease.get().isExpiredAt(nowMillis)) {
                members.deleteBlobsIgnoringIfNotExists(List.of(blobName));
                removed.add(nodeId);
            }
        }
        return removed;
    }

    private static Long parseTerm(String containerName) {
        if (containerName.startsWith("t=") == false) {
            return null;
        }
        try {
            return Long.parseLong(containerName.substring(2));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
