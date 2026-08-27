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
        final BlobContainer shardContainer = blobStore.blobContainer(RegisterMap.shardData(base, indexName, shardId));
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
     * <p>A listing plus one pass per shard. At the scale this design targets that is the wrong shape —
     * a sweep proportional to the whole population is exactly what the metadata plane exists to avoid —
     * and the right answer is a sharded sweep with each worker taking a slice by hash. This is the
     * honest small-deployment version, and it is named as such rather than presented as the design.
     *
     * @param plane the metadata plane
     * @return blob names deleted, keyed by {@code index#shard}
     * @throws IOException if listing or deleting fails
     */
    public Map<String, List<String>> collectAll(MetadataPlane plane) throws IOException {
        final Map<String, List<String>> deleted = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, org.opensearch.serverless.cluster.IndexDescriptor> index : plane.descriptors().listAll().entrySet()) {
            for (int shard = 0; shard < index.getValue().numberOfShards(); shard++) {
                final List<String> orphans = collectShard(plane, index.getKey(), shard);
                if (orphans.isEmpty() == false) {
                    deleted.put(index.getKey() + "#" + shard, orphans);
                }
            }
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
