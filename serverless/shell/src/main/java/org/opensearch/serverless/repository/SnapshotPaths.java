/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.repository;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.serverless.cluster.Names;

/**
 * Where snapshot repositories, snapshot records and the data a standard snapshot copies live, under a deployment's
 * base path.
 */
public final class SnapshotPaths {

    private SnapshotPaths() {}

    /**
     * Returns where snapshot repository descriptors live.
     *
     * @param base the deployment root
     * @return the repositories container
     */
    public static BlobPath repositories(BlobPath base) {
        return base.add("repositories");
    }

    /**
     * Returns the container path holding snapshot records.
     *
     * <p>Its own container, the same reasoning as {@code RegisterMap.pointsInTime}: the collector and the delete
     * path both have to list every live one, and a listing that also stepped over every repository or
     * index descriptor would make their cost depend on a population they have nothing to do with.
     *
     * @param base the deployment's base path
     * @return the snapshots container path
     */
    public static BlobPath snapshots(BlobPath base) {
        return base.add("snapshots");
    }

    /**
     * Returns the authoritative snapshot-name registers, excluded from active discovery listings.
     *
     * @param base the deployment base
     * @return snapshot state path
     */
    public static BlobPath snapshotStates(BlobPath base) {
        return base.add("snapshot-states");
    }

    /**
     * Returns one snapshot lifetime's copied data root.
     *
     * @param base the deployment base
     * @param generation the unique storage lifetime
     * @return the lifetime's storage path
     */
    public static BlobPath snapshotData(BlobPath base, String generation) {
        if (Names.looksLikeAnId(generation) == false) {
            throw new IllegalArgumentException("invalid snapshot storage generation");
        }
        return base.add("snapshot-data-v2").add(generation);
    }

    /**
     * Resolves copied shard storage, retaining name-based paths for legacy records.
     *
     * @param base the deployment base
     * @param snapshot the captured record
     * @param index the captured index name
     * @param shard the captured shard
     * @return its copied data path
     */
    public static BlobPath snapshotShardData(BlobPath base, SnapshotRecord snapshot, String index, int shard) {
        return snapshot.storageGeneration() == null
            ? snapshotShardData(base, snapshot.repo(), snapshot.name(), index, shard)
            : snapshotData(base, snapshot.storageGeneration()).add(index).add(Integer.toString(shard));
    }

    /**
     * Returns the blob name one snapshot is stored under: a repository and a name, joined the same way a
     * shard-head joins an index name and a shard number.
     *
     * @param repo the repository
     * @param name the snapshot name
     * @return the blob name
     */
    public static String snapshotKey(String repo, String name) {
        return repo + Names.KEY_SEPARATOR + name;
    }

    /**
     * Returns the legacy name-based path for one captured shard. New captures use
     * {@link #snapshotShardData(BlobPath, SnapshotRecord, String, int)} and a unique storage lifetime.
     *
     * <p>A shallow snapshot references a live shard's own blobs directly ({@code RegisterMap.shardData}), which is
     * what makes it free to take and what makes it depend on that shard's own storage continuing to exist.
     * A standard snapshot exists precisely to not depend on that: its bytes are copied here, under the
     * repository and snapshot's own names rather than any live index's, so the snapshot's lifecycle is
     * entirely its own — deleting the index it was taken from, or the garbage collector sweeping that
     * index's shard, never touches this path at all, because neither one lists it.
     *
     * @param base the deployment's base path
     * @param repo the repository
     * @param snapshot the snapshot name
     * @param indexName the captured index's name, at capture time
     * @param shardId the shard number
     * @return the container path
     */
    public static BlobPath snapshotShardData(BlobPath base, String repo, String snapshot, String indexName, int shardId) {
        return base.add("snapshot-data").add(repo + Names.KEY_SEPARATOR + snapshot).add(indexName + Names.KEY_SEPARATOR + shardId);
    }
}
