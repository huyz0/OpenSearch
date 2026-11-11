/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Creates, finishes, lists and deletes snapshot records, and deletes a snapshot's captured files with it. Split out
 * of {@link MetadataPlane}, which keeps the public entry points.
 */
final class SnapshotRecords {

    private final SnapshotStore snapshotStore;
    private final BlobStore blobStore;
    private final BlobPath base;
    private final MetadataPlane plane;

    SnapshotRecords(SnapshotStore snapshotStore, BlobStore blobStore, BlobPath base, MetadataPlane plane) {
        this.snapshotStore = snapshotStore;
        this.blobStore = blobStore;
        this.base = base;
        this.plane = plane;
    }

    /**
     * Reserves a snapshot name and publishes its provisional pin before capture reads any manifests.
     *
     * @param snapshot the provisional record
     * @return the reserved record carrying the unique storage lifetime required to finish or clean up
     * @throws RepositoryMissingException if the repository is absent
     * @throws SnapshotAlreadyExistsException if the name is taken
     * @throws IOException if reservation or discovery publication fails
     */
    public SnapshotRecord createSnapshot(SnapshotRecord snapshot) throws IOException {
        if (plane.describeRepository(snapshot.repo()).isEmpty()) {
            throw new RepositoryMissingException(snapshot.repo());
        }
        return snapshotStore.create(snapshot);
    }

    /**
     * Atomically completes the same capture lifetime that reserved the name.
     *
     * @param snapshot the finished record, including its reserved storage generation
     * @throws IOException if the capture changed or was deleted, or the write fails
     */
    public void finishSnapshot(SnapshotRecord snapshot) throws IOException {
        snapshotStore.finish(snapshot);
    }

    /**
     * Reads one snapshot's authoritative record.
     *
     * @param repo the repository
     * @param name the snapshot name
     * @return the record if present
     * @throws IOException if the read fails
     */
    public Optional<SnapshotRecord> snapshot(String repo, String name) throws IOException {
        return snapshotStore.read(repo, name);
    }

    /**
     * Deletes the lifetime observed at admission; a replacement under the same name is left alone.
     *
     * @param repo the repository
     * @param name the snapshot name
     * @return whether this call deleted that lifetime
     * @throws IOException if the state or marker operation fails
     */
    public boolean deleteSnapshot(String repo, String name) throws IOException {
        return deleteSnapshot(repo, name, null);
    }

    /**
     * Cleans up exactly one capture, including after capture failed and the name may have been reused.
     *
     * @param expected the reserved lifetime to delete
     * @return whether this call deleted it
     * @throws IOException if the state or marker operation fails
     */
    public boolean deleteSnapshot(SnapshotRecord expected) throws IOException {
        return deleteSnapshot(expected.repo(), expected.name(), expected);
    }

    boolean deleteSnapshot(String repo, String name, SnapshotRecord expected) throws IOException {
        final var removed = snapshotStore.remove(repo, name, expected);
        if (removed.isEmpty()) {
            return false;
        }
        final SnapshotRecord record = removed.get().snapshot();
        boolean cleared = true;
        if (record.shallow() == false) {
            if (record.storageGeneration() != null) {
                try {
                    blobStore.blobContainer(RegisterMap.snapshotData(base, record.storageGeneration())).delete();
                } catch (Exception failure) {
                    cleared = false;
                    MetadataPlane.LOGGER.warn("could not clear storage for deleted snapshot " + record.key(), failure);
                }
            } else {
                for (var index : record.indices().entrySet()) {
                    for (int shard = 0; shard < index.getValue().numberOfShards(); shard++) {
                        try {
                            blobStore.blobContainer(RegisterMap.snapshotShardData(base, record, index.getKey(), shard)).delete();
                        } catch (Exception failure) {
                            cleared = false;
                            MetadataPlane.LOGGER.warn("could not clear legacy snapshot storage for " + record.key(), failure);
                        }
                    }
                }
            }
        }
        if (cleared) {
            snapshotStore.removeMarker(removed.get());
        }
        return true;
    }

    /**
     * Lists active snapshot records within one repository, logging unreadable records.
     *
     * @param repo the repository
     * @return its active snapshots
     * @throws IOException if listing fails
     */
    public List<SnapshotRecord> listSnapshots(String repo) throws IOException {
        return snapshotStore.list(repo);
    }

    /**
     * Reads the active snapshots a collector must protect. An unreadable discovered record stops collection.
     *
     * @return the live snapshot records
     * @throws IOException if listing or establishing protection fails
     */
    public List<SnapshotRecord> liveSnapshots() throws IOException {
        return snapshotStore.live();
    }
}
