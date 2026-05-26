/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.lucene.store.Directory;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.Collection;

/**
 * Context object containing all the parameters needed to upload segment metadata.
 */
@ExperimentalApi
public final class MetadataUploadContext {
    private final Collection<String> activeSegmentFiles;
    private final CatalogSnapshot catalogSnapshot;
    private final Directory storeDirectory;
    private final long translogGeneration;
    private final ReplicationCheckpoint checkpoint;
    private final String nodeId;
    private final CheckedFunction<CatalogSnapshot, byte[], IOException> catalogSnapshotToCommitSerializer;

    public MetadataUploadContext(
        Collection<String> activeSegmentFiles,
        CatalogSnapshot catalogSnapshot,
        Directory storeDirectory,
        long translogGeneration,
        ReplicationCheckpoint checkpoint,
        String nodeId,
        CheckedFunction<CatalogSnapshot, byte[], IOException> catalogSnapshotToCommitSerializer
    ) {
        this.activeSegmentFiles = activeSegmentFiles;
        this.catalogSnapshot = catalogSnapshot;
        this.storeDirectory = storeDirectory;
        this.translogGeneration = translogGeneration;
        this.checkpoint = checkpoint;
        this.nodeId = nodeId;
        this.catalogSnapshotToCommitSerializer = catalogSnapshotToCommitSerializer;
    }

    public Collection<String> getActiveSegmentFiles() {
        return activeSegmentFiles;
    }

    public CatalogSnapshot getCatalogSnapshot() {
        return catalogSnapshot;
    }

    public Directory getStoreDirectory() {
        return storeDirectory;
    }

    public long getTranslogGeneration() {
        return translogGeneration;
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public String getNodeId() {
        return nodeId;
    }

    public CheckedFunction<CatalogSnapshot, byte[], IOException> getCatalogSnapshotToCommitSerializer() {
        return catalogSnapshotToCommitSerializer;
    }
}
