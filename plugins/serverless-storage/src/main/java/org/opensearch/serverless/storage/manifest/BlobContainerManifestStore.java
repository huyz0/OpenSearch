/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Persists and retrieves {@link CommitManifest}s against a real {@link BlobContainer}, using each
 * manifest's canonical {@link CommitManifest#manifestName()} as its blob name
 * (rfc-serverless-opensearch.md &sect;6.1/&sect;6.3).
 */
public final class BlobContainerManifestStore {

    private final BlobContainer blobContainer;

    public BlobContainerManifestStore(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /**
     * Writes {@code manifest} under its canonical name. Manifests are immutable, so this always
     * writes a brand new blob. Uses {@link BlobContainer#writeBlobAtomic} rather than plain
     * {@code writeBlob}: a reader must never observe a partially-written manifest.
     */
    public void writeManifest(CommitManifest manifest) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        manifest.writeTo(out);
        byte[] bytes = BytesReference.toBytes(out.bytes());
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            blobContainer.writeBlobAtomic(manifest.manifestName(), in, bytes.length, true);
        }
    }

    public CommitManifest readManifest(long primaryTerm, long generation) throws IOException {
        String name = CommitManifest.manifestName(primaryTerm, generation);
        try (InputStream in = blobContainer.readBlob(name)) {
            return new CommitManifest(StreamInput.wrap(in.readAllBytes()));
        }
    }

    /** Whether a manifest for this (primaryTerm, generation) has already been written. */
    public boolean manifestExists(long primaryTerm, long generation) throws IOException {
        return blobContainer.blobExists(CommitManifest.manifestName(primaryTerm, generation));
    }

    /**
     * Every manifest ever written to this shard's container, read and deserialized in full. This
     * is the one operation in this class that legitimately enumerates storage wholesale -- every
     * other method here is a targeted read/write by exact name -- so it is expected to be slow and
     * called rarely (retention-policy evaluation, not any request path), the same tradeoff {@link
     * org.opensearch.serverless.storage.directory.DirectoryRebuildService} already makes for the
     * same reason.
     */
    public List<CommitManifest> listManifests() throws IOException {
        List<CommitManifest> manifests = new ArrayList<>();
        for (String blobName : blobContainer.listBlobsByPrefix(CommitManifest.NAME_PREFIX).keySet()) {
            try (InputStream in = blobContainer.readBlob(blobName)) {
                manifests.add(new CommitManifest(StreamInput.wrap(in.readAllBytes())));
            }
        }
        return manifests;
    }
}
