/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Optional;

/**
 * Persists {@link CloneLineage} as a single, fixed-name blob in a cloned shard's own container
 * (rfc-serverless-opensearch.md &sect;14) -- one blob, not a generation-numbered series like
 * manifests, since a shard is cloned at most once and its lineage never changes afterward.
 */
public final class BlobContainerCloneLineageStore {

    /** Fixed name -- a shard has at most one lineage record, ever. */
    public static final String BLOB_NAME = "clone-lineage";

    private final BlobContainer blobContainer;

    public BlobContainerCloneLineageStore(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    public void writeLineage(CloneLineage lineage) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        lineage.writeTo(out);
        byte[] bytes = BytesReference.toBytes(out.bytes());
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            blobContainer.writeBlobAtomic(BLOB_NAME, in, bytes.length, true);
        }
    }

    /** Empty if this shard was never cloned (the common case -- every non-cloned shard has no lineage blob). */
    public Optional<CloneLineage> readLineage() throws IOException {
        try (InputStream in = blobContainer.readBlob(BLOB_NAME)) {
            return Optional.of(new CloneLineage(StreamInput.wrap(in.readAllBytes())));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }

    /** A no-op if there was never a lineage record -- deleting a clone's lineage twice, or a never-cloned shard's, must not fail. */
    public void deleteLineage() throws IOException {
        blobContainer.deleteBlobsIgnoringIfNotExists(List.of(BLOB_NAME));
    }
}
