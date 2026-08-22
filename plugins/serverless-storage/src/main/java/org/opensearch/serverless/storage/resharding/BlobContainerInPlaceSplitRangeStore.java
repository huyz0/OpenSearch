/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Optional;

/**
 * Persists {@link InPlaceSplitRangeDescriptor} as a single, fixed-name blob in an in-place split
 * child shard's own container -- same one-blob, write-once shape {@link BlobContainerShardPartitionStore}
 * uses for the older equal-count split mechanism's descriptor, kept as a separate class rather than
 * generalizing the two since they store unrelated record types (see {@link InPlaceSplitRangeDescriptor}'s
 * own javadoc for why the two split mechanisms don't share a descriptor shape).
 */
public final class BlobContainerInPlaceSplitRangeStore {

    /** Fixed name -- a shard has at most one in-place-split range descriptor, ever. */
    public static final String BLOB_NAME = "in-place-split-range";

    private final BlobContainer blobContainer;

    /**
     * Wraps a shard's own container.
     *
     * @param blobContainer the shard's own container -- the same one its manifests/head live in.
     */
    public BlobContainerInPlaceSplitRangeStore(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /**
     * Durably records this shard's in-place-split range descriptor.
     *
     * @param descriptor which parent and hash sub-range this shard serves.
     */
    public void writeDescriptor(InPlaceSplitRangeDescriptor descriptor) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        descriptor.writeTo(out);
        byte[] bytes = BytesReference.toBytes(out.bytes());
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            blobContainer.writeBlobAtomic(BLOB_NAME, in, bytes.length, true);
        }
    }

    /** Empty if this shard was never an in-place split child (the common case). */
    public Optional<InPlaceSplitRangeDescriptor> readDescriptor() throws IOException {
        try (InputStream in = blobContainer.readBlob(BLOB_NAME)) {
            return Optional.of(new InPlaceSplitRangeDescriptor(StreamInput.wrap(in.readAllBytes())));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }
}
