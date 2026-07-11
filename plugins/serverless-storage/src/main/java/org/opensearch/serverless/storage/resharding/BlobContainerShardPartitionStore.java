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
 * Persists {@link ShardPartitionDescriptor} as a single, fixed-name blob in a split target
 * shard's own container -- one blob, not a generation-numbered series like manifests, since a
 * shard's partition assignment is set exactly once (at split time, by {@link ShardSplitter#split})
 * and never changes afterward, same shape as {@code BlobContainerCloneLineageStore}.
 */
public final class BlobContainerShardPartitionStore {

    /** Fixed name -- a shard has at most one partition descriptor, ever. */
    public static final String BLOB_NAME = "shard-partition";

    private final BlobContainer blobContainer;

    /**
     * Wraps a shard's own container.
     *
     * @param blobContainer the shard's own container -- the same one its manifests/head live in.
     */
    public BlobContainerShardPartitionStore(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /**
     * Durably records this shard's partition descriptor.
     *
     * @param descriptor which partition, of how many, this shard serves.
     */
    public void writeDescriptor(ShardPartitionDescriptor descriptor) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        descriptor.writeTo(out);
        byte[] bytes = BytesReference.toBytes(out.bytes());
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            blobContainer.writeBlobAtomic(BLOB_NAME, in, bytes.length, true);
        }
    }

    /** Empty if this shard was never a split target (the common case -- every non-split shard has no descriptor blob). */
    public Optional<ShardPartitionDescriptor> readDescriptor() throws IOException {
        try (InputStream in = blobContainer.readBlob(BLOB_NAME)) {
            return Optional.of(new ShardPartitionDescriptor(StreamInput.wrap(in.readAllBytes())));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }
}
