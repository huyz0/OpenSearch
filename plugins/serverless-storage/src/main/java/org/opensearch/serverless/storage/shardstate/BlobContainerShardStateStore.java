/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.shardstate;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.Optional;

/**
 * {@link ShardStateStore} backed by any {@link BlobContainer} that implements
 * {@link BlobContainer#compareAndSwapRegister}/{@link BlobContainer#readRegister}
 * (rfc-serverless-metadata-plane.md &sect;4/&sect;7). This class contains no storage-backend
 * logic of its own &mdash; it only translates between {@link ShardHead}'s wire format and the
 * generic register primitive, so it works unchanged against {@code FsBlobContainer} today and
 * against any future S3/GCS/Azure {@code BlobContainer} that implements the same primitive using
 * that provider's native conditional write, with zero code duplicated per backend.
 */
public final class BlobContainerShardStateStore implements ShardStateStore {

    private final BlobContainer blobContainer;

    public BlobContainerShardStateStore(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    @Override
    public Optional<VersionedShardHead> get(String indexUuid, int shardId) throws IOException {
        return blobContainer.readRegister(registerName(indexUuid, shardId)).map(this::toVersionedShardHead);
    }

    @Override
    public CasResult compareAndSet(String indexUuid, int shardId, Optional<Long> expectedVersion, ShardHead newHead) throws IOException {
        long expectedGeneration = expectedVersion.orElse(BlobRegister.ABSENT_GENERATION);

        BytesStreamOutput out = new BytesStreamOutput();
        newHead.writeTo(out);

        BlobRegisterCasResult result = blobContainer.compareAndSwapRegister(
            registerName(indexUuid, shardId),
            expectedGeneration,
            out.bytes()
        );
        return result.applied() ? CasResult.SUCCESS : CasResult.VERSION_CONFLICT;
    }

    private VersionedShardHead toVersionedShardHead(BlobRegister register) {
        try {
            ShardHead head = new ShardHead(register.value().streamInput());
            return new VersionedShardHead(head, register.generation());
        } catch (IOException e) {
            throw new IllegalStateException("failed to deserialize shard head", e);
        }
    }

    private static String registerName(String indexUuid, int shardId) {
        return "head-" + indexUuid + "-" + shardId;
    }
}
