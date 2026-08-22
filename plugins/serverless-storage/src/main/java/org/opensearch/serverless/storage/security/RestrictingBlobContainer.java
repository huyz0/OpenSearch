/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Denies write and/or delete operations on the wrapped {@link BlobContainer}
 * (rfc-serverless-opensearch.md &sect;15, "credential scoping per tier": "search-compute needs
 * GET-only on data prefixes; the compaction service needs GET+PUT but no DELETE (deletion stays
 * with GC); the GC/reconciler role is the only DELETE-capable principal").
 *
 * <p>Real per-tier credential scoping is ultimately an object-store IAM/deployment concern -- this
 * class doesn't (and can't) replace scoped cloud credentials. What it buys is defense-in-depth
 * <em>inside</em> this process, applied at each construction seam that opts in: the long-lived
 * per-shard engine construction seam in {@code ServerlessStoragePlugin#getEngineFactory} wraps the
 * writer/compaction tier's stores with deletes denied (write still allowed) and, separately, a
 * reader shard's own query-serving stores with both writes and deletes denied (GET-only) -- see
 * that method's own comments for exactly which stores get which scope. The on-demand {@code
 * TransportCompactionTriggerAction#doExecute} trigger wraps with deletes denied the same way the
 * writer/compaction tier does. A bug that makes any of these paths call a denied operation becomes
 * a loud {@link SecurityException} instead of quietly reaching the real store. {@code
 * GcSchedulerConfig} alone is built against the unrestricted underlying container, matching "the
 * GC/reconciler role is the only DELETE-capable principal" exactly.
 *
 * <p>The remaining on-demand transport actions (shard clone, shrink, split, partition-rewrite,
 * snapshot pin/restore/release, index-snapshot restore's validation pass, retention-stats) are also
 * wired through this class now, each independently reviewed for whether it genuinely needs delete.
 * Eight of nine don't -- the one real delete most of this area of the codebase could reach,
 * {@code BlobContainerCloneLineageStore#deleteLineage}, is itself reachable only from {@code
 * ServerlessStoragePlugin}'s internal index-deletion listener, which correctly stays unrestricted.
 * The ninth, {@code TransportShardPartitionRewriteAction}, is a genuine exception found by exactly
 * this kind of review (an initial delete-denied wrap broke a real {@code internalClusterTest}):
 * {@code PartitionRewritePublisher#rewrite} deletes the target's now-superseded partition descriptor
 * as a required last step of its own contract, so that action's target container is left unwrapped.
 */
public final class RestrictingBlobContainer extends RegisterDelegatingBlobContainer {

    private final boolean writeAllowed;
    private final boolean deleteAllowed;

    /**
     * Wraps {@code delegate} with write allowed, denying delete operations unless {@code
     * deleteAllowed} -- the writer/compaction tier's scope ("GET+PUT but no DELETE").
     *
     * @param delegate the real container to restrict.
     * @param deleteAllowed {@code true} to pass delete operations through unchanged (the GC/reconciler
     *                      tier); {@code false} to reject them with a {@link SecurityException}
     *                      (every other tier).
     */
    public RestrictingBlobContainer(BlobContainer delegate, boolean deleteAllowed) {
        this(delegate, true, deleteAllowed);
    }

    /**
     * Wraps {@code delegate}, denying write and/or delete operations independently.
     *
     * @param delegate the real container to restrict.
     * @param writeAllowed {@code true} to pass {@code writeBlob}/{@code writeBlobAtomic}/{@code
     *                     compareAndSwapRegister} through unchanged; {@code false} to reject them
     *                     with a {@link SecurityException} (the search-compute tier's "GET-only" scope).
     * @param deleteAllowed {@code true} to pass delete operations through unchanged (the GC/reconciler
     *                      tier); {@code false} to reject them with a {@link SecurityException}
     *                      (every other tier).
     */
    public RestrictingBlobContainer(BlobContainer delegate, boolean writeAllowed, boolean deleteAllowed) {
        super(delegate);
        this.writeAllowed = writeAllowed;
        this.deleteAllowed = deleteAllowed;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new RestrictingBlobContainer(child, writeAllowed, deleteAllowed);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        requireWriteAllowed();
        super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        requireWriteAllowed();
        super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
        throws IOException {
        requireWriteAllowed();
        return super.compareAndSwapRegister(blobName, expectedGeneration, newValue);
    }

    // The three writeBlob*WithMetadata overloads are not covered by writeBlob/writeBlobAtomic's own
    // overrides above -- FilterBlobContainer (this class's superclass's superclass) passes them
    // straight through to the delegate unchanged, which would let a metadata-carrying write reach
    // the real store even when writeAllowed is false. Nothing in this plugin currently calls these
    // (verified: zero callers under plugins/serverless-storage/src/main/java), but leaving them
    // unguarded would silently break this class's own "any denied-path bug becomes a loud
    // SecurityException" contract the moment something does.

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        java.util.Map<String, String> metadata
    ) throws IOException {
        requireWriteAllowed();
        super.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata);
    }

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        java.util.Map<String, String> metadata,
        org.opensearch.cluster.metadata.CryptoMetadata cryptoMetadata
    ) throws IOException {
        requireWriteAllowed();
        super.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata, cryptoMetadata);
    }

    @Override
    public void writeBlobAtomicWithMetadata(
        String blobName,
        InputStream inputStream,
        java.util.Map<String, String> metadata,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        requireWriteAllowed();
        super.writeBlobAtomicWithMetadata(blobName, inputStream, metadata, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        requireDeleteAllowed();
        return super.delete();
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        requireDeleteAllowed();
        super.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    private void requireWriteAllowed() {
        if (writeAllowed == false) {
            throw new SecurityException(
                "write is not permitted through this credential-scoped container "
                    + "(rfc-serverless-opensearch.md section 15: the search-compute tier is GET-only)"
            );
        }
    }

    private void requireDeleteAllowed() {
        if (deleteAllowed == false) {
            throw new SecurityException(
                "delete is not permitted through this credential-scoped container "
                    + "(rfc-serverless-opensearch.md section 15: deletion is reserved to the GC/reconciler role)"
            );
        }
    }
}
