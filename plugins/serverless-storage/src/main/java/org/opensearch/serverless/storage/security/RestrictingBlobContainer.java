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
 * <p><b>Known, explicit gap, matching this section's own encryption-status pattern</b>: several
 * on-demand transport actions (shard clone, shrink, split, partition-rewrite, snapshot
 * pin/restore/release, retention-stats) still build their stores directly against the raw,
 * unrestricted container from {@code ServerlessStoragePlugin#blobContainerForDirectoryFactory}, not
 * through this class -- some of those (shard clone's own lineage deletion, in particular) genuinely
 * need delete and would need per-action review before wrapping, not a blanket application of this
 * class; each is a distinct decision left for a follow-up, not silently assumed safe.
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
