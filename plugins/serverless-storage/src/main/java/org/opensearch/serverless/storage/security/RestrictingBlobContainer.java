/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.DeleteResult;

import java.io.IOException;
import java.util.List;

/**
 * Denies delete operations on the wrapped {@link BlobContainer} (rfc-serverless-opensearch.md
 * &sect;15, "credential scoping per tier": "the compaction service needs GET+PUT but no DELETE
 * (deletion stays with GC); the GC/reconciler role is the only DELETE-capable principal").
 *
 * <p>Real per-tier credential scoping is ultimately an object-store IAM/deployment concern -- this
 * class doesn't (and can't) replace scoped cloud credentials. What it buys is defense-in-depth
 * <em>inside</em> this process, applied at each construction seam that opts in: the long-lived
 * per-shard engine construction seam in {@code ServerlessStoragePlugin#getEngineFactory} (the
 * reader, writer, and compaction paths sharing one {@link BlobContainer} instance per shard there
 * -- see that method's own comments) and the on-demand {@code
 * TransportCompactionTriggerAction#doExecute} trigger both wrap in an instance of this class with
 * deletes denied, so a bug that makes either path call {@code delete()}/{@code
 * deleteBlobsIgnoringIfNotExists} becomes a loud {@link SecurityException} instead of quietly
 * reaching the real store. {@code GcSchedulerConfig} alone is built against the unrestricted
 * underlying container, matching "the GC/reconciler role is the only DELETE-capable principal"
 * exactly.
 *
 * <p><b>Known, explicit gaps, matching this section's own encryption-status pattern</b>: (1) The
 * GET-only search-compute tier (bullet 1 of &sect;15's credential-scoping list) isn't enforced,
 * because the reader and writer paths aren't yet built against genuinely separate {@link
 * BlobContainer} instances at the engine-construction seam -- a reader shard's own {@code
 * CompactionSchedulerConfig}/{@code GcSchedulerConfig} background tasks (which must write and, for
 * GC, delete) currently share the exact same shard as query serving, so a hard GET-only wrapper
 * there would break those background tasks too; closing that gap needs the reader/writer
 * construction split itself, not just another wrapper. (2) Several other on-demand transport
 * actions (shard clone, shrink, split, partition-rewrite, snapshot pin/restore/release,
 * retention-stats) still build their stores directly against the raw, unrestricted container from
 * {@code ServerlessStoragePlugin#blobContainerForDirectoryFactory}, not through this class -- some
 * of those (shard clone's own lineage deletion, in particular) genuinely need delete and would
 * need per-action review before wrapping, not a blanket application of this class; each is a
 * distinct decision left for a follow-up, not silently assumed safe.
 */
public final class RestrictingBlobContainer extends RegisterDelegatingBlobContainer {

    private final boolean deleteAllowed;

    /**
     * Wraps {@code delegate}, denying delete operations unless {@code deleteAllowed}.
     *
     * @param delegate the real container to restrict.
     * @param deleteAllowed {@code true} to pass delete operations through unchanged (the GC/reconciler
     *                      tier); {@code false} to reject them with a {@link SecurityException}
     *                      (every other tier).
     */
    public RestrictingBlobContainer(BlobContainer delegate, boolean deleteAllowed) {
        super(delegate);
        this.deleteAllowed = deleteAllowed;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new RestrictingBlobContainer(child, deleteAllowed);
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

    private void requireDeleteAllowed() {
        if (deleteAllowed == false) {
            throw new SecurityException(
                "delete is not permitted through this credential-scoped container "
                    + "(rfc-serverless-opensearch.md section 15: deletion is reserved to the GC/reconciler role)"
            );
        }
    }
}
