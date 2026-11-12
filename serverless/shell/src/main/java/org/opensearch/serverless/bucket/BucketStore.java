/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.ConditionalBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Where buckets live in the store, and their manifest register: {@code buckets/<id>/manifest}, with the bucket's packs
 * beside it under {@code buckets/<id>/p/}.
 *
 * <p>Reads return the register's generation with the manifest, so every change is a compare-and-swap against what was
 * read: two owners racing after a takeover cannot both land, and a member added by one change is never lost to another.
 */
public final class BucketStore {

    /** The container every bucket lives under. */
    public static final String ROOT = "buckets";

    /** The manifest register's name inside a bucket's container. */
    public static final String MANIFEST = "manifest";

    /** The folder inside a bucket's container that holds its packs. */
    public static final String PACKS = "p";

    /** A manifest with the register generation it was read at. */
    public record Read(BucketManifest manifest, long generation) {
    }

    private final BlobStore blobStore;
    private final BlobPath base;

    /**
     * Creates the bucket store over a deployment's base path.
     *
     * @param blobStore the object store
     * @param base the deployment's base path
     */
    public BucketStore(BlobStore blobStore, BlobPath base) {
        this.blobStore = blobStore;
        this.base = base;
    }

    /**
     * Returns a bucket's container path.
     *
     * @param bucketId the bucket
     * @return its path
     */
    public BlobPath bucketPath(String bucketId) {
        return base.add(ROOT).add(BucketIds.validate(bucketId));
    }

    /**
     * Returns the path a bucket's packs are written under.
     *
     * @param bucketId the bucket
     * @return its packs' path
     */
    public BlobPath packsPath(String bucketId) {
        return bucketPath(bucketId).add(PACKS);
    }

    private ConditionalBlobContainer container(String bucketId) {
        return ConditionalBlobContainer.of(blobStore.blobContainer(bucketPath(bucketId)));
    }

    /**
     * Reads a bucket's manifest: one GET.
     *
     * @param bucketId the bucket
     * @return the manifest and its generation, or empty if the bucket was never written
     * @throws IOException if the register cannot be read or parsed
     */
    public Optional<Read> read(String bucketId) throws IOException {
        final Optional<BlobRegister> stored = container(bucketId).readRegister(MANIFEST);
        if (stored.isEmpty()) {
            return Optional.empty();
        }
        try (InputStream in = stored.get().value().streamInput()) {
            final BucketManifest manifest = BucketManifest.fromStream(in);
            if (manifest.bucketId().equals(bucketId) == false) {
                throw new IOException("bucket register [" + bucketId + "] holds the manifest of [" + manifest.bucketId() + "]");
            }
            return Optional.of(new Read(manifest, stored.get().generation()));
        }
    }

    /**
     * Writes a bucket's manifest if the register is still at the generation it was read at: one conditional PUT.
     *
     * @param manifest the new manifest
     * @param expectedGeneration the generation read, or {@link BlobRegister#ABSENT_GENERATION} to create the bucket
     * @return the new generation, or empty if another change landed first
     * @throws IOException if the register cannot be written
     */
    /** Where each stripe's open bucket is recorded. */
    public static final String OPEN_ROOT = "bucket-open";

    /** A stripe's open bucket generation as read, and the register generation it was read at. */
    public record Open(long bucketGeneration, long generation) {
    }

    private ConditionalBlobContainer openContainer() {
        return ConditionalBlobContainer.of(blobStore.blobContainer(base.add(OPEN_ROOT)));
    }

    /**
     * Reads which bucket of a stripe new members join.
     *
     * @param stripe the stripe
     * @return its open bucket's generation, or empty if the stripe has none yet
     * @throws IOException if the register cannot be read
     */
    public Optional<Open> readOpen(int stripe) throws IOException {
        final Optional<BlobRegister> stored = openContainer().readRegister(Integer.toString(stripe));
        if (stored.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(new Open(Long.parseLong(stored.get().value().utf8ToString().trim()), stored.get().generation()));
        } catch (NumberFormatException e) {
            throw new IOException("malformed open bucket of stripe " + stripe, e);
        }
    }

    /**
     * Moves a stripe on to its next bucket, if no one else has.
     *
     * @param stripe the stripe
     * @param bucketGeneration the new open bucket's generation
     * @param expectedGeneration the register generation read, or the absent generation to create it
     * @return true if this swap applied
     * @throws IOException if the register cannot be written
     */
    public boolean swapOpen(int stripe, long bucketGeneration, long expectedGeneration) throws IOException {
        return openContainer().compareAndSwapRegister(
            Integer.toString(stripe),
            expectedGeneration,
            new org.opensearch.core.common.bytes.BytesArray(Long.toString(bucketGeneration))
        ).applied();
    }

    public Optional<Long> swap(BucketManifest manifest, long expectedGeneration) throws IOException {
        final var result = container(manifest.bucketId()).compareAndSwapRegister(MANIFEST, expectedGeneration, manifest.toBytes());
        return result.applied() ? Optional.of(result.currentGeneration()) : Optional.empty();
    }
}
