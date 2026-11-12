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

    /**
     * A manifest with the register generation it was read at, and its stored size.
     *
     * @param manifest the manifest
     * @param generation the register generation
     * @param storedBytes the manifest's size as stored, or -1 when not known (a manifest this node wrote and cached)
     */
    public record Read(BucketManifest manifest, long generation, int storedBytes) {

        /**
         * A read whose stored size is not known.
         *
         * @param manifest the manifest
         * @param generation the register generation
         */
        public Read(BucketManifest manifest, long generation) {
            this(manifest, generation, -1);
        }
    }

    private final BlobStore blobStore;
    private final BlobPath base;
    private volatile ManifestCodec codec = BucketSettings.DEFAULT_MANIFEST_CODEC;
    private volatile boolean compressOnlyIfWorthIt = true;

    /**
     * Sets how manifests are written from here on; reading takes either encoding and any codec whatever this says.
     *
     * @param codec the codec, or null to write JSON, the encoding nodes without the compact one read
     * @return this
     */
    public BucketStore codec(ManifestCodec codec) {
        return codec(codec, false);
    }

    /**
     * Sets how manifests are written from here on; reading takes either encoding and any codec whatever this says.
     *
     * @param codec the codec, or null to write JSON, the encoding nodes without the compact one read
     * @param onlyIfWorthIt true to compress only manifests large enough, and shrunk enough, for it to pay
     * @return this
     */
    public BucketStore codec(ManifestCodec codec, boolean onlyIfWorthIt) {
        this.codec = codec;
        this.compressOnlyIfWorthIt = onlyIfWorthIt;
        return this;
    }

    /**
     * Sets how manifests are written from a node's settings ({@link BucketSettings#MANIFEST_CODEC}).
     *
     * @param settings the node settings
     * @return this
     */
    public BucketStore configure(org.opensearch.common.settings.Settings settings) {
        return codec(BucketSettings.manifestCodec(settings), BucketSettings.compressOnlyIfWorthIt(settings));
    }

    /** @return how manifests are written, or null for JSON */
    public ManifestCodec codec() {
        return codec;
    }

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
        final BucketManifest manifest = BucketManifest.parse(stored.get().value());
        if (manifest.bucketId().equals(bucketId) == false) {
            throw new IOException("bucket register [" + bucketId + "] holds the manifest of [" + manifest.bucketId() + "]");
        }
        return Optional.of(new Read(manifest, stored.get().generation(), stored.get().value().length()));
    }

    /**
     * Reads one member's entry of a bucket's manifest: one GET, and only that member's entry decoded.
     *
     * @param bucketId the bucket
     * @param indexUuid the member index's uuid
     * @return its entry, or empty if the bucket was never written or does not hold the index
     * @throws IOException if the register cannot be read or parsed
     */
    public Optional<BucketManifest.Member> readMember(String bucketId, String indexUuid) throws IOException {
        final Optional<BlobRegister> stored = container(bucketId).readRegister(MANIFEST);
        if (stored.isEmpty()) {
            return Optional.empty();
        }
        return BucketManifest.parseMember(stored.get().value(), indexUuid);
    }

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

    /**
     * Writes a bucket's manifest if the register is still at the generation it was read at: one conditional PUT.
     *
     * @param manifest the new manifest
     * @param expectedGeneration the generation read, or {@link BlobRegister#ABSENT_GENERATION} to create the bucket
     * @return the new generation, or empty if another change landed first
     * @throws IOException if the register cannot be written
     */
    public Optional<Long> swap(BucketManifest manifest, long expectedGeneration) throws IOException {
        final ManifestCodec writing = codec;
        final var result = container(manifest.bucketId()).compareAndSwapRegister(
            MANIFEST,
            expectedGeneration,
            writing == null ? manifest.toBytes()
                : compressOnlyIfWorthIt ? manifest.toBytesCompressingIfWorthIt(writing)
                : manifest.toBytes(writing)
        );
        return result.applied() ? Optional.of(result.currentGeneration()) : Optional.empty();
    }
}
