/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.hash.MurmurHash3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;

/**
 * Picks the bucket a new index joins, and records it there.
 *
 * <p>Indices are spread over a fixed number of stripes by their group (a tenant, say), so a tenant's indices made around
 * the same time share buckets and creates in different stripes never contend. Each stripe has one open bucket, named by
 * a generation in the stripe's register; a full bucket moves the stripe on to the next generation. Joining is one
 * compare-and-swap of the bucket's manifest, and is done before the descriptor names the bucket: the manifest is the
 * truth, so an index the manifest does not list is never served as a member.
 */
public final class BucketAssigner {

    /** How many stripes new indices are spread over. */
    public static final int STRIPES = 16;

    private static final int ATTEMPTS = 16;

    private final BucketStore store;
    private final int maxMembers;

    /**
     * Creates an assigner.
     *
     * @param store the bucket registers
     * @param maxMembers the most members one bucket holds
     */
    public BucketAssigner(BucketStore store, int maxMembers) {
        this.store = store;
        this.maxMembers = Math.max(1, maxMembers);
    }

    /** The stripe a group's indices go to. */
    static int stripe(String group) {
        final byte[] bytes = group.getBytes(StandardCharsets.UTF_8);
        return Math.floorMod(MurmurHash3.hash128(bytes, 0, bytes.length, 0L, new MurmurHash3.Hash128()).h1, STRIPES);
    }

    /** A stripe's bucket of one generation. */
    static String bucketId(int stripe, long generation) {
        return String.format(Locale.ROOT, "s%02d-%d", stripe, generation);
    }

    /**
     * Adds an index to its group's open bucket.
     *
     * @param group the group
     * @param indexName the index
     * @param indexUuid its uuid
     * @return the bucket it joined
     * @throws IOException if the registers cannot be read or written, or every attempt lost a race
     */
    public String join(String group, String indexName, String indexUuid) throws IOException {
        final int stripe = stripe(group);
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            final Optional<BucketStore.Open> open = store.readOpen(stripe);
            final long generation = open.map(BucketStore.Open::bucketGeneration).orElse(0L);
            final String bucket = bucketId(stripe, generation);
            final Optional<BucketStore.Read> read = store.read(bucket);
            final BucketManifest current = read.map(BucketStore.Read::manifest).orElseGet(() -> BucketManifest.empty(bucket));
            if (current.member(indexUuid).isPresent()) {
                return bucket;
            }
            if (current.size() >= maxMembers) {
                // Full: move the stripe on, whoever gets there first. A lost swap means another create already did.
                store.swapOpen(stripe, generation + 1, open.map(BucketStore.Open::generation).orElse(BlobRegister.ABSENT_GENERATION));
                continue;
            }
            final BucketManifest joined = current.withMember(indexUuid, new BucketManifest.Member(indexName, null));
            if (store.swap(joined, read.map(BucketStore.Read::generation).orElse(BlobRegister.ABSENT_GENERATION)).isPresent()) {
                return bucket;
            }
        }
        throw new IOException("could not join a bucket for [" + indexName + "] after " + ATTEMPTS + " attempts");
    }

    /**
     * Takes an index back out of a bucket it joined, for a create that failed after joining. Best effort: a member
     * left behind has no descriptor naming the bucket, and is never served.
     *
     * @param bucket the bucket
     * @param indexUuid the index's uuid
     * @throws IOException if the registers cannot be read or written
     */
    public void leave(String bucket, String indexUuid) throws IOException {
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            final Optional<BucketStore.Read> read = store.read(bucket);
            if (read.isEmpty() || read.get().manifest().member(indexUuid).isEmpty()) {
                return;
            }
            if (store.swap(read.get().manifest().withoutMember(indexUuid), read.get().generation()).isPresent()) {
                return;
            }
        }
    }
}
