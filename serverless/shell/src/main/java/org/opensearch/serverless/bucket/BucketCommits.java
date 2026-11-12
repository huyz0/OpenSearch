/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.CommitRegister;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Members' commits, written into their bucket's one manifest: one compare-and-swap for as many members as checkpoint
 * together.
 *
 * <p>Each member's swap is judged against its own entry's version, never the bucket register's generation, so a
 * sibling's checkpoint landing first is not this member's commit moving. A lost race on the register is retried here,
 * against a fresh read; only a member whose own entry moved is told so.
 *
 * <p>Writes to one bucket are serialised on this node, so a node's members never spend a conditional PUT losing to each
 * other, and the last manifest this node wrote is kept, so a checkpoint swaps over it without reading it again. Another
 * node writing the bucket, which only happens across a takeover, costs one lost swap and one read.
 */
public final class BucketCommits {

    /** Lost races on the bucket register before giving up: each is a sibling or another owner landing first. */
    static final int MAX_ATTEMPTS = 16;

    /** One member's swap: the version it read and the commit it writes. */
    public record Swap(long expectedVersion, CommitManifest commit) {
        /**
         * Creates a swap.
         *
         * @param expectedVersion the member entry's version read, or the absent generation for a member with no commit
         * @param commit the commit to write
         */
        public Swap {
            Objects.requireNonNull(commit);
        }
    }

    private final BucketStore store;
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BucketStore.Read> written = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong registerWrites = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Creates the writer over a deployment's buckets.
     *
     * @param store the buckets' store
     */
    public BucketCommits(BucketStore store) {
        this.store = store;
    }

    /** @return conditional PUTs made on bucket registers, applied or not; for tests and the meter */
    public long registerWrites() {
        return registerWrites.get();
    }

    /**
     * Returns where one member's commit is recorded.
     *
     * @param bucketId the bucket
     * @param indexUuid the member index's uuid
     * @return its register
     */
    public CommitRegister member(String bucketId, String indexUuid) {
        BucketIds.validate(bucketId);
        Objects.requireNonNull(indexUuid);
        return new CommitRegister() {
            @Override
            public Optional<BlobRegister> read() throws IOException {
                final BucketStore.Read read = store.read(bucketId).orElseThrow(() -> notAMember(bucketId, indexUuid));
                final BucketManifest.Member member = read.manifest().member(indexUuid).orElseThrow(() -> notAMember(bucketId, indexUuid));
                if (member.commit() == null) {
                    return Optional.empty();
                }
                return Optional.of(new BlobRegister(member.version(), member.commit().toBytes()));
            }

            @Override
            public BlobRegisterCasResult compareAndSwap(long expectedGeneration, BytesReference value) throws IOException {
                final CommitManifest commit;
                try (InputStream in = value.streamInput()) {
                    commit = CommitManifest.fromStream(in);
                }
                return swapAll(bucketId, Map.of(indexUuid, new Swap(expectedGeneration, commit))).get(indexUuid);
            }

            @Override
            public String toString() {
                return "bucket[" + bucketId + "] member[" + indexUuid + "]";
            }
        };
    }

    /**
     * Writes many members' commits in one swap of their bucket's manifest. A member whose entry is no longer at the
     * version it read is left as it is and told the version it is at; the others land together or not at all.
     *
     * @param bucketId the bucket
     * @param swaps member index uuid to its swap
     * @return member index uuid to its outcome, in the order given
     * @throws IOException if the register cannot be read or written, the bucket does not exist, or the register kept
     *                     moving under this node
     */
    public Map<String, BlobRegisterCasResult> swapAll(String bucketId, Map<String, Swap> swaps) throws IOException {
        if (swaps.isEmpty()) {
            return Map.of();
        }
        synchronized (locks.computeIfAbsent(bucketId, k -> new Object())) {
            BucketStore.Read read = written.get(bucketId);
            for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
                if (read == null) {
                    read = store.read(bucketId).orElseThrow(() -> notAMember(bucketId, swaps.keySet().iterator().next()));
                }
                final Map<String, BlobRegisterCasResult> results = new LinkedHashMap<>();
                BucketManifest next = read.manifest();
                for (Map.Entry<String, Swap> swap : swaps.entrySet()) {
                    final BucketManifest.Member member = next.member(swap.getKey()).orElse(null);
                    if (member == null) {
                        // Moved out of the bucket: its own read says so, and its siblings still land.
                        results.put(swap.getKey(), BlobRegisterCasResult.conflict(BlobRegisterCasResult.UNKNOWN_GENERATION));
                        continue;
                    }
                    if (member.version() != swap.getValue().expectedVersion()) {
                        results.put(swap.getKey(), BlobRegisterCasResult.conflict(member.version()));
                        continue;
                    }
                    final BucketManifest.Member advanced = member.advance(swap.getValue().commit());
                    next = next.withMember(swap.getKey(), advanced);
                    results.put(swap.getKey(), BlobRegisterCasResult.applied(advanced.version()));
                }
                if (next == read.manifest()) {
                    // Every member had moved: a cached read may be what is stale, so only a fresh one answers.
                    if (written.remove(bucketId) != null) {
                        read = null;
                        continue;
                    }
                    return results;
                }
                registerWrites.incrementAndGet();
                final Optional<Long> generation = store.swap(next, read.generation());
                if (generation.isPresent()) {
                    written.put(bucketId, new BucketStore.Read(next, generation.get()));
                    return results;
                }
                written.remove(bucketId);
                read = null;
            }
            throw new IOException("bucket [" + bucketId + "] manifest kept moving: " + MAX_ATTEMPTS + " swaps lost");
        }
    }

    /**
     * Forgets what this node last wrote to a bucket, for a caller that changed it some other way.
     *
     * @param bucketId the bucket
     */
    public void forget(String bucketId) {
        written.remove(bucketId);
    }

    private static IOException notAMember(String bucketId, String indexUuid) {
        return new IOException("index [" + indexUuid + "] is not a member of bucket [" + bucketId + "]");
    }
}
