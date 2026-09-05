/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link GcSweepStateStore} as one small blob in the shard's own container, alongside its manifests, pin
 * register and head -- the same "a shard's state lives with the shard" placement every other per-shard
 * record in this plugin uses.
 *
 * <p>Written with {@code writeBlobAtomic} so a sweep interrupted mid-write never leaves a half-read
 * observation map behind, and read with the same absent-means-empty discrimination {@link
 * org.opensearch.serverless.storage.retention.BlobContainerPinLedgerStore} makes: only {@link
 * NoSuchFileException} means "never written", and anything else is a store problem that must surface rather
 * than be mistaken for a fresh shard whose orphan clock should restart.
 *
 * <p>A blob rather than a register even though the container has registers: the state is a hint, updated by
 * a single writer per tick and safely last-writer-wins (see {@link GcSweepStateStore}'s own javadoc), and
 * registers on some backends are size-constrained in a way an observation map is not.
 */
public final class BlobContainerGcSweepStateStore implements GcSweepStateStore {

    /** Prefix of the per-shard state blob, distinct from every other name this plugin writes into a shard container. */
    public static final String BLOB_PREFIX = "gc-sweep-state-";

    /**
     * Marks the only format written so far. Present from the first version because this blob is durable and
     * is read by whatever build happens to be running after an upgrade -- the pin register learned that the
     * expensive way (see {@code BlobContainerDurablePinRegistry}'s own versioning note).
     */
    private static final int VERSION = 1;

    private final BlobContainer blobContainer;
    private final String blobName;

    /**
     * @param blobContainer the shard's own container.
     * @param indexUuid the index the shard belongs to, part of the blob name so a container shared by more
     *                  than one logical shard (as tests routinely do) never crosses their state.
     * @param shardId the shard number within that index.
     */
    public BlobContainerGcSweepStateStore(BlobContainer blobContainer, String indexUuid, int shardId) {
        this.blobContainer = blobContainer;
        this.blobName = BLOB_PREFIX + indexUuid + "-" + shardId;
    }

    @Override
    public GcSweepState read() throws IOException {
        try (InputStream in = blobContainer.readBlob(blobName); StreamInput stream = StreamInput.wrap(in.readAllBytes())) {
            int version = stream.readVInt();
            if (version != VERSION) {
                // A blob written by a newer build. Treating it as empty is the safe reading: the only cost
                // is that this sweep re-observes every orphan from scratch (a delayed deletion), whereas
                // guessing at the layout risks reading a first-observation time that is not one.
                return GcSweepState.empty();
            }
            Map<String, Long> observations = new LinkedHashMap<>();
            int count = stream.readVInt();
            for (int i = 0; i < count; i++) {
                observations.put(stream.readString(), stream.readLong());
            }
            return new GcSweepState(observations, stream.readVLong(), stream.readVLong(), stream.readLong(), stream.readLong());
        } catch (NoSuchFileException neverWritten) {
            return GcSweepState.empty();
        }
    }

    @Override
    public void write(GcSweepState state) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeVInt(VERSION);
        Map<String, Long> observations = state.firstObservedOrphanedAtMillis();
        out.writeVInt(observations.size());
        for (Map.Entry<String, Long> entry : observations.entrySet()) {
            out.writeString(entry.getKey());
            out.writeLong(entry.getValue());
        }
        out.writeVLong(state.lastSweptHeadTerm());
        out.writeVLong(state.lastSweptHeadGeneration());
        out.writeLong(state.lastSweptPinsFingerprint());
        out.writeLong(state.lastFullSweepAtMillis());
        byte[] bytes = BytesReference.toBytes(out.bytes());
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            blobContainer.writeBlobAtomic(blobName, in, bytes.length, false);
        }
    }
}
