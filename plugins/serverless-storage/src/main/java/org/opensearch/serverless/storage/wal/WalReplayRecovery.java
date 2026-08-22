/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.security.EncryptionKeyProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches, filters, and decodes the WAL chunks a writer must replay on activation, per
 * rfc-serverless-opensearch.md &sect;6.4 -- the Java counterpart of
 * {@code plugins/serverless-storage/formal/WalReplayFencing.tla}'s verified {@code FixedReplay}
 * design: bound by <em>both</em> a term filter ({@link WalChunkReader#filterByShardAndMinimumTerm})
 * <em>and</em> a chunk-sequence cutoff, never by either alone.
 *
 * <p>This class only reads, filters, and decodes -- it returns an ordered {@code
 * List<Translog.Operation>} and does not apply anything to any engine or index itself. Actually
 * replaying that list into a fresh local Lucene index is a separate step, now wired end to end via
 * {@code Engine#engineRecoveryOperations()} (a small additive core seam) and
 * {@code IndexShard#openEngineAndRecoverFromTranslog()}, which calls it once local translog
 * recovery has completed and replays whatever it returns through the same
 * {@code applyTranslogOperation} path local recovery just used -- see
 * {@code ObjectStoreWriterEngine#replayWalOperations()}/{@code #engineRecoveryOperations()} for
 * this plugin's own wiring, and rfc-serverless-opensearch.md &sect;7.1/&sect;16 Phase 2 for the
 * full, now-closed picture (including {@code ServerlessStorageWriterFailoverIT}'s real-cluster,
 * real-kill proof).
 */
public final class WalReplayRecovery {

    private static final Logger logger = LogManager.getLogger(WalReplayRecovery.class);

    private WalReplayRecovery() {}

    /**
     * Ordered (by chunk sequence, then by within-chunk append order -- i.e. by original append
     * time) list of every operation this shard must replay to catch up from {@code
     * lastDurableWalPosition} (the previous writer's last durably-published {@link WalPosition},
     * or {@code null} for a shard with no prior manifest at all) up to, but excluding,
     * {@code activationWalPosition} (the exclusive chunk-sequence upper bound snapshotted at this
     * writer's own activation -- see {@code ObjectStoreWriterEngine#activationWalPosition}'s own
     * javadoc for what it captures and its documented residual limitation).
     *
     * @param walBlobContainer the shared blob container every node's WAL chunks are written into
     * @param indexUuid the UUID of the index this shard belongs to, used to filter records
     * @param shardId the shard replaying, used to filter records
     * @param minPrimaryTerm the term floor per {@code WalReplayFencing.tla}'s {@code ReplayFloor}
     *                       (one term back from the term this writer is activating under, so a
     *                       predecessor's legitimately-durable-but-not-yet-manifested records are
     *                       not wrongly excluded by term alone) -- see {@link
     *                       WalChunkReader#filterByShardAndMinimumTerm} for why this term filter is
     *                       necessary but, on its own, insufficient; {@code activationWalPosition}
     *                       is what closes the gap it leaves open.
     * @param lastDurableWalPosition the previous writer's last durably-published {@link WalPosition},
     *                               or {@code null} for a shard with no prior manifest at all
     * @param activationWalPosition {@code < 0} (WAL mirroring was disabled when this writer
     *                              activated) short-circuits to an empty list -- there is nothing
     *                              durable in the WAL to replay from.
     */
    public static List<Translog.Operation> replayOperations(
        BlobContainer walBlobContainer,
        String indexUuid,
        int shardId,
        long minPrimaryTerm,
        WalPosition lastDurableWalPosition,
        long activationWalPosition
    ) throws IOException {
        return replayOperations(walBlobContainer, indexUuid, shardId, minPrimaryTerm, lastDurableWalPosition, activationWalPosition, null);
    }

    /**
     * Same as the six-argument overload, with one addition: {@code encryptionKeyProvider}, the
     * same provider {@code ObjectStoreWriterEngine} passes to {@link EncryptingWalChunkService} on
     * the write side. {@code null} means encryption is off, matching every record read back as
     * plaintext already. Non-{@code null} means every record read back must be decrypted via
     * {@link org.opensearch.serverless.storage.wal.WalRecordCrypto#decryptAll} before being handed
     * to {@link Translog.Operation#readOperation} -- {@link EncryptingWalChunkService}'s own
     * javadoc already documents this as the reader's responsibility, but nothing on this path
     * actually did it until now: a record written through {@link EncryptingWalChunkService} has a
     * ciphertext payload, and decoding ciphertext directly as a serialized {@link Translog.Operation}
     * fails with an opaque deserialization error (a wrong-type byte read as ciphertext), not a
     * clean, recognizable one.
     *
     * @param walBlobContainer the shared blob container every node's WAL chunks are written into
     * @param indexUuid the UUID of the index this shard belongs to, used to filter records
     * @param shardId the shard replaying, used to filter records
     * @param minPrimaryTerm the term floor per {@code WalReplayFencing.tla}'s {@code ReplayFloor}
     *                       (one term back from the term this writer is activating under, so a
     *                       predecessor's legitimately-durable-but-not-yet-manifested records are
     *                       not wrongly excluded by term alone) -- see {@link
     *                       WalChunkReader#filterByShardAndMinimumTerm} for why this term filter is
     *                       necessary but, on its own, insufficient; {@code activationWalPosition}
     *                       is what closes the gap it leaves open.
     * @param lastDurableWalPosition the previous writer's last durably-published {@link WalPosition},
     *                               or {@code null} for a shard with no prior manifest at all
     * @param activationWalPosition {@code < 0} (WAL mirroring was disabled when this writer
     *                              activated) short-circuits to an empty list -- there is nothing
     *                              durable in the WAL to replay from.
     * @param encryptionKeyProvider {@code null} if WAL records are not encrypted; otherwise the
     *                              same provider used to encrypt them, needed to decrypt them back.
     */
    public static List<Translog.Operation> replayOperations(
        BlobContainer walBlobContainer,
        String indexUuid,
        int shardId,
        long minPrimaryTerm,
        WalPosition lastDurableWalPosition,
        long activationWalPosition,
        EncryptionKeyProvider encryptionKeyProvider
    ) throws IOException {
        if (activationWalPosition < 0) {
            return List.of();
        }
        long fromChunkSequenceInclusive = lastDurableWalPosition == null ? 0 : lastDurableWalPosition.offset() + 1;
        if (fromChunkSequenceInclusive >= activationWalPosition) {
            return List.of();
        }

        List<Long> chunkSequences = listChunkSequencesInRange(walBlobContainer, fromChunkSequenceInclusive, activationWalPosition);
        List<Translog.Operation> operations = new ArrayList<>();
        for (int i = 0; i < chunkSequences.size(); i++) {
            long chunkSequence = chunkSequences.get(i);
            byte[] chunkBytes = readChunkBytes(walBlobContainer, chunkSequence);
            List<WalRecord> rawRecords;
            try {
                rawRecords = WalChunkReader.readRecords(chunkBytes);
            } catch (WalFormatException e) {
                // A torn/corrupt chunk at the very end of the range this writer would replay is
                // indistinguishable from an in-flight write that crashed mid-append (the last chunk
                // sequence a predecessor was writing when it died, never fully flushed) -- treat it
                // as "nothing more was durably written," not a fatal error, and stop replay here
                // rather than failing the whole recovery over a write this shard's own activation
                // never depended on completing. A torn chunk anywhere else in the range is real
                // corruption of a chunk some LATER chunk was written after, so it must fail loudly.
                if (i == chunkSequences.size() - 1) {
                    logger.warn(
                        "WAL chunk {} (the last chunk in this replay range) is corrupt or truncated -- "
                            + "treating it as an incomplete tail write and stopping replay there: {}",
                        chunkSequence,
                        e
                    );
                    break;
                }
                throw e;
            }
            List<WalRecord> records = WalChunkReader.filterByShardAndMinimumTerm(rawRecords, indexUuid, shardId, minPrimaryTerm);
            if (encryptionKeyProvider != null) {
                records = WalRecordCrypto.decryptAll(records, encryptionKeyProvider);
            }
            for (WalRecord record : records) {
                operations.add(Translog.Operation.readOperation(StreamInput.wrap(record.payload())));
            }
        }
        return operations;
    }

    /**
     * Package-visible for direct testing of the listing/range logic without needing real chunk bytes.
     *
     * <p>Probes each candidate sequence in {@code [fromInclusive, uptoExclusive)} directly via
     * {@link BlobContainer#blobExists}, rather than listing the entire shared container and
     * filtering client-side. Chunk sequences are a single counter shared across every shard using
     * this container (see {@code WalChunkService#claimNextChunkSequence}), so a full listing costs
     * O(every live chunk from every shard sharing this container, ever written and not yet WAL-GC'd)
     * -- unrelated entirely to how large the range one writer's own activation actually needs to
     * replay is, and, unlike a manifest-body read, cannot be cached across calls (a chunk's mere
     * existence, not its content, is what's being asked here, and this is the one-time question a
     * replay asks at activation, not a repeated poll). One {@code blobExists} HEAD-shaped call per
     * candidate sequence instead costs O(this replay's own range size) -- for the common case (a
     * healthy failover replaying a short recent gap against a large, busy shared container), this is
     * the far cheaper direction; a genuinely huge range is still bounded to that range's own size,
     * never the whole container's history the way the listing this replaces was.
     */
    static List<Long> listChunkSequencesInRange(BlobContainer walBlobContainer, long fromInclusive, long uptoExclusive) throws IOException {
        List<Long> sequences = new ArrayList<>();
        for (long candidate = fromInclusive; candidate < uptoExclusive; candidate++) {
            if (walBlobContainer.blobExists(WalChunkNaming.LOG_BLOB_PREFIX + candidate)) {
                sequences.add(candidate);
            }
        }
        return sequences;
    }

    private static byte[] readChunkBytes(BlobContainer walBlobContainer, long chunkSequence) throws IOException {
        String blobName = WalChunkNaming.LOG_BLOB_PREFIX + chunkSequence;
        try (InputStream in = walBlobContainer.readBlob(blobName)) {
            return in.readAllBytes();
        }
    }
}
