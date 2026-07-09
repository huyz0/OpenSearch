/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.storage.manifest.WalPosition;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
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
     * @param minPrimaryTerm the term floor per {@code WalReplayFencing.tla}'s {@code ReplayFloor}
     *                       (one term back from the term this writer is activating under, so a
     *                       predecessor's legitimately-durable-but-not-yet-manifested records are
     *                       not wrongly excluded by term alone) -- see {@link
     *                       WalChunkReader#filterByShardAndMinimumTerm} for why this term filter is
     *                       necessary but, on its own, insufficient; {@code activationWalPosition}
     *                       is what closes the gap it leaves open.
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
        if (activationWalPosition < 0) {
            return List.of();
        }
        long fromChunkSequenceInclusive = lastDurableWalPosition == null ? 0 : lastDurableWalPosition.offset() + 1;
        if (fromChunkSequenceInclusive >= activationWalPosition) {
            return List.of();
        }

        List<Translog.Operation> operations = new ArrayList<>();
        for (long chunkSequence : listChunkSequencesInRange(walBlobContainer, fromChunkSequenceInclusive, activationWalPosition)) {
            byte[] chunkBytes = readChunkBytes(walBlobContainer, chunkSequence);
            List<WalRecord> records = WalChunkReader.filterByShardAndMinimumTerm(
                WalChunkReader.readRecords(chunkBytes),
                indexUuid,
                shardId,
                minPrimaryTerm
            );
            for (WalRecord record : records) {
                operations.add(Translog.Operation.readOperation(StreamInput.wrap(record.payload())));
            }
        }
        return operations;
    }

    /** Package-visible for direct testing of the listing/range logic without needing real chunk bytes. */
    static List<Long> listChunkSequencesInRange(BlobContainer walBlobContainer, long fromInclusive, long uptoExclusive) throws IOException {
        List<Long> sequences = new ArrayList<>();
        for (String blobName : walBlobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).keySet()) {
            long chunkSequence = WalChunkNaming.parseChunkSequence(blobName);
            if (chunkSequence >= fromInclusive && chunkSequence < uptoExclusive) {
                sequences.add(chunkSequence);
            }
        }
        Collections.sort(sequences);
        return sequences;
    }

    private static byte[] readChunkBytes(BlobContainer walBlobContainer, long chunkSequence) throws IOException {
        String blobName = WalChunkNaming.LOG_BLOB_PREFIX + chunkSequence;
        try (InputStream in = walBlobContainer.readBlob(blobName)) {
            return in.readAllBytes();
        }
    }
}
