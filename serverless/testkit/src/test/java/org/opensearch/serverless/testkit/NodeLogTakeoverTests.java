/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.store.NodeLogBlob.ShardKey;
import org.opensearch.serverless.store.NodeLogCommitter;
import org.opensearch.serverless.store.NodeLogEpoch;
import org.opensearch.serverless.store.NodeLogStore;
import org.opensearch.serverless.store.NodeLogTakeover;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** A successor fences every epoch a predecessor's lease names, once, and reads any shard's unpublished records by name. */
public class NodeLogTakeoverTests extends OpenSearchTestCase {

    private static final ShardKey A = new ShardKey("uuid-a", 0);
    private static final ShardKey B = new ShardKey("uuid-b", 0);
    private static final ShardKey C = new ShardKey("uuid-c", 0);

    private static List<WalRecord> one(String id, long seqNo) {
        return List.of(new WalRecord(id, "{\"n\":" + seqNo + "}", seqNo, 1L, 1L));
    }

    private static List<String> ids(List<WalRecord> records) {
        final List<String> out = new ArrayList<>();
        for (WalRecord record : records) {
            out.add(record.id());
        }
        return out;
    }

    private static NodeLogCommitter committer(NodeLogStore log) {
        return new NodeLogCommitter(log, 0L, epochs -> {}, (shard, term, epoch) -> {}, 0L, 1L << 30, 1000);
    }

    public void testASuccessorReadsEveryUnpublishedRecordOfItsShardAcrossEpochs() throws Exception {
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final NodeLogCommitter writer = committer(log);
            writer.commit(A, 1L, one("a0", 0));
            writer.commit(B, 1L, one("b0", 0));
            writer.commit(A, 1L, one("a1", 1));
            // Something closes epoch 0 under the writer; it rolls to epoch 1.
            log.fence(0L, 0L, "someone", 1L);
            expectThrows(IOException.class, () -> writer.commit(A, 1L, one("refused", 2)));
            writer.commit(A, 1L, one("a2", 2));
            writer.commit(B, 1L, one("b1", 1));
            // B published its first record, so the log no longer needs it.
            writer.onPublished(B, new NodeLogCommitter.Position(0L, 1L));
            final List<NodeLogEpoch> lease = writer.positions();

            final NodeLogTakeover takeover = new NodeLogTakeover(log, lease, "node-2", 2L);
            assertEquals(List.of("a0", "a1", "a2"), ids(takeover.records(A)));
            // Epoch 0's last header was written before B published, so it still names b0; recovery skips a record the
            // restored commit holds. Epoch 1 never held it.
            assertEquals(List.of("b0", "b1"), ids(takeover.records(B)));
            assertEquals(List.of(), takeover.records(C));
            expectThrows(IOException.class, () -> writer.commit(A, 1L, one("after", 3)));
        }
    }

    public void testShardsTakenFromOnePredecessorShareItsFencesAndHeaders() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final NodeLogCommitter writer = committer(log);
            final List<ShardKey> shards = List.of(A, B, C);
            for (int window = 0; window < 4; window++) {
                for (ShardKey shard : shards) {
                    // A zero window: one blob per commit, so each shard is in four blobs of its own.
                    writer.commit(shard, 1L, one(shard.indexUuid() + "-" + window, window));
                }
            }
            final List<NodeLogEpoch> lease = writer.positions();

            store.reset();
            final NodeLogTakeover takeover = new NodeLogTakeover(log, lease, "node-2", 2L);
            for (ShardKey shard : shards) {
                assertEquals(4, takeover.records(shard).size());
            }
            assertEquals("one fence for every shard taken", 1L, store.blobWrites());
            assertEquals("and no listing", 0L, store.listings());

            // A second successor taking another shard of the same predecessor shares that fence.
            store.reset();
            final NodeLogTakeover second = new NodeLogTakeover(log, lease, "node-3", 2L);
            assertEquals(4, second.records(A).size());
            assertEquals(0L, store.blobWrites());
        }
    }

    public void testAFenceFromAStaleLeaseIsSearchedForAgainAboveWhatTheWriterDeleted() throws Exception {
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final NodeLogCommitter writer = committer(log);
            writer.commit(A, 1L, one("old-0", 0));
            writer.commit(A, 1L, one("old-1", 1));
            // A successor reads the lease here, with a landed hint of 2...
            final List<NodeLogEpoch> stale = writer.positions();
            for (int i = 2; i < 6; i++) {
                writer.commit(A, 1L, one("old-" + i, i));
            }
            // ...and before it fences, A publishes, the writer records the new trim point, deletes behind it, and writes on.
            writer.onPublished(A, writer.lastPosition(A));
            final List<NodeLogEpoch> recorded = writer.positions();
            log.deleteRange(0L, 0L, recorded.get(0).trimPoint());
            writer.commit(B, 1L, one("b-after", 0));
            final List<NodeLogEpoch> now = writer.positions();

            assertEquals("the fence alone would land in the hole", 2L, stale.get(0).landed());
            final NodeLogTakeover takeover = new NodeLogTakeover(log, stale, "node-2", 2L, () -> now);
            assertEquals("found above the hole the deletion left", List.of("b-after"), ids(takeover.records(B)));
            expectThrows(IOException.class, () -> writer.commit(B, 1L, one("after-the-fence", 1)));
        }
    }

    public void testAnEpochWithNothingAboveItsTrimPointReadsNoRecords() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final NodeLogCommitter writer = committer(log);
            writer.commit(A, 1L, one("a0", 0));
            writer.onPublished(A, writer.lastPosition(A));
            final List<NodeLogEpoch> lease = writer.positions();
            assertEquals(List.of(new NodeLogEpoch(0L, 1L, 1L)), lease);

            store.reset();
            final NodeLogTakeover takeover = new NodeLogTakeover(log, lease, "node-2", 2L);
            assertEquals(List.of(), takeover.records(A));
            assertEquals("the fence, and nothing read below the trim point", 1L, store.blobWrites());
        }
    }
}
