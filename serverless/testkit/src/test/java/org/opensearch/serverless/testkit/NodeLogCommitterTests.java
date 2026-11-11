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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.store.NodeLogBlob;
import org.opensearch.serverless.store.NodeLogBlob.Range;
import org.opensearch.serverless.store.NodeLogBlob.ShardKey;
import org.opensearch.serverless.store.NodeLogCommitter;
import org.opensearch.serverless.store.NodeLogCommitter.Position;
import org.opensearch.serverless.store.NodeLogEpoch;
import org.opensearch.serverless.store.NodeLogStore;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** Every shard of a node in one blob per window: epochs recorded in the lease first, ownership checked, trim points kept. */
public class NodeLogCommitterTests extends OpenSearchTestCase {

    private static final ShardKey A = new ShardKey("uuid-a", 0);
    private static final ShardKey B = new ShardKey("uuid-b", 0);

    private static List<WalRecord> records(String prefix, int count) {
        final List<WalRecord> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(new WalRecord(prefix + i, "{\"n\":" + i + "}", i, 1L, 1L));
        }
        return out;
    }

    private static NodeLogCommitter committer(NodeLogStore log, List<List<NodeLogEpoch>> leases, long windowMillis) {
        return new NodeLogCommitter(log, 0L, leases::add, (shard, term, epoch) -> {}, windowMillis, 1L << 30, 1_000_000);
    }

    public void testEveryShardWrittenInOneWindowIsOnePut() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final NodeLogCommitter committer = committer(log, new CopyOnWriteArrayList<>(), 1_000L);
            final int shards = 16;
            final CountDownLatch go = new CountDownLatch(1);
            final List<Position> landed = new CopyOnWriteArrayList<>();
            final AtomicReference<Exception> failed = new AtomicReference<>();
            final List<Thread> writers = new ArrayList<>();
            for (int shard = 0; shard < shards; shard++) {
                final ShardKey key = new ShardKey("uuid-" + shard, shard % 3);
                final Thread writer = new Thread(() -> {
                    try {
                        go.await();
                        landed.add(committer.commit(key, 1L, records("s" + key.indexUuid(), 3)));
                    } catch (Exception e) {
                        failed.set(e);
                    }
                });
                writer.start();
                writers.add(writer);
            }
            go.countDown();
            for (Thread writer : writers) {
                writer.join();
            }
            assertNull(failed.get());
            assertEquals("sixteen shards in one window are one PUT", 1L, store.blobWrites());
            assertEquals(1L, committer.blobs());
            assertEquals(48L, committer.recordsCommitted());
            assertEquals(Set.of(new Position(0L, 0L)), Set.copyOf(landed));
            final NodeLogBlob.Header header = (NodeLogBlob.Header) log.readEntry(0L, 0L).orElseThrow();
            assertEquals(shards, header.slices().size());
        }
    }

    public void testAZeroWindowWritesEachCommitAtOnce() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final NodeLogCommitter committer = committer(log, new ArrayList<>(), 0L);
            assertEquals(new Position(0L, 0L), committer.commit(A, 1L, records("a", 1)));
            assertEquals(new Position(0L, 1L), committer.commit(B, 1L, records("b", 1)));
            assertEquals(new Position(0L, 2L), committer.commit(A, 1L, records("c", 1)));
            assertEquals(3L, store.blobWrites());
            assertNull("an empty commit writes nothing", committer.commit(A, 1L, List.of()));
            assertEquals(3L, store.blobWrites());
        }
    }

    public void testAFullWindowIsWrittenWithoutWaitingOutItsTime() throws Exception {
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final NodeLogCommitter committer = new NodeLogCommitter(log, 0L, epochs -> {}, (s, t, e) -> {}, 600_000L, 1L << 30, 5);
            final long started = System.nanoTime();
            committer.commit(A, 1L, records("a", 5));
            assertTrue("a full window does not wait ten minutes", System.nanoTime() - started < 30_000_000_000L);
        }
    }

    public void testAnEpochIsInTheLeaseBeforeItsFirstBlob() throws Exception {
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final List<List<NodeLogEpoch>> leases = new ArrayList<>();
            final NodeLogCommitter committer = new NodeLogCommitter(log, 7L, epochs -> {
                assertTrue("nothing is written in an epoch before the lease names it", log.readEntry(7L, 0L).isEmpty());
                leases.add(epochs);
            }, (s, t, e) -> {}, 0L, 1L << 30, 1000);
            assertEquals(new Position(7L, 0L), committer.commit(A, 1L, records("a", 1)));
            committer.commit(A, 1L, records("b", 1));
            assertEquals("one lease write per epoch, not per blob", List.of(List.of(new NodeLogEpoch(7L, 0L, 0L))), leases);
            assertEquals(List.of(new NodeLogEpoch(7L, 0L, 2L)), committer.positions());
        }
    }

    public void testALeaseThatCannotBeWrittenLogsNothing() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final boolean[] refuse = { true };
            final NodeLogCommitter committer = new NodeLogCommitter(log, 0L, epochs -> {
                if (refuse[0]) {
                    throw new IOException("register contended");
                }
            }, (s, t, e) -> {}, 0L, 1L << 30, 1000);
            expectThrows(IOException.class, () -> committer.commit(A, 1L, records("a", 1)));
            assertEquals(0L, store.blobWrites());
            refuse[0] = false;
            assertEquals(new Position(0L, 0L), committer.commit(A, 1L, records("a", 1)));
        }
    }

    public void testAFenceEndsTheEpochAndTheNextWindowOpensAnother() throws Exception {
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final List<List<NodeLogEpoch>> leases = new ArrayList<>();
            final NodeLogCommitter committer = committer(log, leases, 0L);
            committer.commit(A, 1L, records("a", 1));
            committer.commit(B, 1L, records("b", 1));
            assertEquals(2L, log.fence(0L, 0L, "node-2", 5L));

            expectThrows(IOException.class, () -> committer.commit(A, 1L, records("c", 1)));
            assertEquals("the next window opens epoch 1 at slot 0", new Position(1L, 0L), committer.commit(A, 1L, records("d", 1)));
            assertEquals(
                "the new epoch's lease still names the fenced one, which holds unpublished records",
                List.of(new NodeLogEpoch(0L, 0L, 2L), new NodeLogEpoch(1L, 0L, 0L)),
                leases.get(1)
            );

            // Once both shards publish past it, the fenced epoch is complete and leaves the lease.
            committer.onPublished(A, committer.lastPosition(A));
            assertEquals(List.of(new NodeLogEpoch(0L, 1L, 2L), new NodeLogEpoch(1L, 1L, 1L)), committer.positions());
            committer.onPublished(B, committer.lastPosition(B));
            assertEquals(List.of(new NodeLogEpoch(1L, 1L, 1L)), committer.positions());
            assertEquals(List.of(new NodeLogEpoch(0L, 2L, 2L)), committer.takeComplete());
            assertEquals(List.of(), committer.takeComplete());
        }
    }

    public void testAShardIsCheckedOnceAnEpochBeforeItsFirstRecords() throws Exception {
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final List<String> checks = new ArrayList<>();
            final List<List<NodeLogEpoch>> leases = new ArrayList<>();
            final NodeLogCommitter committer = new NodeLogCommitter(log, 0L, leases::add, (shard, term, epoch) -> {
                assertEquals("checked after the epoch is in the lease", 1, leases.size());
                checks.add(shard + "@" + term);
                if (shard.equals(B)) {
                    throw new IOException("head names another node");
                }
            }, 0L, 1L << 30, 1000);
            committer.commit(A, 1L, records("a", 1));
            committer.commit(A, 1L, records("b", 1));
            expectThrows(IOException.class, () -> committer.commit(B, 1L, records("x", 1)));
            committer.commit(A, 2L, records("c", 1));
            assertEquals(List.of(A + "@1", B + "@1", A + "@2"), checks);
            assertEquals("a refused shard costs no blob", new Position(0L, 2L), committer.lastPosition(A));
            assertNull(committer.lastPosition(B));
        }
    }

    public void testTheTrimPointIsTheLowestRecordNotYetPublished() throws Exception {
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final NodeLogCommitter committer = committer(log, new ArrayList<>(), 0L);
            committer.commit(A, 1L, records("a", 1));
            committer.commit(B, 1L, records("b", 1));
            final Position a2 = committer.commit(A, 1L, records("c", 1));
            assertEquals(0L, committer.trimPoint());

            committer.onPublished(A, a2);
            assertEquals("B's record at slot 1 is now the lowest", 1L, committer.trimPoint());
            final Position next = committer.commit(B, 1L, records("d", 1));
            final NodeLogBlob.Header header = (NodeLogBlob.Header) log.readEntry(next.epoch(), next.ordinal()).orElseThrow();
            assertEquals("the header carries it", 1L, header.trimPoint());
            assertEquals("and indexes only what is unpublished", List.of(), header.ordinalsOf(A));
            assertEquals(List.of(new Range(1, 1), new Range(3, 3)), header.ordinalsOf(B));

            committer.onPublished(B, committer.lastPosition(B));
            assertEquals("nothing unpublished: the trim point is the next slot", 4L, committer.trimPoint());
        }
    }

    public void testALeaseCarriesItsNodeLogThroughARoundTrip() throws Exception {
        final List<NodeLogEpoch> epochs = List.of(new NodeLogEpoch(3L, 10L, 12L), new NodeLogEpoch(4L, 0L, 7L));
        final NodeLease lease = new NodeLease("n", "e", "a", Set.of("ingest"), 10L, "name", "v").withLoad(1, 2).withNodeLog(epochs);
        final NodeLease read = NodeLease.fromStream(new ByteArrayInputStream(BytesReference.toBytes(lease.toBytes())));
        assertEquals(epochs, read.nodeLog());
        assertEquals("renewal keeps it", epochs, read.renewedUntil(20L).nodeLog());
        assertEquals(1, read.held());
        final NodeLease old = new NodeLease("n", "e", "a", Set.of("ingest"), 10L);
        assertEquals(
            "a lease without the field parses with none",
            List.of(),
            NodeLease.fromStream(new ByteArrayInputStream(BytesReference.toBytes(old.toBytes()))).nodeLog()
        );
    }
}
