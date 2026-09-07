/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.serverless.store.WalStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M49: fencing a writer that has lost its shard but has not yet noticed.
 *
 * <p><b>The hole this closes.</b> M48 disclosed it precisely: a writer at term T appends to
 * {@code wal/t=T}, and a successor replays every term it finds, so records the old writer appends
 * <em>after</em> ownership moved are replayed as though they had been acknowledged before it. The
 * term-scoped key path separates the two writers' blobs; it never stopped anyone from reading the older
 * term, which is exactly what recovery does. The reference implementation in
 * {@code plugins/serverless-storage} documents the identical hole in its own log.
 *
 * <p><b>Two defences, doing different jobs.</b> A node that checks its own lease before appending stops
 * writing no later than the deadline it last published, which turns an unbounded zombie into one bounded
 * by the TTL. That is a bound, not a fence: the check and the append are not atomic. The fence is on the
 * other side — a successor seals the log at takeover and nothing appended after the seal is ever replayed,
 * by it or by any node after it. The first test here covers the bound, the second the fence.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessWriteFencingTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-fencing")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane freshIndex(AtomicLong clock, Path objectStore) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, objectStore, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        return plane;
    }

    /**
     * A node past the expiry it last published for itself refuses to write, and lets the shard go.
     *
     * <p><b>Why this is not already covered by the heartbeat.</b> A node learns it lost a shard by reading
     * the shard-head, which it does on its heartbeat. A node that cannot reach the object store cannot read
     * the head <em>or</em> renew its lease — so the case where the existing check is guaranteed not to fire
     * is precisely the case where a zombie is possible. This check needs no I/O, which is what makes it
     * affordable on the write path, which is where it has to be.
     *
     * <p>The clock is advanced rather than the store broken, because what is being asserted is the node's
     * own reasoning about its own deadline: past it, this node may no longer claim to hold anything.
     */
    public void testANodePastItsOwnLeaseRefusesToWriteAndReleasesTheShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode node = new ServerlessNode(nodeSettings("fence-self"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            assertTrue("a node inside its lease writes normally", node.index(shardId, "a", "{\"msg\":\"a\"}").seqNo() >= 0);

            // Past the deadline this node last published, with no renewal in between: from here on it has
            // no standing to claim it still holds the shard.
            clock.set(clock.get() + TTL + 1);

            final IOException refused = expectThrows(IOException.class, () -> node.index(shardId, "b", "{\"msg\":\"b\"}"));
            assertTrue(
                "the refusal must say the lease is why, so an operator is not left looking at the store: " + refused.getMessage(),
                refused.getMessage().contains("lease has expired")
            );
            assertNull("a node that may no longer write must not go on holding the shard", node.reconciler().shard(shardId));
        }
    }

    /**
     * A record appended after a successor took over is never replayed — not by that successor, and not by
     * the one after it.
     *
     * <p><b>Why three generations and not two.</b> A cutoff held in memory by the node taking over would
     * pass a two-generation test and still be worthless, because the case that matters is the successor
     * dying before it publishes: the zombie's records then sit in the log looking like ordinary history,
     * and the <em>next</em> node to take over replays them. So B takes over and seals, the zombie appends
     * afterwards, B dies without ever publishing, and C is the one that must not be fooled. Only a durable
     * seal passes this.
     *
     * <p>The zombie is B's predecessor's own log object, deliberately: a fresh {@code WalStore} restarts
     * its ordinal at 1 and would overwrite the existing record rather than append after it. Continuing the
     * ordinal is what a node that has not noticed anything is wrong would actually do.
     */
    public void testARecordAppendedAfterTakeoverIsNeverReplayed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);

        final WalStore zombieLog;
        final long zombieTerm;

        final ServerlessNode a = new ServerlessNode(nodeSettings("fence-a"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId onA = a.reconciler().openShards().iterator().next();

        // Written and acknowledged before anything went wrong: this one is legitimate history and must
        // survive everything below.
        a.index(onA, "legitimate", "{\"msg\":\"written before the takeover\"}");
        zombieTerm = a.get(onA, "legitimate").primaryTerm();
        zombieLog = a.reconciler().wal(onA);
        assertNotNull("the test needs the predecessor's own log object to be a faithful zombie", zombieLog);

        a.close();
        clock.set(clock.get() + TTL + 1);

        // B takes the shard over, which seals the log at the point legitimate history ended.
        final ServerlessNode b = new ServerlessNode(nodeSettings("fence-b"));
        b.start();
        final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
        loopB.want("alpha", 0);
        loopB.tick(clock.get());
        final ShardId onB = b.reconciler().openShards().iterator().next();
        assertTrue("the legitimate write must have replayed onto the successor", b.get(onB, "legitimate").found());

        // Now the zombie wakes up and appends under its old term, exactly as a node that has not yet
        // noticed it lost the shard would. Nothing rejects this: it is a well-formed write to a path the
        // zombie still believes is its own.
        zombieLog.append(zombieTerm, List.of(new WalRecord("zombie", "{\"msg\":\"appended after the takeover\"}", 99L, zombieTerm, 1L)));

        // B dies without ever publishing, so the log is still the only account of this shard's history.
        b.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode c = new ServerlessNode(nodeSettings("fence-c"))) {
            c.start();
            final BackgroundReconciler loopC = new BackgroundReconciler(c, plane);
            loopC.want("alpha", 0);
            loopC.tick(clock.get());
            final ShardId onC = c.reconciler().openShards().iterator().next();

            assertTrue("history written before the takeover must still survive", c.get(onC, "legitimate").found());
            assertFalse(
                "a record appended after the shard changed hands must not be replayed as acknowledged history",
                c.get(onC, "zombie").found()
            );
        }
    }

    /**
     * The same fence, in the state this system is normally in: an empty log.
     *
     * <p><b>Why this is the case that matters, not a variation on the last one.</b> A publish truncates the
     * log, so a shard that has been publishing normally has an empty or nearly-empty log at any given
     * moment — which means the seal a successor writes is usually empty too. An empty seal has to be read
     * as "there was nothing here", because the alternative, treating a term a seal does not mention as
     * unbounded, hands a zombie's later appends straight back to the next successor. That is the steady
     * state of the system, so getting it wrong would leave the fence working only for shards that had
     * never published.
     */
    public void testAZombieIsFencedEvenWhenTheLogWasEmptyAtTakeover() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);

        final WalStore zombieLog;
        final long zombieTerm;

        final ServerlessNode a = new ServerlessNode(nodeSettings("empty-a"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId onA = a.reconciler().openShards().iterator().next();

        a.index(onA, "legitimate", "{\"msg\":\"written and published\"}");
        zombieTerm = a.get(onA, "legitimate").primaryTerm();
        zombieLog = a.reconciler().wal(onA);

        // Publish, which is what empties the log: the document now lives in a commit and its record is
        // truncated away. This is the ordinary state of a healthy shard.
        loopA.tick(clock.get() + 1_000);
        loopA.tick(clock.get() + 2_000);

        a.close();
        clock.set(clock.get() + TTL + 1);

        final ServerlessNode b = new ServerlessNode(nodeSettings("empty-b"));
        b.start();
        final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
        loopB.want("alpha", 0);
        loopB.tick(clock.get());
        final ShardId onB = b.reconciler().openShards().iterator().next();
        assertTrue("the published document must have arrived in the commit", b.get(onB, "legitimate").found());

        // The zombie appends under its old term, into a log that was empty when B sealed it.
        zombieLog.append(zombieTerm, List.of(new WalRecord("zombie", "{\"msg\":\"appended after the takeover\"}", 99L, zombieTerm, 1L)));

        b.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode c = new ServerlessNode(nodeSettings("empty-c"))) {
            c.start();
            final BackgroundReconciler loopC = new BackgroundReconciler(c, plane);
            loopC.want("alpha", 0);
            loopC.tick(clock.get());
            final ShardId onC = c.reconciler().openShards().iterator().next();

            assertTrue("published history must still be there", c.get(onC, "legitimate").found());
            assertFalse("an empty seal must be read as 'there was nothing here', not as 'nothing is known'", c.get(onC, "zombie").found());
        }
    }

    /**
     * The window between the swap that moves ownership and the seal that records where history ended.
     *
     * <p><b>What this is about.</b> The seal used to be taken when the successor opened the shard, which is
     * a shard creation, a local-file drop and a translog bootstrap after the compare-and-swap that actually
     * transferred ownership. Every append a predecessor made inside that stretch was in front of the cutoff
     * and replayed as acknowledged history — a wrong answer, not a missing feature. The seal is now taken at
     * the swap, so the stretch is one blob write instead.
     *
     * <p><b>Where the append is injected, and why there.</b> {@code MetadataPlane#activate} writes the
     * acquiring node's assignment claim immediately after it seals, so a hook on that write fires strictly
     * after the seal under the current code and strictly before it under the old code, while sitting inside
     * the window either way. That is the whole discrimination: nothing else about the test changes.
     *
     * <p><b>What this does not claim.</b> The window is narrowed, not closed. A swap and a seal are two
     * object-store operations and there is no transaction spanning them, so an append landing between them
     * is still inside the cutoff. Closing it entirely would need the register itself to carry the seal.
     *
     * <p><b>D5:</b> {@code FsBlobContainer} only.
     */
    public void testAZombieAppendingBetweenTheSwapAndTheOpenIsFenced() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path objectStore = createTempDir();
        final MetadataPlane setup = freshIndex(clock, objectStore);

        final WalStore zombieLog;
        final long zombieTerm;

        final ServerlessNode a = new ServerlessNode(nodeSettings("window-a"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, setup);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId onA = a.reconciler().openShards().iterator().next();

        // Legitimate history, never published, so the log is the only account of it and the cutoff is what
        // decides whether it survives.
        a.index(onA, "legitimate", "{\"msg\":\"written before the takeover\"}");
        zombieTerm = a.get(onA, "legitimate").primaryTerm();
        zombieLog = a.reconciler().wal(onA);

        a.close();
        clock.set(clock.get() + TTL + 1);

        // The successor's plane, with one hook: the moment it records its claim on the shard it has just
        // won, the predecessor appends. That is inside the window by construction.
        final AtomicBoolean appended = new AtomicBoolean();
        final BlobStore hooked = new ClaimHookingBlobStore(new FsBlobStore(1024, objectStore, false), () -> {
            if (appended.compareAndSet(false, true)) {
                try {
                    zombieLog.append(
                        zombieTerm,
                        List.of(new WalRecord("zombie", "{\"msg\":\"appended inside the window\"}", 99L, zombieTerm, 1L))
                    );
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException("the zombie could not append, so the window was never exercised", e);
                }
            }
        });
        final MetadataPlane plane = new MetadataPlane(hooked, BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode b = new ServerlessNode(nodeSettings("window-b"))) {
            b.start();
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 0);
            loopB.tick(clock.get());
            assertTrue("the hook must have fired, or this test proves nothing", appended.get());

            final ShardId onB = b.reconciler().openShards().iterator().next();
            assertTrue("history written before the takeover must still survive", b.get(onB, "legitimate").found());
            assertFalse(
                "a record appended between the swap and the open must not be replayed as acknowledged history",
                b.get(onB, "zombie").found()
            );
        }
    }

    /** Fires a hook when a node records its claim on a shard it has just acquired. */
    private static final class ClaimHookingBlobStore implements BlobStore {

        private final BlobStore delegate;
        private final Runnable onClaim;

        ClaimHookingBlobStore(BlobStore delegate, Runnable onClaim) {
            this.delegate = delegate;
            this.onClaim = onClaim;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            final BlobContainer honest = delegate.blobContainer(path);
            if (path.buildAsString().startsWith("assignments/") == false) {
                return honest;
            }
            return new DelegatingBlobContainer(honest) {
                @Override
                public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                    throws IOException {
                    onClaim.run();
                    super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
                }
            };
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
