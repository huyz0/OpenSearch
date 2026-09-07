/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.metadata.SnapshotRecord;
import org.opensearch.serverless.metadata.TemplateResolver;
import org.opensearch.serverless.metadata.TemplateStore;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.store.BlockCache;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The store-level rules that keep a deletion from becoming a wrong answer: what a prefix pattern counts,
 * what an orphan sweep leaves alone, what a sweep does with a pin it cannot read, what a template change
 * looks like from a node that did not make it, and how a block is fetched when everyone misses at once.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessStoreInvariantTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private static MetadataPlane planeOver(FsBlobStore store, AtomicLong clock) {
        return new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
    }

    private static void write(BlobContainer container, String name, String content) throws Exception {
        final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        container.writeBlob(name, new ByteArrayInputStream(bytes), bytes.length, false);
    }

    /**
     * A prefix pattern counts live names against its cap, not the tombstones of names that used to exist.
     *
     * <p>A tenant creating and deleting a daily index accumulated a tombstone a day, and after enough days
     * {@code logs-*} was refused as "more than N indices" with one index under it. The tombstones are then
     * collected once their quarantine has passed, and a fresh one is not.
     */
    public void testAPrefixPatternCountsOnlyLiveNamesAgainstItsCap() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(new FsBlobStore(1024, createTempDir(), false), clock);
        for (int day = 1; day <= 7; day++) {
            plane.createIndex(new IndexDescriptor("logs-" + day, "uuid-logs-0000000000" + day, 1, MAPPING, null));
        }
        for (int day = 1; day <= 6; day++) {
            assertTrue(plane.deleteIndex("logs-" + day));
        }

        assertEquals("one live index under the prefix, six deleted ones", List.of("logs-7"), plane.namesWithPrefix("logs-", 5));

        // Past the quarantine the tombstones go, and the bounded listing alone answers again. The plane
        // stamps a tombstone with its own clock, so the sweep is driven by the same one.
        final long quarantine = DescriptorStore.DEFAULT_TOMBSTONE_QUARANTINE_MILLIS;
        clock.addAndGet(quarantine + 60_000L);
        assertEquals(6, plane.descriptors().sweepTombstones(clock.get(), quarantine).size());
        assertEquals(List.of("logs-7"), plane.namesWithPrefix("logs-", 5));
        assertTrue("nothing left to collect", plane.descriptors().sweepTombstones(clock.get(), quarantine).isEmpty());

        // A tombstone inside its quarantine still guards the generation a swap in flight may carry.
        assertTrue(plane.deleteIndex("logs-7"));
        clock.addAndGet(quarantine / 2);
        assertTrue("a fresh tombstone is kept", plane.descriptors().sweepTombstones(clock.get(), quarantine).isEmpty());
        assertTrue("and the name is still absent", plane.describe("logs-7").isEmpty());
        clock.addAndGet(quarantine);
        assertEquals(
            "and goes once its quarantine has passed",
            List.of("logs-7"),
            plane.descriptors().sweepTombstones(clock.get(), quarantine)
        );
    }

    /**
     * The orphan sweep leaves alone what a live shallow snapshot or a live view still names.
     *
     * <p>Deleting an index deliberately leaves the shards a shallow snapshot pins, and a view taken before
     * the delete still names its files. Both looked exactly like orphans to a sweep that consulted only
     * descriptors, and the sweep deleted what a restore or a paging caller was about to read.
     */
    public void testTheOrphanSweepLeavesWhatASnapshotOrAViewStillNames() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final BlobPath base = BlobPath.cleanPath();
        final MetadataPlane plane = planeOver(store, clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-current00", 1, MAPPING, null));

        final CommitManifest frozen = new CommitManifest(1L, Map.of("_0.cfs", "t=1"));
        // Three containers no descriptor names: one a shallow snapshot pins, one a view pins, one nobody does.
        write(store.blobContainer(RegisterMap.shardData(base, "alpha", "uuid-alpha-snapshot0", 0).add("t=1")), "_0.cfs", "snapshotted");
        write(store.blobContainer(RegisterMap.shardData(base, "alpha", "uuid-alpha-viewed000", 0).add("t=1")), "_0.cfs", "viewed");
        write(store.blobContainer(RegisterMap.shardData(base, "beta", "uuid-beta-orphan0000", 0).add("t=1")), "_0.cfs", "orphaned");

        final SnapshotRecord snapshot = new SnapshotRecord(
            "repo",
            "snap",
            "snap-uuid",
            true,
            1L,
            2L,
            Map.of("alpha", new SnapshotRecord.SnapshottedIndex("uuid-alpha-snapshot0", 1, MAPPING, Map.of(0, frozen)))
        );
        final var record = snapshot.toBytes();
        store.blobContainer(RegisterMap.snapshots(base)).writeBlob(snapshot.key(), record.streamInput(), record.length(), true);
        plane.createPointInTime(
            new PointInTime(UUIDs.randomBase64UUID(), "alpha", "uuid-alpha-viewed000", clock.get() + 600_000L, Map.of(0, frozen))
        );

        final List<String> swept = new GarbageCollector(store, base).collectOrphanedShards(plane);

        assertEquals("exactly the container nothing names: " + swept, List.of("beta#uuid-beta-orphan0000#0"), swept);
        assertTrue(
            "the snapshot's bytes stay",
            store.blobContainer(RegisterMap.shardData(base, "alpha", "uuid-alpha-snapshot0", 0).add("t=1")).blobExists("_0.cfs")
        );
        assertTrue(
            "the view's bytes stay",
            store.blobContainer(RegisterMap.shardData(base, "alpha", "uuid-alpha-viewed000", 0).add("t=1")).blobExists("_0.cfs")
        );
        assertTrue(
            "the live index is untouched",
            store.blobContainer(RegisterMap.shardData(base, "alpha", "uuid-alpha-current00", 0)).path() != null
        );
    }

    /**
     * A freeze in progress stops the sweep, so the commit it is about to freeze cannot be swept first.
     *
     * <p><b>The window.</b> Freezing reads one manifest per shard and only then writes the record. Until
     * that write, the collector has no idea a view is being taken: files the first shards froze can stop
     * being referenced — the writer publishes again — and be deleted before the record naming them
     * exists. The wall-clock floor on unreferenced blobs makes a <em>short</em> freeze safe and only a
     * short one. At {@code IndexDescriptor.MAX_SHARDS} the manifest reads alone pass the default floor at
     * any per-read latency over about fifteen milliseconds, which an object store under load exceeds
     * easily.
     *
     * <p>Snapshots already had the guard — an index named by a capture that has not recorded its commits
     * sweeps nothing. Points in time did not, and now do: the freeze writes a marker naming the index and
     * holding no shards before it reads anything, and replaces it under the same id when it is done.
     *
     * <p>The blob here is one a sweep would otherwise take: unreferenced by the live commit, in a dead
     * term, and already a candidate from a previous pass with its grace elapsed.
     */
    public void testAFreezeInProgressStopsTheSweepThatWouldOutrunIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final BlobPath base = BlobPath.cleanPath();
        final MetadataPlane plane = planeOver(store, clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        // Published at term 2, with a file left behind under term 1 that the live commit does not name.
        final BlobContainer shard = store.blobContainer(plane.shardData("alpha", 0));
        shard.compareAndSwapRegister(
            SegmentPublisher.MANIFEST,
            BlobRegister.ABSENT_GENERATION,
            new CommitManifest(2L, Map.of("_0.cfs", "t=2")).toBytes()
        );
        write(store.blobContainer(plane.shardData("alpha", 0).add("t=2")), "_0.cfs", "live");
        write(store.blobContainer(plane.shardData("alpha", 0).add("t=1")), "_z.cfs", "about to be frozen");

        final GarbageCollector gc = new GarbageCollector(store, base);

        // One pass to put it on watch, then past the grace, so the next pass would delete it outright.
        // An empty set rather than null: null means "delete on first sight", which is the operator's
        // sweep, and this test is about the graced one a schedule runs.
        final Set<String> watched = gc.sweepShard(plane, "alpha", 0, Set.of()).candidates();
        assertTrue("the blob must be a candidate for this test to mean anything: " + watched, watched.contains("t=1/_z.cfs"));
        clock.addAndGet(GarbageCollector.DEFAULT_MINIMUM_UNREFERENCED_MILLIS + 1);

        // A freeze begins: the marker names the index and holds nothing yet.
        final String pitId = UUIDs.randomBase64UUID();
        plane.beginPointInTime(new PointInTime(pitId, "alpha", "uuid-alpha-00000000", clock.get() + 600_000L, Map.of(), true));

        final var blocked = gc.sweepShard(plane, "alpha", 0, watched);
        assertTrue("nothing may go while a freeze is reading manifests: " + blocked.deleted(), blocked.deleted().isEmpty());
        assertTrue(
            "and the file the freeze is about to name must still be there",
            store.blobContainer(plane.shardData("alpha", 0).add("t=1")).blobExists("_z.cfs")
        );
        assertTrue("what was on watch stays on watch", blocked.candidates().contains("t=1/_z.cfs"));

        // The freeze finishes, naming that very file. It stays pinned for the ordinary reason now.
        plane.finishPointInTime(
            new PointInTime(
                pitId,
                "alpha",
                "uuid-alpha-00000000",
                clock.get() + 600_000L,
                Map.of(0, new CommitManifest(1L, Map.of("_z.cfs", "t=1")))
            )
        );
        // Asserted about this blob rather than about an empty list: ExtrasFS drops its own file into these
        // directories, and it is a genuine orphan the sweep is right to take. Its arrival is not what this
        // test is about -- the same note as testAnUnreadableViewRecordStopsTheSweepRatherThanPinningNothing.
        final var pinned = gc.sweepShard(plane, "alpha", 0, blocked.candidates());
        assertFalse("a finished view pins it too: " + pinned.deleted(), pinned.deleted().contains("t=1/_z.cfs"));
        assertTrue(
            "and the bytes are still there",
            store.blobContainer(plane.shardData("alpha", 0).add("t=1")).blobExists("_z.cfs")
        );

        // Released -- and the grace restarts rather than the file becoming collectable at once. Being
        // named by a view took it off watch, so the sweep after the release only puts it back on, and the
        // one after that takes it. That is the collector's own rule and it is worth pinning here: a pin
        // dropped a moment ago must not be a pin that never protected anything.
        assertTrue(plane.releasePointInTime(pitId));
        final var rearmed = gc.sweepShard(plane, "alpha", 0, pinned.candidates());
        assertFalse("the sweep straight after a release only re-arms the watch", rearmed.deleted().contains("t=1/_z.cfs"));
        assertTrue("and it is on watch again", rearmed.candidates().contains("t=1/_z.cfs"));

        final List<String> swept = new java.util.ArrayList<>(gc.sweepShard(plane, "alpha", 0, rearmed.candidates()).deleted());
        swept.removeIf(name -> name.endsWith("/extra0"));
        assertEquals("once nothing names it, it goes", List.of("t=1/_z.cfs"), swept);
    }

    /**
     * A view record the plane cannot read stops the sweep rather than pinning nothing.
     *
     * <p>The plane stands in for such a record with a placeholder that names no shards, and "treated as
     * live" used to mean adding its empty references to the set and sweeping on -- which is treating
     * "unreadable" as "holds nothing", the opposite of what the placeholder means.
     */
    public void testAnUnreadableViewRecordStopsTheSweepRatherThanPinningNothing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final BlobPath base = BlobPath.cleanPath();
        final MetadataPlane plane = planeOver(store, clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        // A published commit at term 2, and a blob under term 1 that nothing names: an orphan on any other day.
        final BlobContainer shard = store.blobContainer(plane.shardData("alpha", 0));
        shard.compareAndSwapRegister(
            SegmentPublisher.MANIFEST,
            BlobRegister.ABSENT_GENERATION,
            new CommitManifest(2L, Map.of("_0.cfs", "t=2")).toBytes()
        );
        write(store.blobContainer(plane.shardData("alpha", 0).add("t=2")), "_0.cfs", "live");
        write(store.blobContainer(plane.shardData("alpha", 0).add("t=1")), "_z.cfs", "orphan");

        // A record with the name this system would mint and bytes it cannot read: torn by a crash.
        final String torn = UUIDs.randomBase64UUID();
        assertTrue("the fixture needs a name the plane treats as its own", PointInTime.looksLikeAnId(torn));
        final BlobContainer pits = store.blobContainer(RegisterMap.pointsInTime(base));
        write(pits, torn, "{\"id\":\"" + torn + "\",\"index\":");
        assertEquals("the plane stands in a placeholder for it", 1, plane.livePointsInTime(clock.get()).size());
        assertTrue(plane.livePointsInTime(clock.get()).get(0).isPlaceholder());

        final GarbageCollector gc = new GarbageCollector(store, base);
        final GarbageCollector.ShardSweep blocked = gc.sweepShard(plane, "alpha", 0, null);
        assertTrue("nothing may go while a view record cannot be read: " + blocked.deleted(), blocked.deleted().isEmpty());
        assertTrue(store.blobContainer(plane.shardData("alpha", 0).add("t=1")).blobExists("_z.cfs"));

        // Once the record is gone, the sweep proceeds.
        pits.deleteBlobsIgnoringIfNotExists(List.of(torn));
        // Lucene's ExtrasFS drops an "extra0" into random directories, and about one seed in several it
        // lands in t=1/ -- where it is a genuine orphan that the sweep is right to take, and the exact
        // list then has two entries instead of one. Its arrival is not what this test is about. Dropped
        // rather than tolerated by loosening to a contains(), so "exactly one of ours went" survives.
        // ServerlessPointInTimeTests#testAStrayObjectAmongTheViewsIsNotTreatedAsOne has the same note.
        final List<String> swept = new java.util.ArrayList<>(gc.sweepShard(plane, "alpha", 0, null).deleted());
        swept.removeIf(name -> name.endsWith("/extra0"));
        assertEquals(List.of("t=1/_z.cfs"), swept);
    }

    /**
     * A template change is one register swap: bytes written without it are seen by nobody, and bytes it
     * names are seen by everybody.
     *
     * <p>The first version wrote the template under its name and then bumped a counter. A crash between
     * the two left the blob visible to a node with a cold cache and invisible to one with a warm cache,
     * until an unrelated change somewhere moved the counter.
     */
    public void testATemplateWrittenWithoutItsMarkerIsInvisibleEverywhereAndAMarkedOneIsSeenEverywhere() throws Exception {
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final BlobContainer container = store.blobContainer(RegisterMap.indexTemplates(BlobPath.cleanPath()));
        final TemplateStore writer = new TemplateStore(container, new TemplateStore.Cache());
        final TemplateStore warm = new TemplateStore(container, new TemplateStore.Cache());

        final String first = "{\"index_patterns\":[\"a-*\"]}";
        writer.put("t1", first);
        assertEquals(Set.of("t1"), warm.all().keySet());

        // A crash between writing the bytes and moving the register, in both spellings of the bytes.
        final String orphan = "{\"index_patterns\":[\"b-*\"]}";
        write(container, "template-t2", orphan);
        write(container, "blob-" + sha256(orphan), orphan);
        final TemplateStore cold = new TemplateStore(container, new TemplateStore.Cache());
        assertEquals("a warm node does not see it", Set.of("t1"), warm.all().keySet());
        assertEquals("and neither does a cold one", Set.of("t1"), cold.all().keySet());
        assertTrue("nor a read by name", cold.get("t2").isEmpty());

        // The marked write is seen on the next read, not on the next unrelated change.
        writer.put("t2", orphan);
        assertEquals(Set.of("t1", "t2"), warm.all().keySet());
        assertEquals(orphan, warm.get("t2").orElseThrow());
        assertEquals(orphan, cold.get("t2").orElseThrow());

        // Two names with the same bytes share them; deleting one leaves the other readable.
        writer.put("t3", orphan);
        assertTrue(writer.delete("t2"));
        assertFalse(writer.delete("t2"));
        assertEquals(Set.of("t1", "t3"), warm.all().keySet());
        assertEquals(orphan, warm.get("t3").orElseThrow());
        assertEquals(first, cold.get("t1").orElseThrow());
        assertTrue("the register moved once per change", writer.version() >= 4);
    }

    /** Concurrent misses on one block fetch it once; the rest wait for the fetch rather than repeat it. */
    public void testConcurrentMissesOnOneBlockFetchItOnce() throws Exception {
        final BlockCache cache = new BlockCache(1024, 8);
        final AtomicInteger fetches = new AtomicInteger();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final BlockCache.Fetcher slow = () -> {
            fetches.incrementAndGet();
            entered.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new byte[] { 1, 2, 3 };
        };

        final int readers = 6;
        final List<byte[]> results = Collections.synchronizedList(new ArrayList<>());
        final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch done = new CountDownLatch(readers);
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < readers; i++) {
            final Thread t = new Thread(() -> {
                try {
                    results.add(cache.fetch("shard/t=1/_0.cfs#0", slow));
                } catch (Throwable e) {
                    failures.add(e);
                } finally {
                    done.countDown();
                }
            }, "block-reader-" + i);
            threads.add(t);
            t.start();
        }
        assertTrue("one reader must have started the fetch", entered.await(30, TimeUnit.SECONDS));
        release.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        for (Thread t : threads) {
            t.join();
        }

        assertTrue("no reader may fail: " + failures, failures.isEmpty());
        assertEquals("every reader got the block", readers, results.size());
        assertEquals("and it was fetched exactly once", 1, fetches.get());
        final BlockCache.Stats stats = cache.stats();
        assertEquals(1L, stats.blocksResident());
        assertEquals(3L, stats.bytesResident());
        assertEquals(3L, stats.bytesFetched());
    }

    /** Composing templates replaces a field definition outright and merges an object's fields, as core does. */
    public void testTemplateCompositionReplacesAFieldAndMergesAnObject() throws Exception {
        final String component = "{\"template\":{\"mappings\":{\"properties\":{"
            + "\"host\":{\"type\":\"keyword\",\"ignore_above\":64},"
            + "\"n\":{\"type\":\"long\",\"index\":false},"
            + "\"a\":{\"properties\":{\"x\":{\"type\":\"long\"}}}}}}}";
        final String template = "{\"index_patterns\":[\"logs-*\"],\"composed_of\":[\"c\"],\"template\":{\"mappings\":{\"properties\":{"
            + "\"host\":{\"type\":\"text\"},"
            + "\"n\":{\"type\":\"long\"},"
            + "\"a\":{\"properties\":{\"y\":{\"type\":\"keyword\"}}}}}}}";

        final TemplateResolver.Inherited inherited = TemplateResolver.resolve(Map.of("t", template), Map.of("c", component), "logs-1");

        @SuppressWarnings("unchecked")
        final Map<String, Object> properties = (Map<String, Object>) inherited.mappings().get("properties");
        assertEquals("a field definition is replaced, not composed", Map.of("type", "text"), properties.get("host"));
        assertEquals("even where the composition would have been quiet", Map.of("type", "long"), properties.get("n"));
        @SuppressWarnings("unchecked")
        final Map<String, Object> a = (Map<String, Object>) ((Map<String, Object>) properties.get("a")).get("properties");
        assertEquals("while an object's fields merge", Set.of("x", "y"), a.keySet());
    }

    /** A view record carries the uuid of the index it froze; one written before that reads by name alone. */
    public void testAViewRecordCarriesItsIndexUuidAndAnOlderOneReadsWithout() throws Exception {
        final CommitManifest manifest = new CommitManifest(3L, Map.of("_0.cfs", "t=3"), "n1", Map.of("_0.cfs", 10L));
        final PointInTime pit = new PointInTime("id", "logs", "uuid-1", 99L, Map.of(0, manifest));

        final PointInTime stored = PointInTime.fromStream(pit.toBytes().streamInput());
        assertEquals("uuid-1", stored.indexUuid());
        assertTrue(stored.pins("logs", "uuid-1"));
        assertFalse("a view does not pin a later index of the same name", stored.pins("logs", "uuid-2"));
        assertTrue("a caller that does not know the uuid is answered by name", stored.pins("logs", null));
        assertFalse(stored.pins("other", "uuid-1"));
        assertEquals("extending keeps the uuid", "uuid-1", pit.withExpiry(200L).indexUuid());
        assertFalse(pit.isPlaceholder());

        final BytesStreamOutput wire = new BytesStreamOutput();
        pit.writeTo(wire);
        assertEquals("uuid-1", new PointInTime(wire.bytes().streamInput()).indexUuid());

        final String older =
            "{\"id\":\"id\",\"index\":\"logs\",\"expires_at\":5,\"shards\":[{\"shard\":0,\"term\":1,\"files\":{\"_0.cfs\":\"t=1\"}}]}";
        final PointInTime legacy = PointInTime.fromStream(new ByteArrayInputStream(older.getBytes(StandardCharsets.UTF_8)));
        assertNull(legacy.indexUuid());
        assertTrue("a record without a uuid pins by name, which errs towards holding", legacy.pins("logs", "anything"));
    }

    private static String sha256(String source) throws Exception {
        final byte[] hash = MessageDigest.getInstance("SHA-256").digest(source.getBytes(StandardCharsets.UTF_8));
        final StringBuilder hex = new StringBuilder();
        for (byte b : hash) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }
}
