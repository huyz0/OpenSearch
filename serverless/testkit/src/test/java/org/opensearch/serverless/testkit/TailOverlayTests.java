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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.transport.CommitPush;
import org.opensearch.serverless.transport.TailOverlays;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A search node serving a shard searches the writes its writer has acknowledged and not yet published, with no
 * publish and no store request: new documents, a new version of one the commit holds, and a deletion. Off for an
 * index, the search node serves its commit only. A search node opening the shard after the writes asks the writer for
 * them; and a commit that then takes them over leaves each document counted once.
 */
public class TailOverlayTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-tail-overlay")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put(CommitPush.ENABLED, true)
            .build();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        return node;
    }

    private interface Scenario {
        void run(MetadataPlane plane, CountingBlobStore store, ServerlessNode writer, ServerlessNode reader, ShardId shardId, long term)
            throws Exception;
    }

    private void withNodes(Settings indexSettings, Scenario scenario) throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            plane.createIndex(new IndexDescriptor("tail", "uuid-tail-00000000", 1, MAPPING, indexSettings));
            try (
                ServerlessNode writer = joined(new ServerlessNode(nodeSettings("tail-writer", "ingest")), plane);
                ServerlessNode reader = joined(new ServerlessNode(nodeSettings("tail-reader", "search")), plane)
            ) {
                plane.membership().refresh();
                final ShardId shardId = writer.activation().activateWriter(plane, "tail", 0).orElseThrow();
                final long term = plane.heads().read("tail", 0).orElseThrow().term();
                scenario.run(plane, store, writer, reader, shardId, term);
            }
        }
    }

    private static long hits(ServerlessNode reader, MetadataPlane plane, String field, String text) throws Exception {
        final ShardId onReader = reader.readers().serveAsReader(plane, "tail", 0);
        return ShardOps.hits(reader.searchService(), onReader, field, text);
    }

    public void testAcknowledgedWritesAreSearchableBeforeTheyArePublished() throws Exception {
        withNodes(null, (plane, store, writer, reader, shardId, term) -> {
            writer.writes().index(shardId, "a", "{\"msg\":\"old\",\"n\":1}");
            writer.writes().index(shardId, "b", "{\"msg\":\"gone\",\"n\":2}");
            writer.shards().publishShard(shardId, term);
            assertEquals(1L, hits(reader, plane, "msg", "old"));
            final ShardId onReader = reader.readers().serveAsReader(plane, "tail", 0);
            final CommitManifest commit = reader.reconciler().readerCommit(onReader).orElseThrow();
            // The open fetched the tail, so the writer pushes to this node from now on.
            assertBusy(() -> assertEquals(1, writer.router().tailPush().subscribersOf(shardId).size()));

            store.reset();
            writer.writes().index(shardId, "a", "{\"msg\":\"new\",\"n\":1}");
            writer.writes().index(shardId, "c", "{\"msg\":\"added\",\"n\":3}");
            writer.writes().delete(shardId, "b");
            assertBusy(() -> {
                assertEquals("a new document", 1L, hits(reader, plane, "msg", "added"));
                assertEquals("a new version replaces the commit's", 1L, hits(reader, plane, "msg", "new"));
                assertEquals("and hides the old one", 0L, hits(reader, plane, "msg", "old"));
                assertEquals("a deletion hides the commit's document", 0L, hits(reader, plane, "msg", "gone"));
            }, 10, TimeUnit.SECONDS);
            assertEquals("still on the commit it opened", commit.files(), reader.reconciler().readerCommit(onReader).orElseThrow().files());
            final ShardId now = reader.readers().serveAsReader(plane, "tail", 0);
            assertEquals(
                "every document once",
                2L,
                ShardOps.hits(reader.searchService(), now, "n", "1") + ShardOps.hits(reader.searchService(), now, "n", "3")
            );
            // Nothing was listed or read from the store for it: the writes' log appends, and the head checks every search
            // makes, are all.
            assertEquals("no blob read for the overlay:\n" + store.byPlace(), 0L, store.blobReads());
            assertEquals("no listing for the overlay:\n" + store.byPlace(), 0L, store.listings());

            // A publish moves the reader onto a commit holding them; each document is still there once.
            writer.shards().publishShard(shardId, term);
            assertBusy(
                () -> assertNotEquals(commit.files(), reader.reconciler().readerCommit(onReader).map(CommitManifest::files).orElse(null))
            );
            assertBusy(() -> {
                assertEquals(1L, hits(reader, plane, "msg", "new"));
                assertEquals(1L, hits(reader, plane, "msg", "added"));
                assertEquals(0L, hits(reader, plane, "msg", "gone"));
                assertEquals(0L, hits(reader, plane, "msg", "old"));
            });
        });
    }

    public void testAReaderOpeningTheShardFetchesWhatIsNotPublished() throws Exception {
        withNodes(null, (plane, store, writer, reader, shardId, term) -> {
            writer.writes().index(shardId, "a", "{\"msg\":\"published\",\"n\":1}");
            writer.shards().publishShard(shardId, term);
            writer.writes().index(shardId, "b", "{\"msg\":\"unpublished\",\"n\":2}");
            assertBusy(() -> assertEquals(1L, hits(reader, plane, "msg", "unpublished")), 10, TimeUnit.SECONDS);
            assertEquals(1L, hits(reader, plane, "msg", "published"));
            assertTrue(reader.router().tailOverlays().counts()[1] >= 1L);
        });
    }

    public void testALostPushStopsThePushesToThatReader() throws Exception {
        withNodes(null, (plane, store, writer, reader, shardId, term) -> {
            writer.writes().index(shardId, "a", "{\"msg\":\"published\",\"n\":1}");
            writer.shards().publishShard(shardId, term);
            assertEquals(1L, hits(reader, plane, "msg", "published"));
            assertBusy(() -> assertEquals(1, writer.router().tailPush().subscribersOf(shardId).size()));

            // The reader goes away while its lease still stands: the next push fails, and it is pushed nothing more, so
            // what it holds never has a gap in it.
            reader.close();
            writer.writes().index(shardId, "b", "{\"msg\":\"lost\",\"n\":2}");
            assertBusy(() -> assertTrue(writer.router().tailPush().subscribersOf(shardId).isEmpty()), 10, TimeUnit.SECONDS);
            assertTrue(writer.router().tailPush().counts()[2] >= 1L);
        });
    }

    public void testOffForAnIndexTheReaderServesItsCommitOnly() throws Exception {
        withNodes(Settings.builder().put(TailOverlays.INDEX_SETTING, "false").build(), (plane, store, writer, reader, shardId, term) -> {
            writer.writes().index(shardId, "a", "{\"msg\":\"published\",\"n\":1}");
            writer.shards().publishShard(shardId, term);
            assertEquals(1L, hits(reader, plane, "msg", "published"));
            writer.writes().index(shardId, "b", "{\"msg\":\"unpublished\",\"n\":2}");
            Thread.sleep(500);
            assertEquals(0L, hits(reader, plane, "msg", "unpublished"));
            assertEquals(0L, writer.router().tailPush().counts()[0]);
            assertTrue(writer.router().tailPush().subscribersOf(shardId).isEmpty());
        });
    }

    public void testTheIndexSettingWinsOverTheClusterAndTheClusterOverTheNode() {
        final Settings on = Settings.builder().put(TailOverlays.INDEX_SETTING, "true").build();
        final Settings off = Settings.builder().put(TailOverlays.INDEX_SETTING, "false").build();
        assertTrue(TailOverlays.on(on, "false", false));
        assertFalse(TailOverlays.on(off, "true", true));
        assertFalse(TailOverlays.on(Settings.EMPTY, "false", true));
        assertTrue(TailOverlays.on(Settings.EMPTY, "true", false));
        assertTrue(TailOverlays.on(Settings.EMPTY, null, true));
        assertFalse(TailOverlays.on(null, "", false));
        expectThrows(IllegalArgumentException.class, () -> TailOverlays.check("sometimes"));
    }
}
