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
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.transport.CommitPush;
import org.opensearch.serverless.transport.ReaderRegisterLooks;
import org.opensearch.serverless.transport.TailOverlays;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;

/**
 * A search node skips the periodic register look for a reader whose live writer pushes it every commit, and looks again
 * as soon as that stops being known: the writer gone, the writer not pushing, the switch off, or the backstop passed.
 */
public class ReaderRegisterLooksTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}";

    private Settings nodeSettings(String name, String roles, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-register-looks")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put(TailOverlays.ENABLED, false)
            .put(extra)
            .build();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        return node;
    }

    private static long manifestReads(CountingBlobStore store) {
        long reads = 0;
        for (String line : store.byPlace().split("\n")) {
            if (line.contains("REGISTER_GET") && line.contains("manifest")) {
                reads += Long.parseLong(line.trim().split("\\s+")[0]);
            }
        }
        return reads;
    }

    /** A writer and a search node holding its one shard as a reader, with one commit pushed to it. */
    private final class Pair implements AutoCloseable {
        final CountingBlobStore store;
        final MetadataPlane writerPlane;
        final MetadataPlane readerPlane;
        final ServerlessNode writer;
        final ServerlessNode reader;
        final ShardId onWriter;
        final ShardId onReader;
        final long term;
        final BackgroundReconciler readerLoop;

        Pair(Settings writerExtra, Settings readerExtra) throws Exception {
            store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false));
            writerPlane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            readerPlane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            writerPlane.createIndex(new IndexDescriptor("looked", "uuid-looked-00000000", 1, MAPPING, null));
            writer = joined(new ServerlessNode(nodeSettings("looks-writer", "ingest", writerExtra)), writerPlane);
            reader = joined(new ServerlessNode(nodeSettings("looks-reader", "search", readerExtra)), readerPlane);
            writerPlane.membership().refresh();
            readerPlane.membership().refresh();
            onWriter = writer.activation().activateWriter(writerPlane, "looked", 0).orElseThrow();
            term = writerPlane.heads().read("looked", 0).orElseThrow().term();
            writer.writes().index(onWriter, "a", "{\"msg\":\"first\"}");
            writer.shards().publishShard(onWriter, term);
            onReader = reader.readers().serveAsReader(readerPlane, "looked", 0);
            readerLoop = new BackgroundReconciler(reader, readerPlane);
        }

        /** Publishes one more write and waits for the reader to have taken the push of it, if the writer pushes. */
        void publishOne(String id, boolean expectPush) throws Exception {
            final long received = reader.router().commitPush().counts()[2];
            writer.writes().index(onWriter, id, "{\"msg\":\"" + id + "\"}");
            final var published = writer.shards().publishShard(onWriter, term);
            if (expectPush) {
                assertBusy(() -> {
                    assertEquals(received + 1, reader.router().commitPush().counts()[2]);
                    final var open = reader.reconciler().readerCommit(onReader);
                    assertTrue(open.isPresent());
                    assertEquals(published.files(), open.get().files());
                }, 10, TimeUnit.SECONDS);
            }
        }

        /** Runs a reader refresh pass and returns how many manifests it read. */
        long refreshPass() {
            store.reset();
            readerLoop.refreshReaders();
            return manifestReads(store);
        }

        @Override
        public void close() throws Exception {
            readerLoop.close();
            reader.close();
            writer.close();
            store.close();
        }
    }

    public void testAPushedReaderIsNotLookedAtAgainUntilTheBackstop() throws Exception {
        try (Pair pair = new Pair(Settings.EMPTY, Settings.EMPTY)) {
            pair.publishOne("b", true);
            assertEquals("the first pass looks, which starts the window", 1L, pair.refreshPass());
            assertEquals("its writer is live and pushes: no look", 0L, pair.refreshPass());
            assertEquals(0L, pair.refreshPass());
            assertTrue(pair.reader.router().commitPush().registerLooks().counts()[0] >= 2);
            // A push still moves it, with no look.
            pair.publishOne("c", true);
            assertEquals(0L, pair.refreshPass());
            assertEquals(1L, ShardOps.hits(pair.reader.searchService(), pair.onReader, "msg", "c"));
        }
    }

    public void testAWriterThatDoesNotPushKeepsEveryLook() throws Exception {
        try (Pair pair = new Pair(Settings.builder().put(CommitPush.ENABLED, false).build(), Settings.EMPTY)) {
            pair.publishOne("b", false);
            assertEquals("the look finds the reader behind and lets it go", 1L, pair.refreshPass());
            final ShardId reopened = pair.reader.readers().serveAsReader(pair.readerPlane, "looked", 0);
            assertEquals(1L, ShardOps.hits(pair.reader.searchService(), reopened, "msg", "b"));
            assertEquals("no push was ever seen from this writer", 1L, pair.refreshPass());
            assertEquals(1L, pair.refreshPass());
        }
    }

    public void testTheSwitchOffKeepsEveryLook() throws Exception {
        try (Pair pair = new Pair(Settings.EMPTY, Settings.builder().put(ReaderRegisterLooks.ENABLED, false).build())) {
            pair.publishOne("b", true);
            assertEquals(1L, pair.refreshPass());
            assertEquals(1L, pair.refreshPass());
            assertEquals(0L, pair.reader.router().commitPush().registerLooks().counts()[0]);
        }
    }

    public void testABackstopOfZeroLooksEveryTime() throws Exception {
        try (Pair pair = new Pair(Settings.EMPTY, Settings.builder().put(ReaderRegisterLooks.BACKSTOP, 0L).build())) {
            pair.publishOne("b", true);
            assertEquals(1L, pair.refreshPass());
            assertEquals(1L, pair.refreshPass());
        }
    }

    public void testAWriterThatLeavesBringsTheLookBack() throws Exception {
        try (Pair pair = new Pair(Settings.EMPTY, Settings.EMPTY)) {
            pair.publishOne("b", true);
            assertEquals(1L, pair.refreshPass());
            assertEquals(0L, pair.refreshPass());
            // The writer's lease leaves the membership, as a dead or released node's does.
            pair.writerPlane.membership().release(pair.writer.localNode().getId());
            pair.readerPlane.membership().refresh();
            assertEquals("a departure since the last look brings it back", 1L, pair.refreshPass());
            assertEquals("and with the writer gone it stays back", 1L, pair.refreshPass());
        }
    }
}
