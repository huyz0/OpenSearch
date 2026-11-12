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
import org.opensearch.serverless.transport.TailOverlays;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The check a search makes before a reader answers stands for longer while the writer it found stays live, the same
 * process, holding its lease without a gap; anything else brings the head read back.
 */
public class ReaderCheckWindowTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}";

    private Settings nodeSettings(String name, String roles, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-reader-check-window")
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

    private static long headReads(CountingBlobStore store) {
        long reads = 0;
        for (String line : store.byPlace().split("\n")) {
            if (line.contains("REGISTER_GET") && line.contains("shards/")) {
                reads += Long.parseLong(line.trim().split("\\s+")[0]);
            }
        }
        return reads;
    }

    /** Head reads made by two checks of a reader whose writer is live, more than the one-second window apart. */
    private long[] checksApart(Settings readerExtra, boolean writerLeaves) throws Exception {
        return checksApart(readerExtra, writerLeaves, false);
    }

    private long[] checksApart(Settings readerExtra, boolean writerLeaves, boolean newCommit) throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane writerPlane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            final MetadataPlane readerPlane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            writerPlane.createIndex(new IndexDescriptor("checked", "uuid-checked-00000000", 1, MAPPING, null));
            try (
                ServerlessNode writer = joined(new ServerlessNode(nodeSettings("check-writer", "ingest", Settings.EMPTY)), writerPlane);
                ServerlessNode reader = joined(new ServerlessNode(nodeSettings("check-reader", "search", readerExtra)), readerPlane)
            ) {
                writerPlane.membership().refresh();
                readerPlane.membership().refresh();
                final ShardId onWriter = writer.activation().activateWriter(writerPlane, "checked", 0).orElseThrow();
                final long term = writerPlane.heads().read("checked", 0).orElseThrow().term();
                writer.writes().index(onWriter, "a", "{\"msg\":\"a\"}");
                writer.shards().publishShard(onWriter, term);
                ShardId onReader = reader.readers().serveAsReader(readerPlane, "checked", 0);
                Thread.sleep(1_200);
                if (newCommit) {
                    // The writer publishes again and pushes the commit; the reader moves onto it in the background.
                    final long reopened = reader.router().commitPush().counts()[3];
                    writer.writes().index(onWriter, "b", "{\"msg\":\"b\"}");
                    writer.shards().publishShard(onWriter, term);
                    assertBusy(() -> assertEquals(reopened + 1, reader.router().commitPush().counts()[3]));
                    onReader = reader.reconciler().readerShards().iterator().next();
                }
                // A renewal, and a refresh of the reader's membership: neither is a gap in the writer's lease.
                writer.leases().renewLease(writerPlane);
                readerPlane.membership().refresh();
                if (writerLeaves) {
                    writerPlane.membership().release(writer.localNode().getId());
                    readerPlane.membership().refresh();
                }
                store.reset();
                try {
                    reader.readers().ensureReaderCurrent(readerPlane, "checked", 0, onReader);
                } catch (Exception e) {
                    // A reader whose writer has gone may be refused; what is counted here is whether the head was read.
                }
                final long first = headReads(store);
                store.reset();
                try {
                    reader.readers().ensureReaderCurrent(readerPlane, "checked", 0, onReader);
                } catch (Exception e) {
                    // As above.
                }
                return new long[] { first, headReads(store) };
            }
        }
    }

    public void testALiveWritersCheckStandsPastOneSecond() throws Exception {
        final long[] reads = checksApart(Settings.EMPTY, false);
        assertEquals("the writer is live and the same process: no head read", 0L, reads[0]);
        assertEquals(0L, reads[1]);
    }

    public void testWithTheWindowOffTheHeadIsReadEverySecond() throws Exception {
        final long[] reads = checksApart(Settings.builder().put("serverless.search.reader_check.live_owner_millis", 0L).build(), false);
        assertEquals("past one second, the head is read", 1L, reads[0]);
        assertEquals("and the check then stands for a second", 0L, reads[1]);
    }

    public void testACommitTheSameWriterPushedKeepsTheCheck() throws Exception {
        final long[] reads = checksApart(Settings.EMPTY, false, true);
        assertEquals("the pushed commit is the live writer's, in the same term: no head read", 0L, reads[0]);
        assertEquals(0L, reads[1]);
    }

    public void testAWriterThatLeavesBringsTheHeadReadBack() throws Exception {
        final long[] reads = checksApart(Settings.EMPTY, true);
        assertTrue("a departure brings the head read back: " + reads[0], reads[0] >= 1L);
    }
}
