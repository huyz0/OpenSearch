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
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A writer that pushes its commits moves the readers of a shard onto each one it publishes, with no register read and
 * no reader refresh pass; with pushing off, a reader stays where it is until its register look.
 */
public class CommitPushTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}";

    private Settings nodeSettings(String name, String roles, boolean push) {
        return nodeSettings(name, roles, push, CommitPush.DEFAULT_INLINE_BYTES);
    }

    private Settings nodeSettings(String name, String roles, boolean push, long inlineBytes) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-commit-push")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put(CommitPush.ENABLED, push)
            .put(CommitPush.INLINE_BYTES, inlineBytes + "b")
            // The commit alone: what a reader sees of unpublished writes is the overlay's business, measured on its own.
            .put(org.opensearch.serverless.transport.TailOverlays.ENABLED, false)
            .build();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        return node;
    }

    private void run(boolean push) throws Exception {
        run(push, CommitPush.DEFAULT_INLINE_BYTES);
    }

    private void run(boolean push, long inlineBytes) throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            plane.createIndex(new IndexDescriptor("pushed", "uuid-pushed-00000000", 1, MAPPING, null));
            try (
                ServerlessNode writer = joined(new ServerlessNode(nodeSettings("push-writer", "ingest", push, inlineBytes)), plane);
                ServerlessNode reader = joined(new ServerlessNode(nodeSettings("push-reader", "search", push, inlineBytes)), plane)
            ) {
                // The writer ranks readers from its membership snapshot, which a running node refreshes every renewal.
                plane.membership().refresh();
                final ShardId shardId = writer.activation().activateWriter(plane, "pushed", 0).orElseThrow();
                final long term = plane.heads().read("pushed", 0).orElseThrow().term();
                writer.writes().index(shardId, "a", "{\"msg\":\"first\"}");
                writer.shards().publishShard(shardId, term);
                final ShardId onReader = reader.readers().serveAsReader(plane, "pushed", 0);
                assertEquals(1L, ShardOps.hits(reader.searchService(), onReader, "msg", "first"));
                final CommitManifest before = reader.reconciler().readerCommit(onReader).orElseThrow();

                writer.writes().index(shardId, "b", "{\"msg\":\"second\"}");
                final long[] readerBefore = reader.router().commitPush().counts();
                final long[] writerBefore = writer.router().commitPush().counts();
                final CommitManifest published = writer.shards().publishShard(shardId, term);
                store.reset();
                if (push) {
                    // Nothing on the reader runs a refresh pass here: only the push can move it.
                    assertBusy(() -> {
                        final var now = reader.reconciler().readerCommit(onReader);
                        assertTrue("the reader must be open on the pushed commit", now.isPresent());
                        assertEquals(published.files(), now.get().files());
                    }, 10, TimeUnit.SECONDS);
                    assertEquals(1L, ShardOps.hits(reader.searchService(), onReader, "msg", "second"));
                    // A descriptor and a head read, as any reader open makes, and the pushed segment's blocks: not the manifest.
                    assertBusy(() -> {
                        final long[] readerAfter = reader.router().commitPush().counts();
                        assertEquals("one push taken", 1L, readerAfter[2] - readerBefore[2]);
                        assertEquals("one reader reopened", 1L, readerAfter[3] - readerBefore[3]);
                    });
                    for (String line : store.byPlace().split("\n")) {
                        assertFalse(
                            "the reader's move reads no manifest: " + line,
                            line.contains("REGISTER_GET") && line.contains("manifest")
                        );
                    }
                    assertBusy(() -> assertEquals(1L, writer.router().commitPush().counts()[0] - writerBefore[0]));
                    final long taken = reader.router().commitPush().counts()[5] - readerBefore[5];
                    if (inlineBytes > 0) {
                        // The new segment came with the push: the reopen and the search read no segment block from the store.
                        assertTrue("the push carried the new segment's blocks", taken > 0);
                        assertEquals("no segment block read from the store: " + store.byPlace(), 0L, store.blobReads());
                        // The writer's mapping is the one the reader already serves with: no descriptor read either.
                        assertEquals("the reopen reused the served descriptor", 1L, reader.readers().descriptorReadsSkipped());
                    } else {
                        assertEquals("nothing carried with inlining off", 0L, taken);
                        assertTrue("the reopen read the new segment from the store", store.blobReads() > 0);
                    }
                } else {
                    // Give a push, were there one, the time it would take.
                    Thread.sleep(500);
                    assertEquals(before.files(), reader.reconciler().readerCommit(onReader).orElseThrow().files());
                    assertEquals(0L, ShardOps.hits(reader.searchService(), onReader, "msg", "second"));
                    assertEquals("nothing pushed with pushing off", 0L, reader.router().commitPush().counts()[2]);
                }

                // A push of a commit no newer than the one open does not move the reader.
                if (push) {
                    final CommitManifest open = reader.reconciler().readerCommit(onReader).orElseThrow();
                    assertFalse(CommitPush.newer(before, open));
                    assertTrue(CommitPush.newer(open, before));
                }
            }
        }
    }

    public void testAPushedCommitMovesTheReaderAtOnce() throws Exception {
        run(true);
    }

    public void testWithInliningOffTheReaderReadsTheNewSegmentFromTheStore() throws Exception {
        run(true, 0);
    }

    public void testWithoutPushTheReaderStaysUntilItsRegisterLook() throws Exception {
        run(false);
    }
}
