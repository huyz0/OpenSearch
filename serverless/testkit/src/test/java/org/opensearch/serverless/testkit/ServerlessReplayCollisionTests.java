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
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A shard whose log holds an acknowledged operation under a sequence number its restored commit gives to a different
 * document refuses to open, rather than opening without it.
 *
 * <p>Core's recovery skips a replayed operation whose sequence number the commit has already processed. A fleet run lost
 * three acknowledged writes whose records were in the log at a takeover and never reached the successor's commit; one
 * way that happens is two operations sharing a number. Opened, the shard would drop one silently and its next publish
 * would trim the record away for good. Refused, it is an outage, named in the log, with both operations still there.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessReplayCollisionTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-replay-collision")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    public void testLegacyReplayRefusesAnInvalidLoggedDocument() throws Exception {
        assertLegacyReplay("{\"properties\":{\"n\":{\"type\":\"long\"}}}", "{\"n\":\"not a number\"}", false);
    }

    public void testLegacyReplayRefusesAnUnappliedMappingUpdate() throws Exception {
        assertLegacyReplay("{\"properties\":{}}", "{\"unmapped\":\"logged\"}", false);
    }

    public void testLegacyReplayStillAppliesValidLoggedOperations() throws Exception {
        assertLegacyReplay("{\"properties\":{\"n\":{\"type\":\"long\"}}}", "{\"n\":7}", true);
    }

    private void assertLegacyReplay(String mapping, String source, boolean succeeds) throws Exception {
        final var clock = new AtomicLong(1_000L);
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            final var descriptor = new IndexDescriptor("legacy", "uuid-legacy", 1, mapping, null);
            plane.createIndex(descriptor);
            plane.walStore("legacy", descriptor.uuid(), 0).append(1L, new WalRecord("logged", source));
            try (var node = new ServerlessNode(nodeSettings("legacy-replay-result"))) {
                node.start();
                node.setMetadataPlane(plane);
                if (succeeds) {
                    final var shard = node.activateWriter(plane, "legacy", 0).orElseThrow();
                    assertTrue("a valid old record is still replayed", node.get(shard, "logged").found());
                } else {
                    expectThrows(Exception.class, () -> node.activateWriter(plane, "legacy", 0));
                    assertTrue("a rejected logged operation must prevent activation", node.reconciler().openShards().isEmpty());
                    assertTrue(
                        "the log must remain available for repair and retry",
                        plane.walStore("legacy", descriptor.uuid(), 0)
                            .replayableAfterFencing()
                            .stream()
                            .anyMatch(record -> record.id().equals("logged"))
                    );
                }
            }
        }
    }

    public void testAShardWhoseLogCollidesWithItsCommitRefusesToOpen() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final IndexDescriptor alpha = new IndexDescriptor(
            "alpha",
            "uuid-alpha-0000000000",
            1,
            "{\"properties\":{\"msg\":{\"type\":\"text\"}}}",
            null
        );
        plane.createIndex(alpha);
        try (
            ServerlessNode first = new ServerlessNode(nodeSettings("collision-first"));
            ServerlessNode second = new ServerlessNode(nodeSettings("collision-second"))
        ) {
            first.start();
            first.setMetadataPlane(plane);
            first.renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane).setDemandDrivenActivation(true);
            loop.activateOnDemand(List.of(Map.entry("alpha", 0)));
            final ShardId shardId = first.reconciler().openShards().iterator().next();
            first.index(shardId, "a", "{\"msg\":\"a\"}");
            loop.publishAll();
            loop.setIdleAfterMillis(1);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
            assertTrue(first.reconciler().openShards().isEmpty());

            // A different acknowledged operation under the same sequence number, at a later term: what a writer that
            // opened from an outdated commit would have written.
            final long term = plane.heads().read("alpha", 0).orElseThrow().term() + 1;
            plane.walStore("alpha", alpha.uuid(), 0).append(term, new WalRecord("b", "{\"msg\":\"b\"}", 0L, term, 1L));

            second.start();
            second.setMetadataPlane(plane);
            second.renewLease(plane);
            final BackgroundReconciler other = new BackgroundReconciler(second, plane).setDemandDrivenActivation(true);
            try {
                other.activateOnDemand(List.of(Map.entry("alpha", 0)));
            } catch (Exception expected) {
                // Refused: the activation reports it, or settles without the shard.
            }
            assertTrue("the shard does not open without the colliding write", second.reconciler().openShards().isEmpty());
            assertNull("and its head is given back for the next attempt", plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
        }
    }

    /**
     * A write reaching a reader that is open but not recorded as one -- the moment between a reader's open and its
     * recording, before readers were recorded first -- is refused and logs nothing.
     *
     * <p>On an ingest node a reader's engine accepts writes, at its commit's term and numbered from its commit. A fleet
     * run found one acknowledged that way: its first append began the term unchecked, fenced the writer's records off
     * behind it, and logged a sequence number the writer had already given -- which replay would have dropped one of.
     */
    public void testAWriteToAReaderIsNeverLogged() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final IndexDescriptor alpha = new IndexDescriptor(
            "alpha",
            "uuid-alpha-0000000000",
            1,
            "{\"properties\":{\"msg\":{\"type\":\"text\"}}}",
            null
        );
        plane.createIndex(alpha);
        try (
            ServerlessNode writer = new ServerlessNode(nodeSettings("reader-write-writer"));
            ServerlessNode other = new ServerlessNode(nodeSettings("reader-write-other"))
        ) {
            writer.start();
            writer.setMetadataPlane(plane);
            writer.renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(writer, plane).setDemandDrivenActivation(true);
            loop.activateOnDemand(List.of(Map.entry("alpha", 0)));
            writer.index(writer.reconciler().openShards().iterator().next(), "a", "{\"msg\":\"a\"}");
            loop.publishAll();
            loop.setIdleAfterMillis(1);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);

            other.start();
            other.setMetadataPlane(plane);
            other.renewLease(plane);
            final ShardId reader = other.serveAsReader(plane, "alpha", 0);
            other.reconciler().unrecordReaderForTesting(reader);

            expectThrows(java.io.IOException.class, () -> other.index(reader, "b", "{\"msg\":\"b\"}"));
            for (WalRecord record : plane.walStore("alpha", alpha.uuid(), 0).replayableAfterFencing()) {
                assertNotEquals("nothing written through a reader reaches the log", "b", record.id());
            }
        }
    }

    /**
     * Two records in the log itself under one sequence number, both past the commit: what a writer's record landing past
     * its successor's fence leaves. Core would apply the first and skip the second, and the open's first publish would
     * trim it -- which is how a fleet run lost three acknowledged writes. The shard refuses to open instead.
     */
    public void testAShardWhoseLogHoldsOneSequenceNumberTwiceRefusesToOpen() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final IndexDescriptor alpha = new IndexDescriptor(
            "alpha",
            "uuid-alpha-0000000000",
            1,
            "{\"properties\":{\"msg\":{\"type\":\"text\"}}}",
            null
        );
        plane.createIndex(alpha);
        try (
            ServerlessNode first = new ServerlessNode(nodeSettings("twice-first"));
            ServerlessNode second = new ServerlessNode(nodeSettings("twice-second"))
        ) {
            first.start();
            first.setMetadataPlane(plane);
            first.renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane).setDemandDrivenActivation(true);
            loop.activateOnDemand(List.of(Map.entry("alpha", 0)));
            final ShardId shardId = first.reconciler().openShards().iterator().next();
            first.index(shardId, "a", "{\"msg\":\"a\"}");
            loop.publishAll();
            loop.setIdleAfterMillis(1);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
            assertTrue(first.reconciler().openShards().isEmpty());

            // Sequence number 1, twice: an older writer's record past the fence, and its successor's own write.
            final long term = plane.heads().read("alpha", 0).orElseThrow().term();
            plane.walStore("alpha", alpha.uuid(), 0).append(term, new WalRecord("stale", "{\"msg\":\"stale\"}", 1L, term, 1L));
            plane.walStore("alpha", alpha.uuid(), 0)
                .append(term + 1, new WalRecord("acknowledged", "{\"msg\":\"acknowledged\"}", 1L, term + 1, 1L));

            second.start();
            second.setMetadataPlane(plane);
            second.renewLease(plane);
            final BackgroundReconciler other = new BackgroundReconciler(second, plane).setDemandDrivenActivation(true);
            try {
                other.activateOnDemand(List.of(Map.entry("alpha", 0)));
            } catch (Exception expected) {
                // Refused: the activation reports it, or settles without the shard.
            }
            assertTrue("the shard does not open dropping one of them", second.reconciler().openShards().isEmpty());
            assertNull("and its head is given back for the next attempt", plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
        }
    }

    public void testLegacyPrefixCannotOverwriteANewerSequencedWrite() throws Exception {
        assertMixedReplay(
            java.util.List.of(new WalRecord("logged", "{\"n\":1}"), new WalRecord("logged", "{\"n\":9}", 0L, 1L, 1L)),
            "{\"n\":9}"
        );
    }

    public void testLegacyPrefixCannotResurrectASequencedDelete() throws Exception {
        assertMixedReplay(java.util.List.of(new WalRecord("logged", "{\"n\":1}"), WalRecord.deletion("logged", 0L, 1L, 1L)), null);
    }

    public void testLegacyDeletePrefixCannotEraseASequencedWrite() throws Exception {
        assertMixedReplay(java.util.List.of(WalRecord.deletion("logged"), new WalRecord("logged", "{\"n\":9}", 0L, 1L, 1L)), "{\"n\":9}");
    }

    public void testLegacySuffixStillAppliesAfterASequencedWrite() throws Exception {
        assertMixedReplay(
            java.util.List.of(new WalRecord("logged", "{\"n\":1}", 0L, 1L, 1L), new WalRecord("logged", "{\"n\":9}")),
            "{\"n\":9}"
        );
    }

    private void assertMixedReplay(java.util.List<WalRecord> records, String expected) throws Exception {
        final var clock = new AtomicLong(1_000L);
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            final var descriptor = new IndexDescriptor("mixed", "uuid-mixed", 1, "{\"properties\":{\"n\":{\"type\":\"long\"}}}", null);
            plane.createIndex(descriptor);
            plane.walStore("mixed", descriptor.uuid(), 0).append(1L, records);
            try (var node = new ServerlessNode(nodeSettings("mixed-replay-order"))) {
                node.start();
                node.setMetadataPlane(plane);
                final var shard = node.activateWriter(plane, "mixed", 0).orElseThrow();
                final var document = node.get(shard, "logged");
                assertEquals(expected != null, document.found());
                assertEquals(expected, document.source());
            }
        }
    }

    public void testMixedReplayRetainsUnrelatedLegacyDocuments() throws Exception {
        assertMixedReplay(
            java.util.List.of(new WalRecord("logged", "{\"n\":9}"), new WalRecord("other", "{\"n\":1}", 0L, 1L, 1L)),
            "{\"n\":9}"
        );
    }

    public void testLegacyDeleteSuffixStillAppliesAfterASequencedWrite() throws Exception {
        assertMixedReplay(java.util.List.of(new WalRecord("logged", "{\"n\":1}", 0L, 1L, 1L), WalRecord.deletion("logged")), null);
    }

    public void testSameSequenceIdentityCannotHideDifferentSources() throws Exception {
        assertSequencedConflict(
            java.util.List.of(new WalRecord("logged", "{\"n\":1}", 0L, 1L, 1L), new WalRecord("logged", "{\"n\":9}", 0L, 1L, 1L))
        );
    }

    public void testSameSequenceIdentityCannotHideDifferentVersions() throws Exception {
        assertSequencedConflict(
            java.util.List.of(new WalRecord("logged", "{\"n\":1}", 0L, 1L, 1L), new WalRecord("logged", "{\"n\":1}", 0L, 1L, 2L))
        );
    }

    public void testSameSequenceDeleteCannotHideDifferentVersions() throws Exception {
        assertSequencedConflict(java.util.List.of(WalRecord.deletion("logged", 0L, 1L, 1L), WalRecord.deletion("logged", 0L, 1L, 2L)));
    }

    public void testSameSequenceDeleteCannotHideDifferentDocumentIds() throws Exception {
        assertSequencedConflict(java.util.List.of(WalRecord.deletion("logged", 0L, 1L, 1L), WalRecord.deletion("other", 0L, 1L, 1L)));
    }

    public void testIdenticalSequencedWritesRemainReplayable() throws Exception {
        final var record = new WalRecord("logged", "{\"n\":9}", 0L, 1L, 1L);
        assertMixedReplay(java.util.List.of(record, record), "{\"n\":9}");
    }

    public void testIdenticalSequencedDeletesRemainReplayable() throws Exception {
        final var record = WalRecord.deletion("logged", 0L, 1L, 1L);
        assertMixedReplay(java.util.List.of(record, record), null);
    }

    private void assertSequencedConflict(java.util.List<WalRecord> records) throws Exception {
        final var clock = new AtomicLong(1_000L);
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            final var descriptor = new IndexDescriptor(
                "conflict",
                "uuid-conflict",
                1,
                "{\"properties\":{\"n\":{\"type\":\"long\"}}}",
                null
            );
            plane.createIndex(descriptor);
            plane.walStore("conflict", descriptor.uuid(), 0).append(1L, records);
            try (var node = new ServerlessNode(nodeSettings("payload-replay-conflict"))) {
                node.start();
                node.setMetadataPlane(plane);
                final Exception failure = expectThrows(Exception.class, () -> node.activateWriter(plane, "conflict", 0));
                final String reason = org.opensearch.ExceptionsHelper.detailedMessage(failure);
                assertTrue(reason, reason.contains("share a sequence number"));
                assertTrue(node.reconciler().openShards().isEmpty());
            }
        }
    }
}
