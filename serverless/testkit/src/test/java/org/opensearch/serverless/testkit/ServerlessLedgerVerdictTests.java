/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * The fleet ledger's durability verdict says lost only when a write is lost.
 *
 * <p>Two fleet runs reported 16 and 399 acknowledged writes lost; every one was found by hand afterwards, on shards that
 * had been changing hands while the read-back asked for them. A checker that cries wolf is one that will be ignored the
 * day the loss is real. {@link DurabilityCheck} reads the commit and the log from the store, needs no owner, and answers
 * in three: verified, lost with its evidence, or unreached.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessLedgerVerdictTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";
    private static final IndexDescriptor ALPHA = new IndexDescriptor("alpha", "uuid-alpha-0000000000", 1, MAPPING, null);

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-ledger-verdict")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static MetadataPlane plane(BlobStore store, AtomicLong clock) {
        return new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
    }

    /** A node holding alpha, with its writes acknowledged and in the log only. */
    private ShardId writeAcknowledged(ServerlessNode node, MetadataPlane plane, List<String> ids) throws Exception {
        node.start();
        node.setMetadataPlane(plane);
        node.renewLease(plane);
        new BackgroundReconciler(node, plane).setDemandDrivenActivation(true).activateOnDemand(List.of(Map.entry("alpha", 0)));
        final ShardId shardId = node.reconciler().openShards().iterator().next();
        for (String id : ids) {
            node.index(shardId, id, "{\"msg\":\"m\",\"n\":1}");
        }
        return shardId;
    }

    private static List<String> ids(String prefix, int n) {
        final List<String> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ids.add(prefix + "-" + i);
        }
        return ids;
    }

    /** A write whose log record is gone, never published, is lost -- and the verdict says what was read. */
    public void testAPlantedLossIsReportedLostWithItsEvidence() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path dir = createTempDir();
        final BlobStore store = new FsBlobStore(1024, dir, false);
        final MetadataPlane plane = plane(store, clock);
        plane.createIndex(ALPHA);
        final List<String> ids = ids("kept", 3);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("verdict-loss"))) {
            final List<String> all = new ArrayList<>(ids);
            all.add("planted-loss-xyzzy");
            writeAcknowledged(node, plane, all);

            // After it was acknowledged, its record goes: the loss the ledger exists to catch.
            int removed = 0;
            try (Stream<Path> files = Files.walk(dir)) {
                for (Path file : files.filter(Files::isRegularFile).filter(p -> p.toString().contains("wal")).toList()) {
                    if (new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1).contains("planted-loss-xyzzy")) {
                        Files.delete(file);
                        removed++;
                    }
                }
            }
            assertEquals("the planted record was found and removed", 1, removed);

            final Map<String, DurabilityCheck.Finding> findings = new DurabilityCheck(plane, createTempDir()).check("alpha", all, 10_000L);
            for (String id : ids) {
                assertEquals(findings.get(id).toString(), DurabilityCheck.Status.VERIFIED, findings.get(id).status());
            }
            final DurabilityCheck.Finding lost = findings.get("planted-loss-xyzzy");
            assertEquals(lost.toString(), DurabilityCheck.Status.LOST, lost.status());
            assertTrue("with what was read: " + lost.evidence(), lost.evidence().contains("log:") && lost.evidence().contains("commit:"));
        }
    }

    /** A write whose record the log no longer holds, because a publish covered it, is found in the commit. */
    public void testAWritePublishedAndTruncatedFromTheLogIsVerified() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path dir = createTempDir();
        final BlobStore store = new FsBlobStore(1024, dir, false);
        final MetadataPlane plane = plane(store, clock);
        plane.createIndex(ALPHA);
        final List<String> ids = ids("published", 20);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("verdict-published"))) {
            final ShardId shardId = writeAcknowledged(node, plane, ids);
            node.publishShard(shardId, plane.heads().read("alpha", 0).orElseThrow().term());
            // What truncation leaves once a publish covers the records: none of them in the log.
            try (Stream<Path> files = Files.walk(dir)) {
                for (Path file : files.filter(Files::isRegularFile).filter(p -> p.toString().contains("wal")).toList()) {
                    if (new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1).contains("published-")) {
                        Files.delete(file);
                    }
                }
            }
            final Map<String, DurabilityCheck.Finding> findings = new DurabilityCheck(plane, createTempDir()).check("alpha", ids, 10_000L);
            for (String id : ids) {
                assertEquals(findings.get(id).toString(), DurabilityCheck.Status.VERIFIED, findings.get(id).status());
                assertTrue("from the commit: " + findings.get(id), findings.get(id).evidence().contains("commit"));
            }
        }
    }

    /**
     * A shard taken over -- replayed, published and its log truncated -- while the check reads it is verified on every
     * read: the handover a fleet read-back mistook for loss.
     */
    public void testAHandoverDuringTheCheckIsVerifiedNotLost() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final BlobStore store = new FsBlobStore(1024, createTempDir(), false);
        plane(store, clock).createIndex(ALPHA);
        final List<String> ids = ids("handed", 60);
        try (
            ServerlessNode before = new ServerlessNode(nodeSettings("verdict-before"));
            ServerlessNode after = new ServerlessNode(nodeSettings("verdict-after"))
        ) {
            writeAcknowledged(before, plane(store, clock), ids);
            clock.addAndGet(2 * TTL);

            final MetadataPlane takerPlane = plane(store, clock);
            after.start();
            after.setMetadataPlane(takerPlane);
            after.renewLease(takerPlane);
            final AtomicReference<Exception> takeoverFailed = new AtomicReference<>();
            final Thread takeover = new Thread(() -> {
                try {
                    final BackgroundReconciler loop = new BackgroundReconciler(after, takerPlane).setDemandDrivenActivation(true);
                    loop.activateOnDemand(List.of(Map.entry("alpha", 0)));
                    loop.publishAll();
                } catch (Exception e) {
                    takeoverFailed.set(e);
                }
            }, "verdict-takeover");

            final DurabilityCheck check = new DurabilityCheck(plane(store, clock), createTempDir());
            takeover.start();
            int checks = 0;
            do {
                final Map<String, DurabilityCheck.Finding> findings = check.check("alpha", ids, 30_000L);
                checks++;
                for (String id : ids) {
                    assertEquals("check " + checks + ": " + findings.get(id), DurabilityCheck.Status.VERIFIED, findings.get(id).status());
                }
            } while (takeover.isAlive());
            takeover.join();
            assertNull("the takeover itself worked", takeoverFailed.get());
            final Map<String, DurabilityCheck.Finding> settled = check.check("alpha", ids, 30_000L);
            for (String id : ids) {
                assertEquals(settled.get(id).toString(), DurabilityCheck.Status.VERIFIED, settled.get(id).status());
            }
        }
    }

    /** A log that cannot be read is unreached, never lost: the verdict does not guess. */
    public void testAnUnreadableStoreIsUnreachedNotLost() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final HookedBlobStore store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = plane(store, clock);
        plane.createIndex(ALPHA);
        final List<String> ids = ids("unreached", 5);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("verdict-unreached"))) {
            writeAcknowledged(node, plane, ids);
            store.failListingsUnder("/wal/");
            final Map<String, DurabilityCheck.Finding> findings = new DurabilityCheck(plane, createTempDir()).check("alpha", ids, 2_000L);
            for (String id : ids) {
                assertEquals(findings.get(id).toString(), DurabilityCheck.Status.UNREACHED, findings.get(id).status());
            }
            store.failListingsUnder(null);
        }
    }
}
