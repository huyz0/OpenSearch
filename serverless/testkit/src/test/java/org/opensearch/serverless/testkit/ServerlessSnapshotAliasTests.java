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
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.repository.SnapshotAliases;
import org.opensearch.serverless.repository.SnapshotRecord;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.snapshot.SnapshotAliasRestore;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A snapshot captures the aliases of its indices behind {@code serverless.snapshots.take.capture_aliases}, and a restore
 * with {@code include_aliases} brings them back, renamed if asked, never at the cost of an index.
 */
public class ServerlessSnapshotAliasTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name, boolean capture) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-snapshot-alias")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest");
        if (capture) {
            builder.put("serverless.snapshots.take.capture_aliases", true);
        }
        return builder.build();
    }

    /** A node over a fresh plane, with each named index created, written to once and published. */
    private interface Body {
        void run(ServerlessNode node, MetadataPlane plane) throws Exception;
    }

    private void withIndices(boolean capture, Body body, String... indices) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        metered = new org.opensearch.serverless.store.MeteredBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(metered, BlobPath.cleanPath(), clock::get, TTL);
        for (String index : indices) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index + "-0000000", 1, MAPPING, null));
        }
        try (
            ServerlessNode node = new ServerlessNode(
                nodeSettings("alias-" + randomAlphaOfLength(5).toLowerCase(java.util.Locale.ROOT), capture)
            )
        ) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (String index : indices) {
                loop.want(index, 0);
            }
            loop.tick(clock.get());
            for (String index : indices) {
                assertEquals(201, send(node, "PUT", "/" + index + "/_doc/1?refresh=true", "{\"msg\":\"doc\"}").status());
            }
            loop.tick(clock.get());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());
            body.run(node, plane);
        }
    }

    private org.opensearch.serverless.store.MeteredBlobStore metered;

    private static void take(ServerlessNode node, String name, String indices) throws Exception {
        final Response taken = send(node, "PUT", "/_snapshot/backups/" + name, "{\"indices\":\"" + indices + "\"}");
        assertEquals(taken.body(), 200, taken.status());
    }

    private static Response restore(ServerlessNode node, String name, String body) throws Exception {
        return send(node, "POST", "/_snapshot/backups/" + name + "/_restore?wait_for_completion=true", body);
    }

    private static AliasRecord alias(MetadataPlane plane, String name) throws Exception {
        return plane.resolve(name).alias();
    }

    public void testATakeRecordsAliasesOnlyWhenTheSettingIsOn() throws Exception {
        for (boolean capture : new boolean[] { false, true }) {
            withIndices(capture, (node, plane) -> {
                plane.createAlias(new AliasRecord("read", List.of("one", "two")));
                plane.createAlias(new AliasRecord("write", List.of("one")).withWriteMember("one"));
                plane.createAlias(new AliasRecord("implicit", List.of("two")));
                take(node, "s", "one,two");
                final SnapshotRecord record = plane.snapshot("backups", "s").orElseThrow();
                final String bytes = record.toBytes().utf8ToString();
                if (capture == false) {
                    assertFalse(bytes, bytes.contains("aliases"));
                    return;
                }
                assertEquals(Map.of("read", false, "write", true), SnapshotAliases.read(record.indices().get("one")).writeFlags());
                // An implicit write target is recorded as an ordinary member.
                assertEquals(Map.of("read", false, "implicit", false), SnapshotAliases.read(record.indices().get("two")).writeFlags());
                assertFalse(SnapshotAliases.read(record.indices().get("one")).truncated());
            }, "one", "two");
        }
    }

    /** On, a take reads each distinct alias register once, however many indices it names; off, it reads none of them. */
    public void testATakeReadsEachDistinctAliasOnceAndNoneWhenOff() throws Exception {
        final long[] reads = new long[2];
        for (int on = 0; on < 2; on++) {
            final int mode = on;
            withIndices(on == 1, (node, plane) -> {
                plane.createAlias(new AliasRecord("shared", List.of("one", "two", "three")));
                plane.createAlias(new AliasRecord("solo", List.of("one")));
                final long before = metered.registerReads();
                take(node, "s", "one,two,three");
                reads[mode] = metered.registerReads() - before;
            }, "one", "two", "three");
        }
        assertEquals("two distinct aliases, however many indices they name", 2L, reads[1] - reads[0]);
    }

    public void testATakeIgnoresAStaleHintAndAStream() throws Exception {
        withIndices(true, (node, plane) -> {
            plane.createAlias(new AliasRecord("kept", List.of("one")));
            plane.createAlias(new AliasRecord("gone", List.of("one")));
            plane.deleteAlias("gone");
            take(node, "s", "one");
            assertEquals(
                Map.of("kept", false),
                SnapshotAliases.read(plane.snapshot("backups", "s").orElseThrow().indices().get("one")).writeFlags()
            );
        }, "one");
    }

    public void testATakeNamesAtMostSixtyFourAliasesAndSaysWhenItCutThem() throws Exception {
        withIndices(true, (node, plane) -> {
            for (int i = 0; i < 70; i++) {
                plane.createAlias(new AliasRecord(String.format(java.util.Locale.ROOT, "a%02d", i), List.of("one")));
            }
            take(node, "s", "one");
            final var recorded = SnapshotAliases.read(plane.snapshot("backups", "s").orElseThrow().indices().get("one"));
            assertEquals(64, recorded.writeFlags().size());
            assertTrue(recorded.writeFlags().containsKey("a00"));
            assertTrue("the first 64 names by order", recorded.writeFlags().containsKey("a63"));
            assertFalse(recorded.writeFlags().containsKey("a64"));
            assertTrue(recorded.truncated());
            final Response restored = restore(
                node,
                "s",
                "{\"include_aliases\":true,\"rename_pattern\":\"one\",\"rename_replacement\":\"back\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(restored.body(), restored.body().contains("\"aliases_truncated\":[\"back\"]"));
        }, "one");
    }

    public void testARestoreBringsAliasesBackUnderTheirRenamedNames() throws Exception {
        withIndices(true, (node, plane) -> {
            plane.createAlias(new AliasRecord("read", List.of("one")));
            plane.createAlias(new AliasRecord("write", List.of("one")).withWriteMember("one"));
            take(node, "s", "one");
            final Response restored = restore(
                node,
                "s",
                "{\"rename_pattern\":\"one\",\"rename_replacement\":\"copy\",\"include_aliases\":true,"
                    + "\"rename_alias_pattern\":\"^(.*)$\",\"rename_alias_replacement\":\"$1-restored\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(restored.body(), restored.body().contains("\"aliases_restored\":2"));
            assertEquals(List.of("copy"), alias(plane, "read-restored").indices());
            assertNull(alias(plane, "read-restored").writeMember());
            assertEquals("copy", alias(plane, "write-restored").writeMember());
            // The originals are untouched.
            assertEquals(List.of("one"), alias(plane, "read").indices());
        }, "one");
    }

    public void testWithoutIncludeAliasesNothingIsRestoredAndTheRenameKeysAreIgnored() throws Exception {
        withIndices(true, (node, plane) -> {
            plane.createAlias(new AliasRecord("read", List.of("one")));
            take(node, "s", "one");
            final String[] extras = {
                "",
                ",\"include_aliases\":false",
                ",\"rename_alias_pattern\":\"read\",\"rename_alias_replacement\":\"x\"" };
            for (int i = 0; i < extras.length; i++) {
                final Response restored = restore(
                    node,
                    "s",
                    "{\"rename_pattern\":\"one\",\"rename_replacement\":\"c" + i + "\"" + extras[i] + "}"
                );
                assertEquals(restored.body(), 200, restored.status());
                assertFalse(restored.body(), restored.body().contains("aliases_restored"));
                assertTrue(plane.describe("c" + i).isPresent());
            }
            assertEquals(List.of("one"), alias(plane, "read").indices());
            assertTrue(plane.resolve("x").absent());
        }, "one");
    }

    public void testIncludeAliasesOfASnapshotThatRecordedNoneSaysSo() throws Exception {
        withIndices(false, (node, plane) -> {
            plane.createAlias(new AliasRecord("read", List.of("one")));
            take(node, "s", "one");
            final Response restored = restore(
                node,
                "s",
                "{\"include_aliases\":true,\"rename_pattern\":\"one\",\"rename_replacement\":\"copy\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(restored.body(), restored.body().contains("\"aliases_restored\":0"));
            assertEquals(List.of("one"), alias(plane, "read").indices());
        }, "one");
    }

    public void testARestoredMemberJoinsAnAliasThatAlreadyWritesAsNonWrite() throws Exception {
        withIndices(true, (node, plane) -> {
            plane.createAlias(new AliasRecord("w", List.of("one")).withWriteMember("one"));
            take(node, "s", "one");
            // Explicit write target elsewhere: the restored index joins, and does not take the write.
            plane.createIndex(new IndexDescriptor("other", "uuid-other-0000000", 1, MAPPING, null));
            plane.deleteAlias("w");
            plane.createAlias(new AliasRecord("w", List.of("other")).withWriteMember("other"));
            assertEquals(
                200,
                restore(node, "s", "{\"include_aliases\":true,\"rename_pattern\":\"one\",\"rename_replacement\":\"c1\"}").status()
            );
            assertEquals(List.of("other", "c1"), alias(plane, "w").indices());
            assertEquals("other", alias(plane, "w").writeMember());
            // Implicit write target (one member, no flag): that member still writes, so the restored one is non-write.
            plane.deleteAlias("w");
            plane.createAlias(new AliasRecord("w", List.of("other")));
            assertEquals(
                200,
                restore(node, "s", "{\"include_aliases\":true,\"rename_pattern\":\"one\",\"rename_replacement\":\"c2\"}").status()
            );
            assertTrue(alias(plane, "w").indices().contains("c2"));
            assertNull(alias(plane, "w").writeMember());
        }, "one");
    }

    public void testTwoRestoredMembersClaimingTheWriteIndexLeaveItToTheFirst() throws Exception {
        withIndices(true, (node, plane) -> {
            plane.createAlias(new AliasRecord("w1", List.of("one")).withWriteMember("one"));
            plane.createAlias(new AliasRecord("w2", List.of("two")).withWriteMember("two"));
            take(node, "s", "one,two");
            final Response restored = restore(
                node,
                "s",
                "{\"indices\":\"one,two\",\"rename_pattern\":\"^(.*)$\",\"rename_replacement\":\"c-$1\",\"include_aliases\":true,"
                    + "\"rename_alias_pattern\":\"w[12]\",\"rename_alias_replacement\":\"w\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            assertEquals(List.of("c-one", "c-two"), alias(plane, "w").indices());
            assertEquals("c-one", alias(plane, "w").writeMember());
            assertTrue(restored.body(), restored.body().contains("\"aliases_restored\":1"));
        }, "one", "two");
    }

    public void testAnAliasNameThatIsAnIndexRefusesTheWholeRestoreBeforeAnythingIsCreated() throws Exception {
        withIndices(true, (node, plane) -> {
            plane.createAlias(new AliasRecord("read", List.of("one")));
            take(node, "s", "one");
            // The alias would be named like the index being restored.
            Response refused = restore(
                node,
                "s",
                "{\"rename_pattern\":\"one\",\"rename_replacement\":\"x\",\"include_aliases\":true,"
                    + "\"rename_alias_pattern\":\"read\",\"rename_alias_replacement\":\"x\"}"
            );
            assertEquals(refused.body(), 400, refused.status());
            assertTrue(refused.body(), refused.body().contains("invalid_alias_name_exception"));
            assertTrue(plane.describe("x").isEmpty());
            // The alias would be named like an index that exists.
            refused = restore(
                node,
                "s",
                "{\"rename_pattern\":\"one\",\"rename_replacement\":\"y\",\"include_aliases\":true,"
                    + "\"rename_alias_pattern\":\"read\",\"rename_alias_replacement\":\"one\"}"
            );
            assertEquals(refused.body(), 400, refused.status());
            assertTrue(refused.body(), refused.body().contains("invalid_alias_name_exception"));
            assertTrue(plane.describe("y").isEmpty());
        }, "one");
    }

    public void testTheKeysThatStayRefusedAndANonBooleanFlag() throws Exception {
        withIndices(true, (node, plane) -> {
            take(node, "s", "one");
            assertEquals(501, restore(node, "s", "{\"alias_write_index_policy\":\"keep\"}").status());
            assertEquals(501, restore(node, "s", "{\"include_global_state\":true}").status());
            assertEquals(400, restore(node, "s", "{\"include_aliases\":\"maybe\"}").status());
            assertEquals(400, restore(node, "s", "{\"include_aliases\":3}").status());
            assertTrue(plane.describe("one").isPresent());
        }, "one");
    }

    private static SnapshotRecord hand(Object aliases, Object truncated) {
        return new SnapshotRecord(
            "backups",
            "hand",
            "uuid-hand",
            true,
            1_000L,
            2_000L,
            Map.of(
                "one",
                new SnapshotRecord.SnapshottedIndex(
                    "uuid-one-0000000",
                    1,
                    MAPPING,
                    Map.of(),
                    Map.of(0, new CommitManifest(1L, Map.of("f", "t=1"), null, Map.of())),
                    aliases,
                    truncated
                )
            )
        );
    }

    public void testACorruptAliasEntryIsUnreadableOnlyToTheRestoreThatUsesIt() throws Exception {
        withIndices(true, (node, plane) -> {
            plane.finishSnapshot(plane.createSnapshot(hand("not an object", null)));
            // Listing and describing read it.
            assertEquals(200, send(node, "GET", "/_snapshot/backups/hand", null).status());
            assertEquals(200, send(node, "GET", "/_snapshot/backups/_all", null).status());
            final Response unreadable = restore(
                node,
                "hand",
                "{\"include_aliases\":true,\"rename_pattern\":\"one\",\"rename_replacement\":\"copy\"}"
            );
            assertEquals(unreadable.body(), 500, unreadable.status());
            assertTrue(unreadable.body(), unreadable.body().contains("snapshot_unreadable"));
            assertTrue("nothing was created", plane.describe("copy").isEmpty());
        }, "one");
    }

    public void testARestoreOfMoreThanTheBoundOfDistinctAliasesIsRefused() throws Exception {
        withIndices(true, (node, plane) -> {
            final Map<String, SnapshotRecord.SnapshottedIndex> indices = new LinkedHashMap<>();
            for (int i = 0; i < 101; i++) {
                final Map<String, Object> aliases = new LinkedHashMap<>();
                for (int a = 0; a < 64; a++) {
                    aliases.put("i" + i + "-a" + a, Map.of());
                }
                indices.put(
                    "ix" + i,
                    new SnapshotRecord.SnapshottedIndex(
                        "uuid-ix" + i + "-000000",
                        1,
                        MAPPING,
                        Map.of(),
                        Map.of(0, new CommitManifest(1L, Map.of("f", "t=1"), null, Map.of())),
                        aliases,
                        null
                    )
                );
            }
            plane.finishSnapshot(plane.createSnapshot(new SnapshotRecord("backups", "big", "uuid-big", true, 1_000L, 2_000L, indices)));
            final Response refused = restore(node, "big", "{\"include_aliases\":true}");
            assertEquals(refused.body(), 400, refused.status());
            assertTrue(refused.body(), refused.body().contains("6400"));
            assertTrue(plane.describe("ix0").isEmpty());
        }, "one");
    }

    /** An alias that cannot be added never takes an index with it: it is reported, and the rest is applied. */
    public void testAnAliasThatCannotBeAddedIsReportedAndTheOthersAreApplied() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("idx", "uuid-idx-000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("taken", "uuid-taken-00000000", 1, MAPPING, null));
        final List<String> failed = SnapshotAliasRestore.apply(
            plane,
            Map.of(
                "taken",
                List.of(new SnapshotAliasRestore.Member("idx", false)),
                "fine",
                List.of(new SnapshotAliasRestore.Member("idx", true))
            ),
            Map.of()
        );
        assertEquals(List.of("taken"), failed);
        assertEquals(List.of("idx"), alias(plane, "fine").indices());
        assertTrue(plane.describe("idx").isPresent());
        assertTrue(plane.describe("taken").isPresent());
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)
                )
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
