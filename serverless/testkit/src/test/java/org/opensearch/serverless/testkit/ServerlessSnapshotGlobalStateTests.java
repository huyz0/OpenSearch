/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.repository.SnapshotRecord;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.tasks.Task;
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
 * A take with {@code serverless.snapshots.take.capture_global_state} and {@code include_global_state: true} embeds the
 * persistent settings, templates, pipelines and stored scripts in the record; a restore with {@code include_global_state: true}
 * puts them back by name. Both need the action {@code cluster:admin/global_state/snapshot}.
 */
public class ServerlessSnapshotGlobalStateTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";
    private static final String GLOBAL_STATE_ACTION = "cluster:admin/global_state/snapshot";

    private static final String INDEX_TEMPLATE = "{\"index_patterns\":[\"x-*\"],\"template\":{\"settings\":{\"number_of_shards\":1}}}";
    private static final String COMPONENT_TEMPLATE = "{\"template\":{\"settings\":{\"number_of_shards\":1}}}";
    private static final String INGEST_PIPELINE = "{\"description\":\"d\",\"processors\":[]}";
    private static final String SEARCH_PIPELINE = "{\"description\":\"d\",\"request_processors\":[]}";
    private static final String STORED_SCRIPT = "{\"script\":{\"lang\":\"mustache\",\"source\":\"{{a}}\"}}";

    /** Denies one action by name, the way a {@code manage_snapshots} caller is refused an action outside its prefix. */
    public static final class DenyActionPlugin extends Plugin implements ActionPlugin {
        private final String denied;

        public DenyActionPlugin(String denied) {
            this.denied = denied;
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionRequestMetadata<Request, Response> metadata,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (denied.equals(action)) {
                        listener.onFailure(new OpenSearchSecurityException("no permission for [" + action + "]", RestStatus.FORBIDDEN));
                    } else {
                        chain.proceed(task, action, request, listener);
                    }
                }
            });
        }
    }

    private interface Body {
        void run(ServerlessNode node, MetadataPlane plane) throws Exception;
    }

    private CountingBlobStore counting;

    private void with(boolean capture, String deniedAction, Body body) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        counting = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(counting, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("one", "uuid-one-0000000000", 1, MAPPING, null));
        final Settings.Builder settings = Settings.builder()
            .put("node.name", "gs-" + randomAlphaOfLength(5).toLowerCase(java.util.Locale.ROOT))
            .put("cluster.name", "serverless-snapshot-global-state")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest");
        if (capture) {
            settings.put("serverless.snapshots.take.capture_global_state", true);
        }
        final List<Plugin> plugins = deniedAction == null ? List.of() : List.of(new DenyActionPlugin(deniedAction));
        try (ServerlessNode node = new ServerlessNode(settings.build(), plugins)) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("one", 0);
            loop.tick(clock.get());
            assertEquals(201, send(node, "PUT", "/one/_doc/1?refresh=true", "{\"msg\":\"doc\"}").status());
            loop.tick(clock.get());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());
            body.run(node, plane);
        }
    }

    private static void seed(ServerlessNode node) throws Exception {
        assertEquals(200, send(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"gs.a\":\"1\",\"gs.b\":\"two\"}}").status());
        assertEquals(200, send(node, "PUT", "/_index_template/t", INDEX_TEMPLATE).status());
        assertEquals(200, send(node, "PUT", "/_component_template/c", COMPONENT_TEMPLATE).status());
        assertEquals(200, send(node, "PUT", "/_ingest/pipeline/p", INGEST_PIPELINE).status());
        assertEquals(200, send(node, "PUT", "/_search/pipeline/s", SEARCH_PIPELINE).status());
        assertEquals(200, send(node, "PUT", "/_scripts/sc", STORED_SCRIPT).status());
    }

    private static Response take(ServerlessNode node, String name, String flag) throws Exception {
        final String extra = flag == null ? "" : ",\"include_global_state\":" + flag;
        return send(node, "PUT", "/_snapshot/backups/" + name + "?wait_for_completion=true", "{\"indices\":\"one\"" + extra + "}");
    }

    private static Response restore(ServerlessNode node, String name, String extra) throws Exception {
        return send(
            node,
            "POST",
            "/_snapshot/backups/" + name + "/_restore?wait_for_completion=true",
            "{\"rename_pattern\":\"one\",\"rename_replacement\":\"copy\"" + (extra == null ? "" : "," + extra) + "}"
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> state(MetadataPlane plane, String name) throws Exception {
        return (Map<String, Object>) plane.snapshot("backups", name).orElseThrow().globalState();
    }

    public void testWithTheSettingOffATakeThatAsksForGlobalStateCapturesNothingAndSaysSo() throws Exception {
        with(false, null, (node, plane) -> {
            seed(node);
            final Response taken = take(node, "s", "true");
            assertEquals(taken.body(), 200, taken.status());
            assertTrue(taken.body(), taken.body().contains("\"include_global_state\":false"));
            assertNull(plane.snapshot("backups", "s").orElseThrow().globalState());
        });
    }

    public void testWithTheSettingOnATakeCapturesOnlyWhenItIsAsked() throws Exception {
        with(true, null, (node, plane) -> {
            seed(node);
            for (String flag : new String[] { null, "false" }) {
                final String name = "s-" + flag;
                final Response taken = take(node, name, flag);
                assertEquals(taken.body(), 200, taken.status());
                assertTrue(taken.body(), taken.body().contains("\"include_global_state\":false"));
                assertNull(plane.snapshot("backups", name).orElseThrow().globalState());
            }
        });
    }

    public void testATakeEmbedsEveryFamilyInTheRecord() throws Exception {
        with(true, null, (node, plane) -> {
            seed(node);
            final Response taken = take(node, "s", "true");
            assertEquals(taken.body(), 200, taken.status());
            assertTrue(taken.body(), taken.body().contains("\"include_global_state\":true"));
            final Map<String, Object> state = state(plane, "s");
            assertEquals(Map.of("gs.a", "1", "gs.b", "two"), state.get("persistent_settings"));
            for (String family : List.of(
                "index_templates",
                "component_templates",
                "ingest_pipelines",
                "search_pipelines",
                "stored_scripts"
            )) {
                assertTrue(family + " " + state, state.get(family) instanceof Map<?, ?> docs && docs.size() == 1);
            }
            assertEquals(List.of("x-*"), ((Map<?, ?>) ((Map<?, ?>) state.get("index_templates")).get("t")).get("index_patterns"));
            assertTrue(((Map<?, ?>) ((Map<?, ?>) state.get("stored_scripts")).get("sc")).get("script") instanceof Map<?, ?>);
            // GET says the same.
            assertTrue(send(node, "GET", "/_snapshot/backups/s", null).body().contains("\"include_global_state\":true"));
        });
    }

    public void testAnEmptyStateIsEmbeddedAsAnEmptyObject() throws Exception {
        with(true, null, (node, plane) -> {
            assertEquals(200, take(node, "s", "true").status());
            assertEquals(Map.of(), state(plane, "s"));
        });
    }

    public void testARestoreWithTheFlagPutsTheStateBackByNameAndDeletesNothing() throws Exception {
        with(true, null, (node, plane) -> {
            seed(node);
            assertEquals(200, take(node, "s", "true").status());
            // The deployment moves on: settings change, documents go, another appears.
            assertEquals(
                200,
                send(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"gs.a\":\"changed\",\"gs.extra\":\"keep\"}}").status()
            );
            assertEquals(200, send(node, "DELETE", "/_index_template/t", null).status());
            assertEquals(200, send(node, "DELETE", "/_ingest/pipeline/p", null).status());
            assertEquals(200, send(node, "PUT", "/_scripts/other", STORED_SCRIPT).status());

            final Response restored = restore(node, "s", "\"include_global_state\":true");
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(restored.body(), restored.body().contains("\"global_state_restored\":7"));
            assertFalse(restored.body(), restored.body().contains("failed_global_state"));

            final Settings settings = plane.clusterConfig().read().settings();
            assertEquals("the snapshot's value wins", "1", settings.get("gs.a"));
            assertEquals("two", settings.get("gs.b"));
            assertEquals("a key the snapshot lacks stays", "keep", settings.get("gs.extra"));
            assertTrue(plane.indexTemplates().get("t").isPresent());
            assertTrue(plane.pipelines().get("p").isPresent());
            assertTrue(plane.componentTemplates().get("c").isPresent());
            assertTrue(plane.searchPipelines().get("s").isPresent());
            assertTrue(plane.scripts().get("sc").isPresent());
            assertTrue("a document the snapshot lacks stays", plane.scripts().get("other").isPresent());
        });
    }

    public void testARestoreWithoutTheFlagLeavesTheStateAloneAndSaysNothingAboutIt() throws Exception {
        with(true, null, (node, plane) -> {
            seed(node);
            assertEquals(200, take(node, "s", "true").status());
            assertEquals(200, send(node, "DELETE", "/_index_template/t", null).status());
            for (String extra : new String[] { null, "\"include_global_state\":false" }) {
                final Response restored = restore(node, "s", extra);
                assertEquals(restored.body(), 200, restored.status());
                assertFalse(restored.body(), restored.body().contains("global_state_restored"));
                assertTrue(plane.describe("copy").isPresent());
                assertEquals(200, send(node, "DELETE", "/copy", null).status());
            }
            assertTrue(plane.indexTemplates().get("t").isEmpty());
        });
    }

    public void testASnapshotWithoutGlobalStateRestoresZeroWhenAsked() throws Exception {
        with(false, null, (node, plane) -> {
            assertEquals(200, take(node, "s", null).status());
            final Response restored = restore(node, "s", "\"include_global_state\":true");
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(restored.body(), restored.body().contains("\"global_state_restored\":0"));
        });
    }

    public void testAFlagThatIsNotABooleanIsRefused() throws Exception {
        with(true, null, (node, plane) -> {
            assertEquals(200, take(node, "s", null).status());
            assertEquals(400, restore(node, "s", "\"include_global_state\":\"maybe\"").status());
            assertTrue(plane.describe("copy").isEmpty());
        });
    }

    public void testACallerWithoutTheGlobalStateActionIsRefusedBeforeAnyReadOrWrite() throws Exception {
        with(true, GLOBAL_STATE_ACTION, (node, plane) -> {
            seed(node);
            // The ordinary snapshot actions are allowed: this is a manage_snapshots caller.
            assertEquals(200, take(node, "plain", null).status());
            final long beforeRefused = counting.total();
            final Response refusedTake = take(node, "s", "true");
            assertEquals(refusedTake.body(), 403, refusedTake.status());
            assertEquals("a refused take made no blob request", beforeRefused, counting.total());
            assertTrue("nothing was written", plane.snapshot("backups", "s").isEmpty());

            // A snapshot that does carry state is still listable and restorable without it.
            plane.finishSnapshot(plane.createSnapshot(withState(plane, Map.of("persistent_settings", Map.of("gs.z", "9")))));
            assertEquals(200, send(node, "GET", "/_snapshot/backups/stateful", null).status());
            final Response refusedRestore = restore(node, "stateful", "\"include_global_state\":true");
            assertEquals(refusedRestore.body(), 403, refusedRestore.status());
            assertTrue("no index was created", plane.describe("copy").isEmpty());
            assertNull("no setting was applied", plane.clusterConfig().read().settings().get("gs.z"));
            final Response without = restore(node, "stateful", null);
            assertEquals(without.body(), 200, without.status());
            assertTrue(plane.describe("copy").isPresent());
        });
    }

    public void testAStateOverTheCapIsRefusedBeforeAnyShardWork() throws Exception {
        with(true, null, (node, plane) -> {
            final String filler = "x".repeat(900_000);
            for (int i = 0; i < 5; i++) {
                final Response put = send(
                    node,
                    "PUT",
                    "/_index_template/big-" + i,
                    "{\"index_patterns\":[\"big-" + i + "-*\"],\"_meta\":{\"filler\":\"" + filler + "\"}}"
                );
                assertEquals(put.body().substring(0, Math.min(300, put.body().length())), 200, put.status());
            }
            final long before = counting.total();
            final Response refused = take(node, "s", "true");
            assertEquals(refused.body(), 400, refused.status());
            assertTrue(refused.body(), refused.body().contains("snapshot_too_large"));
            assertTrue(refused.body(), refused.body().contains("index_templates"));
            assertTrue(plane.snapshot("backups", "s").isEmpty());
            assertTrue("the shards were not touched", counting.total() - before < 40);
            // Under the cap the same take succeeds.
            for (int i = 2; i < 5; i++) {
                assertEquals(200, send(node, "DELETE", "/_index_template/big-" + i, null).status());
            }
            assertEquals(200, take(node, "s", "true").status());
        });
    }

    /** A shallow record of index "one" at its live commit, carrying the given state: a take's record, written by hand. */
    private static SnapshotRecord withState(MetadataPlane plane, Object globalState) throws Exception {
        final CommitManifest live = plane.segmentPublisher("one", "uuid-one-0000000000", 0).readManifest().orElseThrow();
        return new SnapshotRecord(
            "backups",
            "stateful",
            "uuid-stateful",
            true,
            1_000L,
            2_000L,
            Map.of("one", new SnapshotRecord.SnapshottedIndex("uuid-one-0000000000", 1, MAPPING, Map.of(), Map.of(0, live))),
            globalState
        );
    }

    public void testADocumentThatNoLongerValidatesIsListedAndTheRestFollow() throws Exception {
        with(true, null, (node, plane) -> {
            final Map<String, Object> state = new LinkedHashMap<>();
            state.put("persistent_settings", Map.of("gs.ok", "1"));
            state.put("index_templates", Map.of("bad", Map.of("template", Map.of()), "good", Map.of("index_patterns", List.of("g-*"))));
            plane.finishSnapshot(plane.createSnapshot(withState(plane, state)));
            final Response restored = restore(node, "stateful", "\"include_global_state\":true");
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(restored.body(), restored.body().contains("\"global_state_restored\":2"));
            assertTrue(restored.body(), restored.body().contains("\"failed_global_state\":[\"index_template/bad\"]"));
            assertTrue(plane.describe("copy").isPresent());
            assertTrue(plane.indexTemplates().get("good").isPresent());
            assertTrue(plane.indexTemplates().get("bad").isEmpty());
            assertEquals("1", plane.clusterConfig().read().settings().get("gs.ok"));
        });
    }

    public void testAMalformedStateIsUnreadableOnlyToTheRestoreThatUsesIt() throws Exception {
        with(true, null, (node, plane) -> {
            plane.finishSnapshot(plane.createSnapshot(withState(plane, "not an object")));
            assertEquals(200, send(node, "GET", "/_snapshot/backups/stateful", null).status());
            assertEquals(200, send(node, "GET", "/_snapshot/backups/_all", null).status());
            final Response unreadable = restore(node, "stateful", "\"include_global_state\":true");
            assertEquals(unreadable.body(), 500, unreadable.status());
            assertTrue(unreadable.body(), unreadable.body().contains("snapshot_unreadable"));
            assertTrue("nothing was created", plane.describe("copy").isEmpty());
            assertEquals(200, restore(node, "stateful", null).status());
        });
    }

    public void testAListValuedSettingIsUnreadable() throws Exception {
        with(true, null, (node, plane) -> {
            plane.finishSnapshot(plane.createSnapshot(withState(plane, Map.of("persistent_settings", Map.of("gs.list", List.of("a"))))));
            final Response unreadable = restore(node, "stateful", "\"include_global_state\":true");
            assertEquals(unreadable.body(), 500, unreadable.status());
            assertTrue(unreadable.body(), unreadable.body().contains("snapshot_unreadable"));
        });
    }

    /** One document in each of the five sets and one setting: the six reads of an empty state, plus the five documents. */
    public void testGlobalStateBudgets() throws Exception {
        final long[] take = new long[2];
        for (int mode = 0; mode < 2; mode++) {
            final int slot = mode;
            with(mode == 1, null, (node, plane) -> {
                seed(node);
                final long before = counting.total();
                assertEquals(200, take(node, "s", "true").status());
                take[slot] = counting.total() - before;
            });
        }
        RequestBudgets.check("snapshot.take_global_state.extra_requests", take[1] - take[0], "take with the state captured minus not");

        final long[] restore = new long[2];
        for (int mode = 0; mode < 2; mode++) {
            final int slot = mode;
            final boolean restoring = mode == 1;
            with(true, null, (node, plane) -> {
                seed(node);
                assertEquals(200, take(node, "s", "true").status());
                final long before = counting.total();
                assertEquals(200, restore(node, "s", restoring ? "\"include_global_state\":true" : null).status());
                restore[slot] = counting.total() - before;
            });
        }
        RequestBudgets.check(
            "snapshot.restore_global_state.extra_requests",
            restore[1] - restore[0],
            "restore with the state put back minus not"
        );
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
