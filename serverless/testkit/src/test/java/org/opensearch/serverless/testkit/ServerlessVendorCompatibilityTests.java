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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What the September 2026 four-way comparison found, each item pinned over HTTP.
 *
 * <p>The audit ({@code api-compatibility-audit-2026-09.md}) went below the route table for the first
 * time and found three kinds of gap: two query parameters that stopped the official Java client and
 * OpenSearch Dashboards from searching at all; requests answered confidently and wrongly (a simulate that
 * persisted, a frozen search that ran live, an alias filter that was dropped with an acknowledgement); and
 * response shapes that differed from core's for no architectural reason. Every test here is one of those
 * items, and each would have failed before the fix.
 */
public class ServerlessVendorCompatibilityTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING =
        "{\"properties\":{\"msg\":{\"type\":\"text\"},\"tag\":{\"type\":\"keyword\"},\"n\":{\"type\":\"integer\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-vendor-compat")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** A plane with one held, written, refreshed index, so a search has something to find. */
    private record Fixture(ServerlessNode node, MetadataPlane plane, AtomicLong clock, BackgroundReconciler loop) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            node.close();
        }
    }

    private Fixture fixture(String name) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        assertEquals(201, send(node, "PUT", "/alpha/_doc/1", "{\"msg\":\"hello world\",\"tag\":\"a\",\"n\":1}").status());
        assertEquals(201, send(node, "PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"hello again\",\"tag\":\"b\",\"n\":2}").status());
        return new Fixture(node, plane, clock, loop);
    }

    /**
     * The official Java client puts {@code typed_keys=true} on every search unconditionally. It was an
     * "unrecognized parameter" 400 here, which meant that client could not run a single search.
     */
    public void testJavaClientTypedKeysSearchWorks() throws Exception {
        try (Fixture f = fixture("typed-keys")) {
            final Response found = send(
                f.node(),
                "POST",
                "/alpha/_search?typed_keys=true",
                "{\"size\":0,\"aggs\":{\"by_tag\":{\"terms\":{\"field\":\"tag\"}}}}"
            );
            assertEquals(found.body(), 200, found.status());
            assertTrue(
                "aggregations must be keyed type#name under typed_keys: " + found.body(),
                found.body().contains("\"sterms#by_tag\"")
            );
            final Response plain = send(
                f.node(),
                "POST",
                "/alpha/_search",
                "{\"size\":0,\"aggs\":{\"by_tag\":{\"terms\":{\"field\":\"tag\"}}}}"
            );
            assertTrue(
                "and plainly without it: " + plain.body(),
                plain.body().contains("\"by_tag\"") && plain.body().contains("sterms#") == false
            );
        }
    }

    /** OpenSearch Dashboards sends these on every server-side search. Each was a 400. */
    public void testDashboardsDefaultSearchParametersAreAccepted() throws Exception {
        try (Fixture f = fixture("dashboards")) {
            final Response found = send(
                f.node(),
                "POST",
                "/alpha/_search?track_total_hits=true&max_concurrent_shard_requests=5&ignore_unavailable=true&timeout=30s&preference=abc&rest_total_hits_as_int=true",
                "{\"query\":{\"match_all\":{}}}"
            );
            assertEquals(found.body(), 200, found.status());
            assertTrue("rest_total_hits_as_int renders total as a number: " + found.body(), found.body().contains("\"total\":2"));
            // Strictness is kept for what nothing knows: a misspelt parameter is still refused.
            assertEquals(400, send(f.node(), "GET", "/alpha/_search?bogus=1", null).status());
        }
    }

    /** {@code track_total_hits} is what the caller set, not forced on after parsing. */
    public void testTrackTotalHitsIsHonoured() throws Exception {
        try (Fixture f = fixture("total-hits")) {
            final Response off = send(f.node(), "POST", "/alpha/_search", "{\"track_total_hits\":false,\"query\":{\"match_all\":{}}}");
            assertEquals(off.body(), 200, off.status());
            assertFalse("hits.total is omitted when counting is off, as core omits it: " + off.body(), off.body().contains("\"total\":{"));
            // A disjunction rather than match_all or a single term: core answers those counts from the
            // reader without collecting, so they are exact whatever the ceiling, and this shell does the
            // same. A two-clause should has to be collected, and stops at the ceiling.
            final Response capped = send(
                f.node(),
                "POST",
                "/alpha/_search",
                "{\"track_total_hits\":1,\"size\":0,\"query\":{\"bool\":{\"should\":[{\"match\":{\"msg\":\"world\"}},{\"match\":{\"msg\":\"again\"}}]}}}"
            );
            assertTrue("a ceiling below the count is a lower bound: " + capped.body(), capped.body().contains("\"relation\":\"gte\""));
            final Response exact = send(f.node(), "POST", "/alpha/_search", "{\"query\":{\"match_all\":{}}}");
            assertTrue(exact.body(), exact.body().contains("\"relation\":\"eq\""));
        }
    }

    /** URL parameters core's search action parses are honoured here through the same parser. */
    public void testCoreSearchUrlParametersAreHonoured() throws Exception {
        try (Fixture f = fixture("url-params")) {
            final Response sorted = send(f.node(), "GET", "/alpha/_search?sort=n:desc&_source=n&size=1&q=hello", null);
            assertEquals(sorted.body(), 200, sorted.status());
            assertTrue(
                "sorted by n descending, one hit, source filtered: " + sorted.body(),
                sorted.body().contains("\"_source\":{\"n\":2}")
            );
            assertTrue("an unscored hit renders _score as null, as core does: " + sorted.body(), sorted.body().contains("\"_score\":null"));
            final Response explained = send(
                f.node(),
                "GET",
                "/alpha/_search?explain=true&version=true&seq_no_primary_term=true&q=hello",
                null
            );
            assertTrue(
                "explain, version and seq_no travel to the shard and back: " + explained.body(),
                explained.body().contains("\"_explanation\"")
                    && explained.body().contains("\"_version\"")
                    && explained.body().contains("\"_seq_no\"")
            );
        }
    }

    /** Highlighting was computed on the shard and dropped by a hand-written renderer. */
    public void testHighlightAndNamedQueriesAreRendered() throws Exception {
        try (Fixture f = fixture("highlight")) {
            final Response lit = send(
                f.node(),
                "POST",
                "/alpha/_search",
                "{\"query\":{\"match\":{\"msg\":{\"query\":\"hello\",\"_name\":\"greeting\"}}},\"highlight\":{\"fields\":{\"msg\":{}}}}"
            );
            assertEquals(lit.body(), 200, lit.status());
            assertTrue("highlight: " + lit.body(), lit.body().contains("\"highlight\":{\"msg\":[\"<em>hello</em>"));
            assertTrue("matched_queries: " + lit.body(), lit.body().contains("\"matched_queries\":[\"greeting\"]"));
        }
    }

    /** Every client library's {@code ping()} is {@code HEAD /}, and the root document is what they parse. */
    public void testRootDocumentAndPing() throws Exception {
        try (Fixture f = fixture("root")) {
            assertEquals(200, send(f.node(), "HEAD", "/", null).status());
            final Response root = send(f.node(), "GET", "/", null);
            assertEquals(200, root.status());
            for (String field : new String[] {
                "\"cluster_uuid\"",
                "\"distribution\":\"opensearch\"",
                "\"number\":",
                "\"build_type\"",
                "\"build_hash\"",
                "\"lucene_version\"",
                "\"minimum_wire_compatibility_version\"",
                "\"minimum_index_compatibility_version\"",
                "\"tagline\":\"The OpenSearch Project: https://opensearch.org/\"" }) {
                assertTrue("root must carry " + field + ": " + root.body(), root.body().contains(field));
            }
        }
    }

    /** The standard delete and list spellings, and core's response shapes. */
    public void testPointInTimeStandardSpellings() throws Exception {
        try (Fixture f = fixture("pit")) {
            f.loop().tick(f.clock().get());   // publish, so there is a commit to freeze
            final Response taken = send(f.node(), "POST", "/alpha/_search/point_in_time?keep_alive=1m", null);
            assertEquals(taken.body(), 200, taken.status());
            assertTrue(
                "core's create shape: " + taken.body(),
                taken.body().contains("\"_shards\":{") && taken.body().contains("\"creation_time\"")
            );
            final String id = field(taken.body(), "pit_id");
            final Response listed = send(f.node(), "GET", "/_search/point_in_time/_all", null);
            assertEquals(listed.body(), 200, listed.status());
            assertTrue(listed.body(), listed.body().contains("\"pits\":[{\"pit_id\""));
            final Response released = send(f.node(), "DELETE", "/_search/point_in_time", "{\"pit_id\":[\"" + id + "\"]}");
            assertEquals(released.body(), 200, released.status());
            assertTrue("core's delete shape: " + released.body(), released.body().contains("\"pits\":[{\"successful\":true,\"pit_id\":"));
            assertEquals(
                "released twice is not found",
                404,
                send(f.node(), "DELETE", "/_search/point_in_time", "{\"pit_id\":[\"" + id + "\"]}").status()
            );

            final String second = field(send(f.node(), "POST", "/alpha/_search/point_in_time?keep_alive=1m", null).body(), "pit_id");
            final Response all = send(f.node(), "DELETE", "/_search/point_in_time/_all", null);
            assertEquals(all.body(), 200, all.status());
            assertTrue("_all names what it released: " + all.body(), all.body().contains(second.replace("%3D", "=")));
            assertEquals(
                "and the view is gone",
                404,
                send(f.node(), "POST", "/alpha/_search?pit=" + second, "{\"query\":{\"match_all\":{}}}").status()
            );

            assertEquals(
                "a keep-alive over the maximum is refused, not clamped",
                400,
                send(f.node(), "POST", "/alpha/_search/point_in_time?keep_alive=2h", null).status()
            );
        }
    }

    /** A body {@code pit} block used to parse and be ignored, so the search ran live and looked frozen. */
    public void testBodyPointInTimeBlockIsHonoured() throws Exception {
        try (Fixture f = fixture("pit-body")) {
            f.loop().tick(f.clock().get());
            final String id = field(send(f.node(), "POST", "/alpha/_search/point_in_time?keep_alive=1m", null).body(), "pit_id");
            final Response frozen = send(
                f.node(),
                "POST",
                "/_search",
                "{\"pit\":{\"id\":\"" + id + "\",\"keep_alive\":\"1m\"},\"query\":{\"match_all\":{}}}"
            );
            assertEquals(frozen.body(), 200, frozen.status());
            assertTrue("pit_id is echoed, as core echoes it: " + frozen.body(), frozen.body().contains("\"pit_id\":\"" + id + "\""));
            final Response bogus = send(f.node(), "POST", "/_search", "{\"pit\":{\"id\":\"no-such-view\"},\"query\":{\"match_all\":{}}}");
            assertEquals("a view that is not there is not silently a live search: " + bogus.body(), 404, bogus.status());
        }
    }

    /** {@code refresh=wait_for} is what every client library offers; it was a boolean parse failure. */
    public void testRefreshWaitForIsHonoured() throws Exception {
        try (Fixture f = fixture("wait-for")) {
            assertEquals(
                201,
                send(f.node(), "PUT", "/alpha/_doc/3?refresh=wait_for", "{\"msg\":\"third\",\"tag\":\"c\",\"n\":3}").status()
            );
            assertTrue(send(f.node(), "GET", "/alpha/_search?q=msg:third", null).body().contains("\"_id\":\"3\""));
            final Response bulk = send(
                f.node(),
                "POST",
                "/_bulk?refresh=wait_for",
                "{\"index\":{\"_index\":\"alpha\",\"_id\":\"4\"}}\n{\"msg\":\"fourth\"}\n"
            );
            assertEquals(bulk.body(), 200, bulk.status());
            assertTrue(send(f.node(), "GET", "/alpha/_search?q=msg:fourth", null).body().contains("\"_id\":\"4\""));
        }
    }

    /** {@code op_type: create} on a bulk action line was read and dropped, so a create was an overwrite. */
    public void testBulkActionLineFieldsAreHonouredOrRefused() throws Exception {
        try (Fixture f = fixture("bulk-lines")) {
            final String create = "{\"index\":{\"_index\":\"alpha\",\"_id\":\"9\",\"op_type\":\"create\"}}\n{\"msg\":\"nine\"}\n";
            assertTrue(send(f.node(), "POST", "/_bulk", create).body().contains("\"status\":201"));
            final Response again = send(f.node(), "POST", "/_bulk", create);
            assertTrue("the second create must conflict, not overwrite: " + again.body(), again.body().contains("\"status\":409"));
            final Response routed = send(
                f.node(),
                "POST",
                "/_bulk",
                "{\"index\":{\"_index\":\"alpha\",\"_id\":\"10\",\"routing\":\"r\"}}\n{\"msg\":\"ten\"}\n"
            );
            assertTrue("routing is refused per item, not dropped: " + routed.body(), routed.body().contains("\"status\":501"));
            assertEquals("a document that was not written is not found", 404, send(f.node(), "GET", "/alpha/_doc/10", null).status());
        }
    }

    /** A bulk line's {@code pipeline} runs, as a single document's does. */
    public void testBulkPipelineRuns() throws Exception {
        try (Fixture f = fixture("bulk-pipeline")) {
            assertEquals(
                200,
                send(f.node(), "PUT", "/_ingest/pipeline/tagger", "{\"processors\":[{\"set\":{\"field\":\"tag\",\"value\":\"seen\"}}]}")
                    .status()
            );
            final Response bulk = send(
                f.node(),
                "POST",
                "/_bulk?refresh=true",
                "{\"index\":{\"_index\":\"alpha\",\"_id\":\"20\",\"pipeline\":\"tagger\"}}\n{\"msg\":\"twenty\"}\n"
                    + "{\"index\":{\"_index\":\"alpha\",\"_id\":\"21\"}}\n{\"msg\":\"twenty-one\"}\n"
            );
            assertEquals(bulk.body(), 200, bulk.status());
            assertTrue(send(f.node(), "GET", "/alpha/_doc/20", null).body().contains("\"tag\":\"seen\""));
            assertFalse(send(f.node(), "GET", "/alpha/_doc/21", null).body().contains("\"tag\":\"seen\""));
            final Response dropped = send(
                f.node(),
                "POST",
                "/_bulk?pipeline=dropper",
                "{\"index\":{\"_index\":\"alpha\",\"_id\":\"22\"}}\n{\"msg\":\"x\"}\n"
            );
            assertTrue("a missing pipeline is that item's failure: " + dropped.body(), dropped.body().contains("pipeline_missing"));
        }
    }

    /** Alias options were dropped with an acknowledgement on all three spellings. */
    public void testAliasOptionsAreRefusedNotDropped() throws Exception {
        try (Fixture f = fixture("alias-options")) {
            final Response filtered = send(f.node(), "PUT", "/alpha/_alias/filtered", "{\"filter\":{\"term\":{\"tag\":\"a\"}}}");
            assertEquals(filtered.body(), 501, filtered.status());
            assertEquals("and no alias was created", 404, send(f.node(), "GET", "/_alias/filtered", null).status());
            final Response writeIndex = send(
                f.node(),
                "POST",
                "/_aliases",
                "{\"actions\":[{\"add\":{\"index\":\"alpha\",\"alias\":\"w\",\"is_write_index\":true}}]}"
            );
            assertEquals(writeIndex.body(), 501, writeIndex.status());
            // The array spellings core accepts used to resolve to the literal string "null".
            final Response arrays = send(
                f.node(),
                "POST",
                "/_aliases",
                "{\"actions\":[{\"add\":{\"indices\":[\"alpha\"],\"aliases\":[\"arr\"]}}]}"
            );
            assertEquals(arrays.body(), 200, arrays.status());
            assertTrue(send(f.node(), "GET", "/_alias/arr", null).body().contains("alpha"));
            final Response mustExist = send(
                f.node(),
                "POST",
                "/_aliases",
                "{\"actions\":[{\"remove\":{\"index\":\"alpha\",\"alias\":\"never\",\"must_exist\":true}}]}"
            );
            assertEquals(mustExist.body(), 404, mustExist.status());
        }
    }

    /** {@code POST /_index_template/_simulate} used to store a live template named {@code _simulate}. */
    public void testSimulateTemplateDoesNotPersist() throws Exception {
        try (Fixture f = fixture("simulate")) {
            final Response simulated = send(f.node(), "POST", "/_index_template/_simulate", "{\"index_patterns\":[\"x*\"]}");
            assertEquals(simulated.body(), 501, simulated.status());
            final Response stored = send(f.node(), "GET", "/_index_template", null);
            assertFalse("nothing was stored: " + stored.body(), stored.body().contains("_simulate"));
            final Response aliased = send(
                f.node(),
                "PUT",
                "/_index_template/t1",
                "{\"index_patterns\":[\"x*\"],\"template\":{\"aliases\":{\"a\":{}}}}"
            );
            assertEquals("template.aliases is refused rather than stored and never applied: " + aliased.body(), 501, aliased.status());
            assertEquals(200, send(f.node(), "PUT", "/_index_template/t2", "{\"index_patterns\":[\"y*\"]}").status());
            assertEquals(
                "create=true refuses to overwrite",
                400,
                send(f.node(), "PUT", "/_index_template/t2?create=true", "{\"index_patterns\":[\"y*\"]}").status()
            );
        }
    }

    /** A restore accepted 18 fields and acted on 3; the other 15 are refused with a reason each. */
    public void testRestoreRefusesWhatItDoesNotRead() throws Exception {
        try (Fixture f = fixture("restore")) {
            final Response settings = send(
                f.node(),
                "POST",
                "/_snapshot/r/s/_restore",
                "{\"indices\":\"alpha\",\"index_settings\":{\"index.refresh_interval\":\"1s\"}}"
            );
            assertEquals(settings.body(), 501, settings.status());
            assertTrue(settings.body(), settings.body().contains("index_settings is not supported"));
            assertEquals(501, send(f.node(), "POST", "/_snapshot/r/s/_restore", "{\"indices\":\"alpha\",\"partial\":true}").status());
            final Response unknownOnCreate = send(f.node(), "PUT", "/_snapshot/r/s", "{\"indices\":\"alpha\",\"bogus\":1}");
            assertEquals(
                "an unknown key on create is refused as it is on restore: " + unknownOnCreate.body(),
                400,
                unknownOnCreate.status()
            );
        }
    }

    /** Core's {@code BulkByScrollResponse} shape rather than a two-field object. */
    public void testDeleteByQueryHasCoreShape() throws Exception {
        try (Fixture f = fixture("dbq")) {
            f.loop().tick(f.clock().get());
            final Response deleted = send(
                f.node(),
                "POST",
                "/alpha/_delete_by_query?conflicts=proceed&refresh=wait_for",
                "{\"query\":{\"term\":{\"tag\":\"a\"}}}"
            );
            assertEquals(deleted.body(), 200, deleted.status());
            for (String field : new String[] {
                "\"took\":",
                "\"timed_out\":false",
                "\"total\":1",
                "\"deleted\":1",
                "\"version_conflicts\":0",
                "\"failures\":[]",
                "\"retries\":{" }) {
                assertTrue("delete_by_query must carry " + field + ": " + deleted.body(), deleted.body().contains(field));
            }
            assertEquals(
                "wait_for_completion=false asks for a task there is not",
                501,
                send(f.node(), "POST", "/alpha/_delete_by_query?wait_for_completion=false", "{\"query\":{\"match_all\":{}}}").status()
            );
        }
    }

    /** {@code took} first, each item carrying its own {@code status}, refusals per line, and no silent drops. */
    public void testMultiSearchHasCoreShapeAndRefusals() throws Exception {
        try (Fixture f = fixture("msearch")) {
            final Response batch = send(
                f.node(),
                "POST",
                "/_msearch?typed_keys=true",
                "{\"index\":\"alpha\"}\n{\"query\":{\"match_all\":{}}}\n"
                    + "{\"indices\":[\"alpha\"],\"ignore_unavailable\":true}\n{\"query\":{\"match_all\":{}},\"collapse\":{\"field\":\"tag\"}}\n"
                    + "{\"index\":\"ghost\"}\n{\"query\":{\"match_all\":{}}}\n"
            );
            assertEquals(batch.body(), 200, batch.status());
            assertTrue("took first: " + batch.body(), batch.body().startsWith("{\"took\":"));
            assertTrue("a served item carries status 200: " + batch.body(), batch.body().contains("\"status\":200"));
            assertTrue("collapse is refused inside a batch as it is outside one: " + batch.body(), batch.body().contains("\"status\":501"));
            assertTrue(
                "a missing index is that line's 404, not silently dropped: " + batch.body(),
                batch.body().contains("\"status\":404")
            );
        }
    }

    /** The body of a {@code _field_caps} request was ignored entirely. */
    public void testFieldCapsBodyIsRead() throws Exception {
        try (Fixture f = fixture("field-caps")) {
            final Response narrowed = send(f.node(), "POST", "/alpha/_field_caps", "{\"fields\":[\"msg\"]}");
            assertEquals(narrowed.body(), 200, narrowed.status());
            assertTrue(narrowed.body(), narrowed.body().contains("\"msg\""));
            assertFalse("only the field asked for: " + narrowed.body(), narrowed.body().contains("\"tag\""));
        }
    }

    /** Simulate, the misrouted spellings, and the node metric that used to be a 404 about a node. */
    public void testMisroutedPathsAnswerForThemselves() throws Exception {
        try (Fixture f = fixture("misrouted")) {
            final Response simulated = send(
                f.node(),
                "POST",
                "/_ingest/pipeline/_simulate",
                "{\"pipeline\":{\"processors\":[{\"set\":{\"field\":\"x\",\"value\":1}}]},\"docs\":[{\"_source\":{\"a\":1}},{\"_index\":\"alpha\",\"_id\":\"q\",\"_source\":{\"b\":2}}]}"
            );
            assertEquals(simulated.body(), 200, simulated.status());
            assertTrue(
                "each document is shown as the pipeline left it: " + simulated.body(),
                simulated.body().contains("\"x\":1") && simulated.body().contains("\"_id\":\"q\"")
            );
            final Response repo = send(f.node(), "PUT", "/_snapshot/r1", "{\"type\":\"fs\"}");
            assertEquals(repo.body(), 200, repo.status());
            final Response verified = send(f.node(), "POST", "/_snapshot/r1/_verify", null);
            assertEquals(verified.body(), 200, verified.status());
            assertTrue(verified.body(), verified.body().contains("\"nodes\":{"));
            final Response metric = send(f.node(), "GET", "/_nodes/os", null);
            assertEquals("a metric is not a missing node: " + metric.body(), 501, metric.status());
            assertEquals(501, send(f.node(), "GET", "/_nodes/_all/jvm", null).status());
            final Response analyzed = send(f.node(), "POST", "/_analyze", "{\"analyzer\":\"standard\",\"text\":\"Hello World\"}");
            assertEquals("the index-less form uses the node's own analyzers: " + analyzed.body(), 200, analyzed.status());
            assertTrue(analyzed.body(), analyzed.body().contains("\"token\":\"hello\""));
            final Response cat = send(f.node(), "GET", "/_cat/indices/alp*?format=json", null);
            assertEquals("_cat/indices with a prefix is the bounded listing: " + cat.body(), 200, cat.status());
            assertTrue(cat.body(), cat.body().contains("\"index\":\"alpha\""));
        }
    }

    /** Multi-get carries the concurrency token a single get does; the source endpoint's errors are core's. */
    public void testGetShapes() throws Exception {
        try (Fixture f = fixture("get-shapes")) {
            final Response many = send(f.node(), "GET", "/alpha/_mget", "{\"ids\":[\"1\"]}");
            assertTrue(many.body(), many.body().contains("\"_seq_no\":") && many.body().contains("\"_primary_term\":"));
            assertEquals(400, send(f.node(), "GET", "/alpha/_source/1?_source=false", null).status());
            final Response missing = send(f.node(), "GET", "/alpha/_source/ghost", null);
            assertEquals(404, missing.status());
            assertTrue(
                "a missing document is a JSON error, not an empty body: " + missing.body(),
                missing.body().contains("resource_not_found_exception")
            );
            final Response stored = send(f.node(), "GET", "/alpha/_doc/1?stored_fields=msg", null);
            assertEquals("stored_fields is refused, not unrecognized: " + stored.body(), 501, stored.status());
            assertEquals(
                "hints are consumed",
                200,
                send(f.node(), "GET", "/alpha/_doc/1?realtime=true&preference=x&refresh=false", null).status()
            );
        }
    }

    /** A plugin whose one filter lets everything through: the gate then runs, and the answer is read back. */
    public static final class PassThroughPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        @Override
        public java.util.List<org.opensearch.action.support.ActionFilter> getActionFilters() {
            return java.util.List.of(new org.opensearch.action.support.ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <
                    Request extends org.opensearch.action.ActionRequest,
                    Response extends org.opensearch.core.action.ActionResponse> void apply(
                        org.opensearch.tasks.Task task,
                        String action,
                        Request request,
                        org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                        org.opensearch.core.action.ActionListener<Response> listener,
                        org.opensearch.action.support.ActionFilterChain<Request, Response> chain
                    ) {
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }

    /**
     * With any action filter installed, a get went through the gate and came back rebuilt with the
     * unassigned sentinels: {@code _seq_no -2}, {@code _version -1}. Every conditional write built on
     * those tokens then lost. The security plugin installs a filter, so this was the shape of every
     * secured deployment.
     */
    public void testAGatedGetKeepsItsConcurrencyToken() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("gated-get"), java.util.List.of(new PassThroughPlugin()))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            assertEquals(201, send(node, "PUT", "/alpha/_doc/1", "{\"msg\":\"gated\"}").status());
            final Response got = send(node, "GET", "/alpha/_doc/1", null);
            assertEquals(got.body(), 200, got.status());
            assertTrue(
                "the token must be the real one, not the unassigned sentinel: " + got.body(),
                got.body().contains("\"_seq_no\":0") && got.body().contains("\"_version\":1")
            );
            final Response many = send(node, "GET", "/alpha/_mget", "{\"ids\":[\"1\"]}");
            assertTrue(many.body(), many.body().contains("\"_seq_no\":0"));
            // And the token works: a conditional write on it succeeds, and on a stale one is refused.
            assertEquals(200, send(node, "PUT", "/alpha/_doc/1?if_seq_no=0&if_primary_term=1", "{\"msg\":\"second\"}").status());
            assertEquals(409, send(node, "PUT", "/alpha/_doc/1?if_seq_no=0&if_primary_term=1", "{\"msg\":\"third\"}").status());
        }
    }

    private static String field(String body, String name) {
        final Matcher matcher = Pattern.compile("\"" + name + "\":\"([^\"]+)\"").matcher(body);
        assertTrue("no " + name + " in " + body, matcher.find());
        return java.net.URLEncoder.encode(matcher.group(1), StandardCharsets.UTF_8);
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
