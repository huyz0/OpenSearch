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
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.auth.ServerlessAuthPlugin;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Authentication and authorization, together, on a shell with no action layer.
 *
 * <p>Every milestone since M24 has ended with the same sentence: the shell authenticates and does not
 * authorize, because privilege evaluation in OpenSearch is keyed on action names at the
 * {@code ActionFilter} layer and §6.3 left that layer unbuilt. A plugin could learn who was calling and
 * could do nothing about what they then did.
 *
 * <p>It can now, and the demonstration is the one that matters rather than a unit test of a chain: one node
 * runs the shell's authentication plugin and a second plugin holding a single {@code ActionFilter}, the
 * filter reads the authenticated principal out of the thread context, and a reader is refused a write that
 * an administrator is allowed. Nothing in the filter is shell-specific — it is an {@code ActionFilter}
 * receiving an action name and a request, which is all a real privilege evaluator receives.
 *
 * <p><b>The thread-context hop is the part that could quietly not work.</b> The principal is left by the
 * REST wrapper on the thread that read the request, and every operation is dispatched to another pool
 * before the filter runs. If transients did not survive that hop the filter would see no caller and would
 * have to fail open or fail closed for everybody — so the test asserts on <em>who</em> was refused, not
 * merely that something was.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessAuthorizationTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";
    /** The account index's own fields: the shell has no dynamic mapping, so this has to be right. */
    private static final String AUTH_MAPPING = "{\"properties\":{\"user\":{\"type\":\"keyword\"},\"hash\":{\"type\":\"keyword\"}}}";
    private static final String ADMIN = "admin";
    private static final String ADMIN_PASSWORD = "correct-horse-battery-staple";

    /** Every action name the filter saw, so a test can assert on what reached it. */
    private static final CopyOnWriteArrayList<String> SEEN = new CopyOnWriteArrayList<>();
    /** The indices of the last request the filter saw, for the bulk case. */
    private static final AtomicReference<List<String>> LAST_INDICES = new AtomicReference<>(List.of());

    /**
     * A plugin whose whole contribution is one rule: only the administrator may change anything.
     *
     * <p>Deliberately not a role model. It is the smallest thing that is genuinely authorization —
     * a decision keyed on the action name and the caller — and it is written against core's interface with
     * no knowledge that it is running on this shell.
     */
    public static final class ReadOnlyForEveryoneElsePlugin extends Plugin implements ActionPlugin {

        static final AtomicReference<ThreadContext> CONTEXT = new AtomicReference<>();

        @Override
        public java.util.Collection<Object> createComponents(
            org.opensearch.transport.client.Client client,
            org.opensearch.cluster.service.ClusterService clusterService,
            ThreadPool threadPool,
            org.opensearch.watcher.ResourceWatcherService resourceWatcherService,
            org.opensearch.script.ScriptService scriptService,
            org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry,
            org.opensearch.env.Environment environment,
            org.opensearch.env.NodeEnvironment nodeEnvironment,
            org.opensearch.core.common.io.stream.NamedWriteableRegistry namedWriteableRegistry,
            org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver,
            java.util.function.Supplier<org.opensearch.repositories.RepositoriesService> repositoriesServiceSupplier
        ) {
            CONTEXT.set(threadPool.getThreadContext());
            return List.of(new Object());
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
                    SEEN.add(action);
                    // A BulkRequest is a CompositeIndicesRequest, not an IndicesRequest: its indices live
                    // on the operations it carries, which is where a privilege evaluator looks for them.
                    if (request instanceof org.opensearch.action.bulk.BulkRequest bulk) {
                        LAST_INDICES.set(bulk.requests().stream().map(org.opensearch.action.DocWriteRequest::index).distinct().toList());
                    } else if (request instanceof org.opensearch.action.IndicesRequest indices && indices.indices() != null) {
                        LAST_INDICES.set(List.of(indices.indices()));
                    }
                    final ThreadContext context = CONTEXT.get();
                    final String caller = context == null ? null : context.getTransient(ServerlessAuthPlugin.PRINCIPAL);
                    final boolean changesSomething = action.startsWith("indices:data/write") || action.startsWith("indices:admin/create");
                    if (changesSomething && ADMIN.equals(caller) == false) {
                        listener.onFailure(
                            new OpenSearchSecurityException(
                                "[" + action + "] is not permitted for [" + (caller == null ? "an unidentified caller" : caller) + "]",
                                RestStatus.FORBIDDEN
                            )
                        );
                        return;
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }

    private Settings nodeSettings(String name) {
        final MockSecureSettings secure = new MockSecureSettings();
        secure.setString("serverless.auth.bootstrap.password", ADMIN_PASSWORD);
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-authz")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.auth.bootstrap.username", ADMIN)
            .put("serverless.auth.hash.iterations", 10_000)
            .setSecureSettings(secure)
            .build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SEEN.clear();
        LAST_INDICES.set(List.of());
    }

    /** A reader may read and may not write, and the administrator may do both. */
    public void testAFilterRefusesAWriteForOneCallerAndNotAnother() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor(".serverless_auth", "uuid-auth-000000000", 1, AUTH_MAPPING, null));
        final Settings settings = nodeSettings("authz");

        try (
            ServerlessNode node = new ServerlessNode(
                settings,
                List.of(ServerlessAuthPlugin.withClock(settings, System::currentTimeMillis), new ReadOnlyForEveryoneElsePlugin())
            )
        ) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.want(".serverless_auth", 0);
            loop.tick(clock.get());

            final String asAdmin = basic(ADMIN, ADMIN_PASSWORD);
            final Response created = send(node, "PUT", "/_serverless/security/users/reader", asAdmin, "{\"password\":\"read-only\"}");
            assertEquals("the administrator creates an account: " + created.body(), 200, created.status());
            final String asReader = basic("reader", "read-only");

            // The administrator writes.
            final Response byAdmin = send(node, "PUT", "/alpha/_doc/1?refresh=true", asAdmin, "{\"msg\":\"written by admin\"}");
            assertEquals("the administrator must be allowed to write: " + byAdmin.body(), 201, byAdmin.status());

            // The reader reads what the administrator wrote.
            final Response read = send(node, "POST", "/alpha/_search", asReader, "{\"query\":{\"match_all\":{}}}");
            assertEquals("a reader must be allowed to read: " + read.body(), 200, read.status());
            assertTrue(read.body().contains("written by admin"));

            // And is refused the write, with the plugin's own status and the plugin's own words.
            final Response byReader = send(node, "PUT", "/alpha/_doc/2?refresh=true", asReader, "{\"msg\":\"written by reader\"}");
            assertEquals("a reader must not be allowed to write: " + byReader.body(), 403, byReader.status());
            assertTrue(
                "the plugin's reason must reach the caller: " + byReader.body(),
                byReader.body().contains("not permitted for [reader]")
            );

            // The refusal must have prevented the write, not merely reported on it.
            final Response after = send(node, "POST", "/alpha/_search", asAdmin, "{\"query\":{\"match\":{\"msg\":\"reader\"}}}");
            assertTrue("a refused write must not have happened: " + after.body(), after.body().contains("\"value\":0"));

            assertTrue("the filter must have seen the write action by name: " + SEEN, SEEN.contains("indices:data/write/index"));
            assertTrue("and the read: " + SEEN, SEEN.contains("indices:data/read/search"));
        }
    }

    /**
     * A plugin whose filter redacts a field from every hit a search returns.
     *
     * <p>The smallest thing that is genuinely field-level security: it wraps the listener, waits for the
     * response, and hands back a different one. Written against core's interface with no knowledge of this
     * shell — the same filter would run on a classic node.
     */
    public static final class RedactingPlugin extends Plugin implements ActionPlugin {

        /** The field it removes from every document it lets through. */
        public static final String SECRET = "secret";

        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return 10;
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
                    if (action.equals(org.opensearch.action.get.GetAction.NAME)) {
                        chain.proceed(task, action, request, ActionListener.wrap(response -> {
                            final var answered = (org.opensearch.action.get.GetResponse) response;
                            if (answered.isExists() == false) {
                                listener.onResponse(response);
                                return;
                            }
                            final var source = new java.util.LinkedHashMap<>(answered.getSourceAsMap());
                            source.remove(SECRET);
                            @SuppressWarnings("unchecked")
                            final Response rewritten = (Response) new org.opensearch.action.get.GetResponse(
                                new org.opensearch.index.get.GetResult(
                                    answered.getIndex(),
                                    answered.getId(),
                                    answered.getSeqNo(),
                                    answered.getPrimaryTerm(),
                                    answered.getVersion(),
                                    true,
                                    org.opensearch.core.common.bytes.BytesReference.bytes(
                                        org.opensearch.common.xcontent.XContentFactory.jsonBuilder().map(source)
                                    ),
                                    java.util.Map.of(),
                                    java.util.Map.of()
                                )
                            );
                            listener.onResponse(rewritten);
                        }, listener::onFailure));
                        return;
                    }
                    if (action.equals(org.opensearch.action.search.SearchAction.NAME) == false) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    chain.proceed(task, action, request, ActionListener.wrap(response -> {
                        final var answered = (org.opensearch.action.search.SearchResponse) response;
                        // Document level: a hit the caller may not see is removed entirely, and the total
                        // is reduced with it. Field level: what survives has the field taken out. The two
                        // are the same mechanism and are done together here because a security plugin does
                        // them together.
                        final var kept = new java.util.ArrayList<org.opensearch.search.SearchHit>();
                        for (org.opensearch.search.SearchHit hit : answered.getHits().getHits()) {
                            if (String.valueOf(hit.getSourceAsMap().get("msg")).contains("classified")) {
                                continue;
                            }
                            kept.add(withoutTheSecret(hit));
                        }
                        final long dropped = answered.getHits().getHits().length - kept.size();
                        final var redacted = kept.toArray(new org.opensearch.search.SearchHit[0]);
                        final var hits = new org.opensearch.search.SearchHits(
                            redacted,
                            new org.apache.lucene.search.TotalHits(
                                answered.getHits().getTotalHits().value() - dropped,
                                answered.getHits().getTotalHits().relation()
                            ),
                            answered.getHits().getMaxScore()
                        );
                        final var internal = new org.opensearch.search.internal.InternalSearchResponse(
                            hits,
                            (org.opensearch.search.aggregations.InternalAggregations) answered.getAggregations(),
                            null,
                            null,
                            false,
                            null,
                            1
                        );
                        @SuppressWarnings("unchecked")
                        // Deliberately nonsense coverage. A filter has no idea how many shards answered and
                        // no reason to care, and one that builds a response without thinking about it --
                        // which is the normal case -- must not be able to turn a complete answer into a
                        // partial-looking one. The shell takes coverage from its own fan-out; these numbers
                        // exist so that a test would notice if it stopped.
                        final Response rewritten = (Response) new org.opensearch.action.search.SearchResponse(
                            internal,
                            null,
                            99,
                            0,
                            0,
                            0L,
                            org.opensearch.action.search.ShardSearchFailure.EMPTY_ARRAY,
                            org.opensearch.action.search.SearchResponse.Clusters.EMPTY
                        );
                        listener.onResponse(rewritten);
                    }, listener::onFailure));
                }
            });
        }

        private static org.opensearch.search.SearchHit withoutTheSecret(org.opensearch.search.SearchHit hit) throws java.io.IOException {
            final var source = new java.util.LinkedHashMap<>(hit.getSourceAsMap());
            source.remove(SECRET);
            final var copy = new org.opensearch.search.SearchHit(hit.docId(), hit.getId(), hit.getNestedIdentity(), null, null);
            copy.sourceRef(
                org.opensearch.core.common.bytes.BytesReference.bytes(
                    org.opensearch.common.xcontent.XContentFactory.jsonBuilder().map(source)
                )
            );
            copy.score(hit.getScore());
            // The shard target is what carries the index name onto a hit, so setting it is how the copy
            // keeps saying which index it came from.
            copy.shard(hit.getShard());
            return copy;
        }
    }

    /**
     * A filter can rewrite a search response, which is what field-level security is made of.
     *
     * <p><b>This was impossible until the response reached the filter at all.</b> Every operation completed
     * its filter chain with a token saying the work had been done, so a plugin could refuse a search and
     * could not change one — no redaction, no document-level filtering, nothing that depends on seeing what
     * came back. The gate now shows a search its real {@code SearchResponse} and takes back whatever the
     * chain produced.
     *
     * <p>The test asserts on the document rather than on the mechanism: the field is in the index, and it
     * is not in the answer. It also covers the document-level case, which is the same mechanism used to
     * drop a hit rather than to trim one — and which has a question of its own, because a total that did
     * not come down with the dropped hit would tell the caller there is something they are not being
     * shown.
     */
    public void testAFilterCanRedactAFieldFromASearchResponse() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final String mapping = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"secret\":{\"type\":\"keyword\"}}}";
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, mapping, null));
        plane.createIndex(new IndexDescriptor(".serverless_auth", "uuid-auth-000000000", 1, AUTH_MAPPING, null));
        final Settings settings = nodeSettings("authz-redact");

        try (
            ServerlessNode node = new ServerlessNode(
                settings,
                List.of(ServerlessAuthPlugin.withClock(settings, System::currentTimeMillis), new RedactingPlugin())
            )
        ) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.want(".serverless_auth", 0);
            loop.tick(clock.get());

            final String asAdmin = basic(ADMIN, ADMIN_PASSWORD);
            final Response written = send(
                node,
                "PUT",
                "/alpha/_doc/1?refresh=true",
                asAdmin,
                "{\"msg\":\"visible\",\"secret\":\"do-not-show-this\"}"
            );
            assertEquals(written.body(), 201, written.status());

            final Response searched = send(node, "POST", "/alpha/_search", asAdmin, "{\"query\":{\"match_all\":{}}}");
            assertEquals(searched.body(), 200, searched.status());
            assertTrue("the document must still be found: " + searched.body(), searched.body().contains("visible"));
            assertFalse("the redacted field must not reach the caller: " + searched.body(), searched.body().contains("do-not-show-this"));
            assertTrue("and the hit must still be counted: " + searched.body(), searched.body().contains("\"value\":1"));

            // And a get is redacted too, which is the difference between a redaction and a decoration: a
            // field hidden from search and readable at /alpha/_doc/1 is one URL away from not being hidden.
            final Response got = send(node, "GET", "/alpha/_doc/1", asAdmin, null);
            assertEquals(got.body(), 200, got.status());
            assertTrue("the document must still be returned: " + got.body(), got.body().contains("visible"));
            assertFalse("a get must be redacted as well: " + got.body(), got.body().contains("do-not-show-this"));

            // And _mget, which routes through the same operation -- asserted rather than assumed, because
            // "it goes through the same code" is exactly the belief that is worth one line to check.
            final Response many = send(node, "POST", "/alpha/_mget", asAdmin, "{\"ids\":[\"1\"]}");
            assertEquals(many.body(), 200, many.status());
            assertTrue("the document must still be there: " + many.body(), many.body().contains("visible"));
            assertFalse("_mget must be redacted too: " + many.body(), many.body().contains("do-not-show-this"));

            // Document level: a hit the filter drops is gone from the answer, and the count goes with it.
            // A total that still said two would be a caller told there is something they cannot see, which
            // is the thing document-level security exists to avoid.
            final Response classified = send(
                node,
                "PUT",
                "/alpha/_doc/2?refresh=true",
                asAdmin,
                "{\"msg\":\"classified\",\"secret\":\"also-hidden\"}"
            );
            assertEquals(classified.body(), 201, classified.status());

            final Response filtered = send(node, "POST", "/alpha/_search", asAdmin, "{\"query\":{\"match_all\":{}}}");
            assertEquals(filtered.body(), 200, filtered.status());
            assertFalse("the dropped document must not appear: " + filtered.body(), filtered.body().contains("classified"));
            assertTrue("and the count must drop with it: " + filtered.body(), filtered.body().contains("\"value\":1"));
            assertTrue("while the other document survives: " + filtered.body(), filtered.body().contains("visible"));

            // Coverage is not the filter's to change: it dropped a hit, not a shard.
            assertTrue("the answer must still report itself complete: " + filtered.body(), filtered.body().contains("\"complete\":true"));
            assertTrue(
                "and the shard counts must be the shell's, not the filter's: " + filtered.body(),
                filtered.body().contains("\"total\":1") && filtered.body().contains("\"successful\":1")
            );
        }
    }

    /** A bulk arrives at the filter as a bulk, naming the indices it is about to write. */
    public void testABulkReachesTheFilterUnderTheBulkActionWithItsIndices() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        final Settings settings = nodeSettings("authz-bulk");

        try (
            ServerlessNode node = new ServerlessNode(
                settings,
                List.of(ServerlessAuthPlugin.withClock(settings, System::currentTimeMillis), new ReadOnlyForEveryoneElsePlugin())
            )
        ) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final String bulk = "{\"index\":{\"_index\":\"alpha\",\"_id\":\"b1\"}}\n{\"msg\":\"one\"}\n"
                + "{\"index\":{\"_index\":\"alpha\",\"_id\":\"b2\"}}\n{\"msg\":\"two\"}\n";
            final Response accepted = send(node, "POST", "/_bulk?refresh=true", basic(ADMIN, ADMIN_PASSWORD), bulk);
            assertEquals("the administrator's bulk must be accepted: " + accepted.body(), 200, accepted.status());

            assertTrue(
                "a bulk must reach the filter under the bulk action, not as loose documents: " + SEEN,
                SEEN.contains("indices:data/write/bulk")
            );
            assertFalse("and not also as single writes", SEEN.contains("indices:data/write/index"));
            assertEquals("and must carry the operations it is about to perform, naming their index", List.of("alpha"), LAST_INDICES.get());
        }
    }

    /** Creating an index is an action a filter can refuse, and refusing it leaves no index behind. */
    public void testCreatingAnIndexIsFiltered() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor(".serverless_auth", "uuid-auth-000000000", 1, AUTH_MAPPING, null));
        final Settings settings = nodeSettings("authz-create");

        try (
            ServerlessNode node = new ServerlessNode(
                settings,
                List.of(ServerlessAuthPlugin.withClock(settings, System::currentTimeMillis), new ReadOnlyForEveryoneElsePlugin())
            )
        ) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want(".serverless_auth", 0);
            loop.tick(clock.get());

            final String asAdmin = basic(ADMIN, ADMIN_PASSWORD);
            final Response madeUser = send(node, "PUT", "/_serverless/security/users/nobody", asAdmin, "{\"password\":\"no-rights\"}");
            assertEquals("the administrator creates an account: " + madeUser.body(), 200, madeUser.status());

            final Response refused = send(node, "PUT", "/beta?shards=1", basic("nobody", "no-rights"), MAPPING);
            assertEquals("a reader must not create an index: " + refused.body(), 403, refused.status());
            assertTrue("and nothing must have been created", plane.describe("beta").isEmpty());

            final Response allowed = send(node, "PUT", "/beta?shards=1", asAdmin, MAPPING);
            assertEquals("while the administrator may: " + allowed.body(), 200, allowed.status());
            assertTrue(plane.describe("beta").isPresent());
        }
    }

    /**
     * Every endpoint that reads or changes data reaches a filter, and each is named.
     *
     * <p><b>This is the test that keeps the gate from being forgotten.</b> The gate is not in one place: it
     * is in {@code ShardOperations} for the operations that funnel through it, and in
     * {@code DocumentHandler}, {@code SearchHandler}, {@code BulkHandler} and {@code IndexAdminHandler} for
     * the paths that predate it and carry their own routing and response vocabulary. Five places is five
     * chances to add a sixth handler and not think about authorization — and the failure mode is silent,
     * because an ungated endpoint works perfectly for everybody.
     *
     * <p>Finding that out is not hypothetical. Two of those call sites exist because this table was written
     * and came up short: the REST write path and the REST search path both went round
     * {@code ShardOperations}, so a filter saw a plugin's reads and none of the user's traffic.
     *
     * <p><b>Adding an endpoint means adding a row here.</b> That is the whole contract.
     */
    public void testEveryDataEndpointReachesAFilter() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("doomed", "uuid-doomed-0000000", 1, MAPPING, null));
        final Settings settings = nodeSettings("authz-surface");

        try (
            ServerlessNode node = new ServerlessNode(
                settings,
                List.of(ServerlessAuthPlugin.withClock(settings, System::currentTimeMillis), new ReadOnlyForEveryoneElsePlugin())
            )
        ) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final String asAdmin = basic(ADMIN, ADMIN_PASSWORD);
            send(node, "PUT", "/alpha/_doc/1?refresh=true", asAdmin, "{\"msg\":\"one\"}");
            send(node, "GET", "/alpha/_doc/1", asAdmin, null);
            send(node, "POST", "/alpha/_search", asAdmin, "{\"query\":{\"match_all\":{}}}");
            send(node, "POST", "/alpha/_update/1?refresh=true", asAdmin, "{\"doc\":{\"msg\":\"one-updated\"}}");
            send(node, "DELETE", "/alpha/_doc/1?refresh=true", asAdmin, null);
            send(node, "POST", "/_bulk?refresh=true", asAdmin, "{\"index\":{\"_index\":\"alpha\",\"_id\":\"z\"}}\n{\"msg\":\"z\"}\n");
            send(node, "PUT", "/gamma?shards=1", asAdmin, MAPPING);
            send(node, "GET", "/gamma", asAdmin, null);
            send(node, "DELETE", "/doomed", asAdmin, null);
            send(node, "PUT", "/_cluster/settings", asAdmin, "{\"persistent\":{\"authz-surface-probe\":\"1\"}}");
            send(node, "POST", "/alpha/_delete_by_query", asAdmin, "{\"query\":{\"match_all\":{}}}");
            // Every one of these must reach its filter even though every one of them fails on the way --
            // "alpha" was deleted by the _delete_by_query probe's shard-emptying and nothing here published
            // a fresh commit, so the repository and the index both refuse business-wise. The filter still
            // has to see the action first: index resolution, manifest gathering and the collision/rename
            // checks all run inside the gate now, precisely because they used to run before it and a
            // refused caller would never reach the filter at all -- the defect this sweep exists to catch.
            send(node, "PUT", "/_snapshot/authz-repo", asAdmin, null);
            send(node, "GET", "/_snapshot/authz-repo", asAdmin, null);
            send(node, "PUT", "/_snapshot/authz-repo/snap1", asAdmin, "{\"indices\":\"alpha\"}");
            send(node, "GET", "/_snapshot/authz-repo/snap1", asAdmin, null);
            send(node, "POST", "/_snapshot/authz-repo/snap1/_restore", asAdmin, null);
            send(node, "DELETE", "/_snapshot/authz-repo/snap1", asAdmin, null);
            send(node, "DELETE", "/_snapshot/authz-repo", asAdmin, null);

            for (String action : List.of(
                "indices:data/write/index",
                "indices:data/read/get",
                "indices:data/read/search",
                "indices:data/write/update",
                "indices:data/write/delete",
                "indices:data/write/delete/byquery",
                "indices:data/write/bulk",
                "indices:admin/create",
                "indices:admin/get",
                "indices:admin/delete",
                "cluster:admin/settings/update",
                "cluster:admin/repository/put",
                "cluster:admin/repository/get",
                "cluster:admin/repository/delete",
                "cluster:admin/snapshot/create",
                "cluster:admin/snapshot/get",
                "cluster:admin/snapshot/restore",
                "cluster:admin/snapshot/delete"
            )) {
                assertTrue("no filter ever saw [" + action + "]; the endpoint that performs it is ungated: " + SEEN, SEEN.contains(action));
            }
        }
    }

    /** With no plugin installing a filter, nothing is gated and nothing changes. */
    public void testANodeWithNoFiltersIsUnchanged() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(plainSettings("authz-none"))) {
            node.start();
            node.setMetadataPlane(plane);
            assertTrue("no filters", node.actionGate().isEmpty());
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            assertEquals(201, send(node, "PUT", "/alpha/_doc/1?refresh=true", null, "{\"msg\":\"unguarded\"}").status());
            assertTrue(send(node, "POST", "/alpha/_search", null, "{\"query\":{\"match_all\":{}}}").body().contains("unguarded"));
            assertTrue("and nothing reached a filter", SEEN.isEmpty());
        }
    }

    private Settings plainSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-authz")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static String basic(String user, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String authorization, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json");
            if (authorization != null) {
                builder.header("Authorization", authorization);
            }
            builder.method(
                method,
                body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)
            );
            final HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
