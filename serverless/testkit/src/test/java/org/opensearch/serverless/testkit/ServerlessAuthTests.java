/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.auth.CredentialStore;
import org.opensearch.serverless.auth.PasswordHash;
import org.opensearch.serverless.auth.ServerlessAuthPlugin;
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
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * The shell's own authentication, running as a plugin on the shell's own plugin host.
 *
 * <p>M23 proved the host with a test plugin — one whose behaviour was chosen to make the host look good.
 * This is the same host carrying something the deployment genuinely needs: on the critical path of every
 * request, keeping its accounts in an index, reached through the client, and depending on
 * {@code :server} rather than on the shell. If the host cannot carry this, it cannot carry OpenSearch
 * Security either.
 *
 * <p>The tests are written around the questions an operator would actually ask. Can I get in? Can somebody
 * without a credential? What happens on the first day, when no account exists yet? What happens on a bad
 * day, when the object store is unreachable? And when I remove somebody, when do they actually stop
 * working?
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessAuthTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String AUTH_INDEX = ".serverless_auth";
    private static final String AUTH_MAPPING = "{\"properties\":{\"user\":{\"type\":\"keyword\"},\"hash\":{\"type\":\"keyword\"}}}";
    private static final String DOC_MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private static final String ADMIN = "admin";
    private static final String ADMIN_PASSWORD = "correct-horse-battery-staple";

    /**
     * The work factor tests run at.
     *
     * <p>The floor the setting allows, not the default: at the default a derivation is a few hundred
     * milliseconds, and these tests authenticate dozens of times. {@link #testTheDefaultWorkFactorIsUsable}
     * pays the real price once, so the number that ships is exercised rather than assumed.
     */
    private static final int TEST_ITERATIONS = 10_000;

    /** Lets a test move the credential cache's clock without sleeping. */
    private final AtomicLong authClock = new AtomicLong(1_000_000L);

    /** Set by the test so its probe plugin can ask the node who the caller is. */
    private static final AtomicReference<ServerlessNode> PROBE_NODE = new AtomicReference<>();

    /**
     * A second plugin, which asks the identity service who is making the request.
     *
     * <p>It exists to check the thing a real plugin would rely on: not that the wrapper ran, but that the
     * caller's identity reached core's {@code Subject} API, which is the only vocabulary a plugin written
     * for OpenSearch knows.
     */
    public static final class IdentityProbePlugin extends Plugin implements ActionPlugin {
        @Override
        public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster
        ) {
            return List.of(new RestHandler() {
                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.GET, "/_probe/whoami"));
                }

                @Override
                public void handleRequest(
                    RestRequest request,
                    org.opensearch.rest.RestChannel channel,
                    org.opensearch.transport.client.node.NodeClient client
                ) {
                    final String name = PROBE_NODE.get().identityService().getCurrentSubject().getPrincipal().getName();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, name));
                }
            });
        }
    }

    private Settings nodeSettings(String name) {
        // The password comes through a keystore, because that is the only place the plugin will read one
        // from: core refuses a secure setting found in opensearch.yml rather than quietly accepting it.
        final org.opensearch.common.settings.MockSecureSettings secure = new org.opensearch.common.settings.MockSecureSettings();
        secure.setString("serverless.auth.bootstrap.password", ADMIN_PASSWORD);
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-auth")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.auth.bootstrap.username", ADMIN)
            .put("serverless.auth.hash.iterations", TEST_ITERATIONS)
            .put("serverless.auth.cache.ttl", "60s")
            .setSecureSettings(secure)
            .build();
    }

    private MetadataPlane plane(AtomicLong clock) throws java.io.IOException {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private ServerlessAuthPlugin authPlugin(Settings settings) {
        return new ServerlessAuthPlugin(settings, authClock::get);
    }

    /** A request with no credential is refused, and told how to offer one. */
    public void testARequestWithNoCredentialIsRefusedAndChallenged() throws Exception {
        final Settings settings = nodeSettings("auth-anonymous");
        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            final Response denied = send(node, "GET", "/", null, null);
            assertEquals("an unauthenticated request must not be served: " + denied.body(), 401, denied.status());
            assertTrue("and must say what is missing: " + denied.body(), denied.body().contains("no credentials were offered"));
            // Without a challenge a 401 is indistinguishable from a broken server, and no client library
            // knows it should offer a credential.
            assertTrue(
                "and must challenge, so a client knows to authenticate: " + denied.headers(),
                denied.header("WWW-Authenticate") != null && denied.header("WWW-Authenticate").startsWith("Basic ")
            );
        }
    }

    /** The configured account is accepted, and the request reaches the handler behind the seam. */
    public void testTheConfiguredAccountIsServed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Settings settings = nodeSettings("auth-admin");
        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            // A plane, but no account index: the state a deployment is in before anyone creates an
            // account, and the one where "no such user" must not be reported as "store unreachable".
            node.setMetadataPlane(plane(clock));
            final Response ok = send(node, "GET", "/", basic(ADMIN, ADMIN_PASSWORD), null);
            assertEquals("the configured account must be served: " + ok.body(), 200, ok.status());

            final Response wrong = send(node, "GET", "/", basic(ADMIN, "not-the-password"), null);
            assertEquals("and a wrong password must not be: " + wrong.body(), 401, wrong.status());

            final Response unknown = send(node, "GET", "/", basic("nobody", ADMIN_PASSWORD), null);
            // 401 rather than 503: the account store is reachable and simply has no such account. On a
            // fresh deployment the index does not exist at all, which is still "no such account".
            assertEquals("and an account that does not exist must not be: " + unknown.body(), 401, unknown.status());

            final Response bearer = send(node, "GET", "/", "Bearer some-token", null);
            assertEquals(401, bearer.status());
            assertTrue("an unsupported scheme must say so: " + bearer.body(), bearer.body().contains("HTTP Basic"));
        }
    }

    /**
     * The first account: the plugin creates its own index, and says to retry while a node picks the shard
     * up.
     *
     * <p>This is the first thing an operator does, so the exact sequence matters more than it looks. The
     * index has to exist before it can be written to, and a freshly created index in this system has no
     * owner until some node activates its shard. Answering 503 "retry" is the truth; answering 500 would
     * send an operator looking for a fault that is not there.
     */
    public void testTheFirstAccountCreatesTheIndexAndWaitsForItsShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("auth-first");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            assertTrue("no account index exists yet", plane.describe(AUTH_INDEX).isEmpty());

            final Response early = send(
                node,
                "PUT",
                "/_serverless/security/users/rey",
                basic(ADMIN, ADMIN_PASSWORD),
                "{\"password\":\"jedi-pass\"}"
            );
            assertEquals("the account index has no owner yet, so this must be a retry: " + early.body(), 503, early.status());
            assertTrue("and must say to retry: " + early.body(), early.body().contains("retry"));
            assertTrue("but the plugin must have created its index", plane.describe(AUTH_INDEX).isPresent());

            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want(AUTH_INDEX, 0);
            loop.tick(clock.get());

            final Response created = send(
                node,
                "PUT",
                "/_serverless/security/users/rey",
                basic(ADMIN, ADMIN_PASSWORD),
                "{\"password\":\"jedi-pass\"}"
            );
            assertEquals("with the shard held, the account must be created: " + created.body(), 200, created.status());

            final Response asRey = send(node, "GET", "/", basic("rey", "jedi-pass"), null);
            assertEquals("and the account must then authenticate: " + asRey.body(), 200, asRey.status());

            final Response reyWrong = send(node, "GET", "/", basic("rey", "jedi-pass-but-wrong"), null);
            assertEquals("with its own password and no other", 401, reyWrong.status());
        }
    }

    /** An account created through the endpoint can use the shell, not merely log in. */
    public void testAnAccountCreatedThroughTheEndpointCanUseTheShell() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, DOC_MAPPING, null));
        final Settings settings = nodeSettings("auth-usable");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = ready(node, plane, clock, "alpha");

            assertEquals(
                200,
                send(node, "PUT", "/_serverless/security/users/finn", basic(ADMIN, ADMIN_PASSWORD), "{\"password\":\"stormtrooper\"}")
                    .status()
            );

            final Response written = send(
                node,
                "PUT",
                "/alpha/_doc/1?refresh=true",
                basic("finn", "stormtrooper"),
                "{\"msg\":\"authenticated\"}"
            );
            assertEquals("an ordinary account must be able to write: " + written.body(), 201, written.status());

            final Response found = send(node, "GET", "/alpha/_search?q=msg:authenticated", basic("finn", "stormtrooper"), null);
            assertEquals("and to search: " + found.body(), 200, found.status());
            assertTrue("and see what it wrote: " + found.body(), found.body().contains("authenticated"));

            // The unauthenticated version of exactly the same request.
            assertEquals(
                "while the same request with no credential is refused",
                401,
                send(node, "GET", "/alpha/_search?q=msg:authenticated", null, null).status()
            );
            loop.tick(clock.get());
        }
    }

    /** Account management is not self-service. */
    public void testOnlyTheConfiguredAccountMayManageAccounts() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("auth-privilege");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            ready(node, plane, clock);

            assertEquals(
                200,
                send(node, "PUT", "/_serverless/security/users/poe", basic(ADMIN, ADMIN_PASSWORD), "{\"password\":\"black-one\"}").status()
            );

            // The whole point: an ordinary account that could create accounts could give itself anything,
            // which would make authentication decorative.
            final Response escalation = send(
                node,
                "PUT",
                "/_serverless/security/users/poe2",
                basic("poe", "black-one"),
                "{\"password\":\"another\"}"
            );
            assertEquals("an ordinary account must not create accounts: " + escalation.body(), 403, escalation.status());
            assertTrue(
                "and must be told why, not merely refused: " + escalation.body(),
                escalation.body().contains("only the configured account")
            );
            assertEquals(
                "and the account must not exist",
                404,
                send(node, "GET", "/_serverless/security/users/poe2", basic(ADMIN, ADMIN_PASSWORD), null).status()
            );

            assertEquals(
                "nor remove them",
                403,
                send(node, "DELETE", "/_serverless/security/users/poe", basic("poe", "black-one"), null).status()
            );
            assertEquals(
                "and must still be there",
                200,
                send(node, "GET", "/_serverless/security/users/poe", basic(ADMIN, ADMIN_PASSWORD), null).status()
            );
        }
    }

    /**
     * Removing an account stops it working on the node that removed it, immediately.
     *
     * <p>Immediately is the part worth testing. A verified credential is cached, so a removal that only
     * deleted the document would leave the account working here until the entry aged out — on the very
     * node an operator was looking at when they removed it.
     */
    public void testARemovedAccountStopsWorkingAtOnce() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("auth-removal");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            ready(node, plane, clock);

            assertEquals(
                200,
                send(node, "PUT", "/_serverless/security/users/dj", basic(ADMIN, ADMIN_PASSWORD), "{\"password\":\"codebreaker\"}").status()
            );
            assertEquals("the account works", 200, send(node, "GET", "/", basic("dj", "codebreaker"), null).status());

            final Response removed = send(node, "DELETE", "/_serverless/security/users/dj", basic(ADMIN, ADMIN_PASSWORD), null);
            assertEquals("and is removed: " + removed.body(), 200, removed.status());

            // No clock movement: this must not depend on the cache expiring.
            assertEquals("and stops working on this node at once", 401, send(node, "GET", "/", basic("dj", "codebreaker"), null).status());
            assertEquals(
                "removing it again finds nothing",
                404,
                send(node, "DELETE", "/_serverless/security/users/dj", basic(ADMIN, ADMIN_PASSWORD), null).status()
            );
        }
    }

    /**
     * A credential removed behind this node's back keeps working until the cache expires, and then stops.
     *
     * <p>This is the staleness window written down as a test rather than as a sentence in a comment. In a
     * fleet the node that removes an account is not the only node serving requests, and the others go on
     * accepting the removed credential for up to the TTL. Bounded is the property being claimed; a cache
     * that never re-checked would pass every other test in this file.
     */
    public void testACredentialRemovedElsewhereStopsWorkingWhenTheCacheExpires() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("auth-staleness");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            ready(node, plane, clock);

            assertEquals(
                200,
                send(node, "PUT", "/_serverless/security/users/phasma", basic(ADMIN, ADMIN_PASSWORD), "{\"password\":\"chrome\"}").status()
            );
            assertEquals(200, send(node, "GET", "/", basic("phasma", "chrome"), null).status());

            // Deleted straight out of the index, which is what another node's removal looks like from
            // here: the document is gone and nothing told this node about it.
            node.client()
                .delete(
                    new org.opensearch.action.delete.DeleteRequest(AUTH_INDEX, "phasma").setRefreshPolicy(
                        org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE
                    )
                )
                .actionGet();

            assertEquals(
                "within the TTL the cached credential is still honoured",
                200,
                send(node, "GET", "/", basic("phasma", "chrome"), null).status()
            );

            authClock.addAndGet(61_000L);
            assertEquals(
                "past the TTL it must be checked again, and fail",
                401,
                send(node, "GET", "/", basic("phasma", "chrome"), null).status()
            );
        }
    }

    /**
     * The configured account still works when the account store cannot be read, and everyone else gets a
     * 503 rather than a 401.
     *
     * <p>This is the recovery path, and the reason the configured account is checked before the index. A
     * deployment whose only credentials live in the object store loses the ability to log in exactly when
     * something has gone wrong with the object store. The distinction in the answer matters as much: a
     * user told 401 goes and changes a password that was never the problem.
     */
    public void testTheConfiguredAccountSurvivesAnUnreadableStore() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("auth-degraded");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            ready(node, plane, clock);
            assertEquals(
                200,
                send(node, "PUT", "/_serverless/security/users/rose", basic(ADMIN, ADMIN_PASSWORD), "{\"password\":\"tico\"}").status()
            );
            assertEquals(200, send(node, "GET", "/", basic("rose", "tico"), null).status());

            // The store goes away. A node with no metadata plane cannot resolve an index at all, which is
            // the bluntest version of "the accounts are unreachable".
            node.setMetadataPlane(null);
            authClock.addAndGet(61_000L);

            final Response stillIn = send(node, "GET", "/", basic(ADMIN, ADMIN_PASSWORD), null);
            assertEquals("the configured account must still get in: " + stillIn.body(), 200, stillIn.status());

            final Response unavailable = send(node, "GET", "/", basic("rose", "tico"), null);
            assertEquals("and a stored account must get a 503, not a 401: " + unavailable.body(), 503, unavailable.status());
            assertTrue("naming the real problem: " + unavailable.body(), unavailable.body().contains("account store could not be read"));
        }
    }

    /** The authenticated caller reaches a plugin through core's identity API, not only through the wrapper. */
    public void testThePluginIdentityApiReportsTheCaller() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("auth-identity");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings), new IdentityProbePlugin()))) {
            PROBE_NODE.set(node);
            node.start();
            node.setMetadataPlane(plane);
            ready(node, plane, clock);
            assertEquals(
                200,
                send(node, "PUT", "/_serverless/security/users/maz", basic(ADMIN, ADMIN_PASSWORD), "{\"password\":\"kanata\"}").status()
            );

            final Response asAdmin = send(node, "GET", "/_probe/whoami", basic(ADMIN, ADMIN_PASSWORD), null);
            assertEquals(asAdmin.body(), 200, asAdmin.status());
            assertEquals("a plugin must see who is calling", ADMIN, asAdmin.body());

            final Response asMaz = send(node, "GET", "/_probe/whoami", basic("maz", "kanata"), null);
            assertEquals("and see a different caller differently: " + asMaz.body(), "maz", asMaz.body());

            // Off a request there is no caller, and core's answer for that is the one to give.
            assertEquals(NamedPrincipal.UNAUTHENTICATED.getName(), node.identityService().getCurrentSubject().getPrincipal().getName());
        }
    }

    /** A password is never stored, and what is stored cannot be read back out of the endpoint. */
    public void testWhatIsStoredIsNotThePassword() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("auth-storage");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            ready(node, plane, clock);
            assertEquals(
                200,
                send(node, "PUT", "/_serverless/security/users/hux", basic(ADMIN, ADMIN_PASSWORD), "{\"password\":\"starkiller\"}").status()
            );

            final String stored = node.client()
                .get(new org.opensearch.action.get.GetRequest(AUTH_INDEX, "hux"))
                .actionGet()
                .getSourceAsString();
            assertFalse("the password must not be in the stored document: " + stored, stored.contains("starkiller"));
            assertTrue("what is stored must be a salted derivation: " + stored, stored.contains("pbkdf2_sha512$"));

            // And the endpoint that may manage accounts still does not hand the record back.
            final Response looked = send(node, "GET", "/_serverless/security/users/hux", basic(ADMIN, ADMIN_PASSWORD), null);
            assertEquals(200, looked.status());
            assertFalse("the record must not be readable through the API either: " + looked.body(), looked.body().contains("pbkdf2"));
        }
    }

    /**
     * The account index is not reachable through the request path, by anyone, by any route.
     *
     * <p>This is the hole this milestone would otherwise have opened. Every authenticated caller can do
     * everything — there is no authorization layer — so an account index that behaved like an ordinary
     * index would let any account read the password records out of it with a search. Worse, it would let
     * any account <em>write</em> one: a record whose hash derives from a password of the attacker's
     * choosing, stored under the name of somebody else's account, is that account. Documenting that as a
     * limitation would not have been honest; it is privilege escalation, so it is closed.
     *
     * <p>The bulk case is checked separately because it is the one the registration-time guard cannot see:
     * a bulk request names its indices in the body, so the path it arrived on says nothing about what it
     * touches.
     */
    public void testTheAccountIndexIsNotReachableThroughRest() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("auth-system-index");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            ready(node, plane, clock);
            assertEquals(
                200,
                send(node, "PUT", "/_serverless/security/users/kylo", basic(ADMIN, ADMIN_PASSWORD), "{\"password\":\"ben-solo\"}").status()
            );

            final String asKylo = basic("kylo", "ben-solo");
            for (String[] attempt : new String[][] {
                { "GET", "/" + AUTH_INDEX + "/_search?q=*:*" },
                { "GET", "/" + AUTH_INDEX + "/_doc/" + ADMIN },
                { "GET", "/" + AUTH_INDEX },
                { "DELETE", "/" + AUTH_INDEX },
                { "GET", "/_serverless/shards/" + AUTH_INDEX } }) {
                final Response refused = send(node, attempt[0], attempt[1], asKylo, null);
                assertEquals(attempt[0] + " " + attempt[1] + " must be refused: " + refused.body(), 403, refused.status());
                assertTrue("and say why: " + refused.body(), refused.body().contains("belongs to a plugin"));
            }

            // Not even the configured account, because the rule is about the route and not the caller: the
            // shell has no authorization layer with which to make it about the caller.
            assertEquals(403, send(node, "GET", "/" + AUTH_INDEX + "/_search?q=*:*", basic(ADMIN, ADMIN_PASSWORD), null).status());

            // The escalation itself: forge a record for the configured account through _bulk.
            final String forged = PasswordHash.encode("i-chose-this".toCharArray(), TEST_ITERATIONS);
            final String bulk = "{\"index\":{\"_index\":\""
                + AUTH_INDEX
                + "\",\"_id\":\""
                + ADMIN
                + "\"}}\n"
                + "{\"user\":\""
                + ADMIN
                + "\",\"hash\":\""
                + forged
                + "\"}\n";
            final Response bulkAttempt = send(node, "POST", "/_bulk?refresh=true", asKylo, bulk);

            // The consequence first, deliberately. Asserting on the refusal message before this would mean
            // a canary that removed the check failed on the wording and never reached the question that
            // matters -- whether the forged credential works.
            assertEquals("a forged record must not authenticate", 401, send(node, "GET", "/", basic(ADMIN, "i-chose-this"), null).status());
            assertEquals("while the real one still does", 200, send(node, "GET", "/", basic(ADMIN, ADMIN_PASSWORD), null).status());
            assertTrue(
                "and the bulk item must have been refused by name: " + bulkAttempt.body(),
                bulkAttempt.body().contains("system_index")
            );
        }
    }

    /** With no plugin declaring one, nothing is guarded and an ordinary dotted index behaves normally. */
    public void testAnOrdinaryDottedIndexIsNotASystemIndex() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor(".ordinary", "uuid-ordinary-000000", 1, DOC_MAPPING, null));
        final Settings settings = nodeSettings("auth-dotted");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = ready(node, plane, clock, ".ordinary");
            // A leading dot is decoration. What makes an index a system index is a plugin declaring it.
            assertEquals(
                201,
                send(node, "PUT", "/.ordinary/_doc/1?refresh=true", basic(ADMIN, ADMIN_PASSWORD), "{\"msg\":\"still an index\"}").status()
            );
            assertEquals(200, send(node, "GET", "/.ordinary/_doc/1", basic(ADMIN, ADMIN_PASSWORD), null).status());
            loop.tick(clock.get());
        }
    }

    /** Two accounts with the same password get different records, which is what the salt is for. */
    public void testTheSamePasswordStoresDifferently() {
        final String first = PasswordHash.encode("identical".toCharArray(), TEST_ITERATIONS);
        final String second = PasswordHash.encode("identical".toCharArray(), TEST_ITERATIONS);
        assertNotEquals("without a per-record salt, one cracked password cracks every account that shares it", first, second);
        assertTrue(PasswordHash.verify("identical".toCharArray(), first));
        assertTrue(PasswordHash.verify("identical".toCharArray(), second));
        assertFalse(PasswordHash.verify("identica".toCharArray(), first));
        // A record this cannot parse must fail, never pass.
        assertFalse(PasswordHash.verify("identical".toCharArray(), "not-a-record"));
        assertFalse(PasswordHash.verify("identical".toCharArray(), "pbkdf2_sha512$0$AAAA$AAAA"));
        assertFalse(PasswordHash.verify("identical".toCharArray(), null));
    }

    /**
     * The work factor that actually ships is exercised, once.
     *
     * <p>Every other test here runs at the lowest allowed setting so that it finishes. That would leave the
     * shipped number tested by nobody, and a default that is wrong — an algorithm the JVM will not do at
     * that size, say — would be found in production rather than here.
     */
    public void testTheDefaultWorkFactorIsUsable() {
        final String record = PasswordHash.encode("default-factor".toCharArray(), PasswordHash.DEFAULT_ITERATIONS);
        assertTrue(record.startsWith("pbkdf2_sha512$" + PasswordHash.DEFAULT_ITERATIONS + "$"));
        assertTrue(PasswordHash.verify("default-factor".toCharArray(), record));
        assertFalse(PasswordHash.verify("default-facto".toCharArray(), record));
    }

    /** A node configured with authentication and no way to authenticate refuses to exist. */
    public void testTheConfiguredAccountIsMandatory() {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("serverless.auth.hash.iterations", TEST_ITERATIONS)
            .build();
        final var failure = expectThrows(IllegalArgumentException.class, () -> new ServerlessAuthPlugin(settings));
        assertTrue(
            "the refusal must name the setting an operator has to set: " + failure.getMessage(),
            failure.getMessage().contains("serverless.auth.bootstrap.password")
        );
    }

    /** The password is a keystore setting, and putting it in opensearch.yml is an error rather than a downgrade. */
    public void testThePasswordCannotBeConfiguredInTheClear() {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("serverless.auth.bootstrap.password", "written-in-the-yml")
            .build();
        final var failure = expectThrows(IllegalArgumentException.class, () -> new ServerlessAuthPlugin(settings));
        assertTrue(
            "core must refuse a secure setting found in the clear: " + failure.getMessage(),
            failure.getMessage().contains("keystore")
        );
    }

    /** Names this system will not store are refused before anything is written. */
    public void testUsernamesAreRestricted() {
        expectThrows(IllegalArgumentException.class, () -> CredentialStore.validateUsername("has space"));
        expectThrows(IllegalArgumentException.class, () -> CredentialStore.validateUsername(""));
        expectThrows(IllegalArgumentException.class, () -> CredentialStore.validateUsername(null));
        expectThrows(IllegalArgumentException.class, () -> CredentialStore.validateUsername("quote\"injected"));
        CredentialStore.validateUsername("ok.name-1@example");
    }

    /**
     * Core's {@code UserSubject#authenticate} works, which is the API a plugin written for OpenSearch uses.
     *
     * <p>Also the reason the wrapper decodes the header itself: core's {@link BasicAuthToken} decodes with
     * the URL-safe Base64 alphabet, which rejects the {@code +} and {@code /} that RFC 7617 headers use.
     * The token is fine once constructed; it is the constructor that cannot read a standard header.
     */
    public void testAPluginCanAuthenticateThroughCoreSubjectApi() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Settings settings = nodeSettings("auth-subject");
        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            // With a plane but no account index, an unknown credential is "no such account" rather than
            // "the accounts are unreachable", which is the case worth checking here.
            node.setMetadataPlane(plane(clock));
            final var subject = (org.opensearch.identity.UserSubject) node.identityService().getCurrentSubject();
            subject.authenticate(urlSafeBasicToken(ADMIN, ADMIN_PASSWORD));
            final var refused = expectThrows(
                org.opensearch.OpenSearchSecurityException.class,
                () -> subject.authenticate(urlSafeBasicToken(ADMIN, "wrong"))
            );
            assertEquals(RestStatus.UNAUTHORIZED, refused.status());
        }
    }

    /**
     * The daemon can be told to run it, which is the difference between authentication that exists and
     * authentication that can be switched on.
     *
     * <p>Not plugin loading from disk: the class is already on the node's classpath and there is no
     * descriptor and no separate classloader. It is the configuration half of that, and the half that
     * decides whether an operator can enable this without editing the shell.
     */
    public void testTheDaemonCanBeConfiguredToRunIt() throws Exception {
        final java.nio.file.Path store = createTempDir();
        final Settings settings = Settings.builder()
            .put(nodeSettings("auth-daemon"))
            .put(org.opensearch.serverless.shell.ServerlessBootstrap.STORE_PATH, store.toString())
            .put(org.opensearch.serverless.shell.ServerlessBootstrap.PLUGINS, ServerlessAuthPlugin.class.getName())
            .build();

        try (var boot = org.opensearch.serverless.shell.ServerlessBootstrap.start(settings)) {
            final var http = boot.node().boundHttpAddress().publishAddress();
            assertEquals("the daemon must be running the plugin it was told to run", 1, boot.node().plugins().plugins().size());

            final Response anonymous = sendTo(http, "GET", "/", null, null);
            assertEquals("and a request with no credential must be refused: " + anonymous.body(), 401, anonymous.status());
            assertEquals(
                "while the configured account is served",
                200,
                sendTo(http, "GET", "/", basic(ADMIN, ADMIN_PASSWORD), null).status()
            );
        }
    }

    /** A plugin an operator asked for and which is not there stops the node rather than being skipped. */
    public void testADaemonRefusesAPluginItCannotFind() throws Exception {
        final Settings settings = Settings.builder()
            .put(nodeSettings("auth-daemon-missing"))
            .put(org.opensearch.serverless.shell.ServerlessBootstrap.STORE_PATH, createTempDir().toString())
            .put(org.opensearch.serverless.shell.ServerlessBootstrap.PLUGINS, "com.example.NotAPluginThatExists")
            .build();
        final var failure = expectThrows(
            IllegalArgumentException.class,
            () -> org.opensearch.serverless.shell.ServerlessBootstrap.start(settings)
        );
        assertTrue(
            "the refusal must name what it could not find: " + failure.getMessage(),
            failure.getMessage().contains("NotAPluginThatExists")
        );
    }

    private static BasicAuthToken urlSafeBasicToken(String user, String password) {
        final String encoded = Base64.getUrlEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
        return new BasicAuthToken("Basic " + encoded);
    }

    /** Creates the account index, gives its shard an owner, and does the same for any other index named. */
    private BackgroundReconciler ready(ServerlessNode node, MetadataPlane plane, AtomicLong clock, String... indices) throws Exception {
        plane.createIndex(new IndexDescriptor(AUTH_INDEX, "uuid-auth-000000000", 1, AUTH_MAPPING, null));
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want(AUTH_INDEX, 0);
        for (String index : indices) {
            loop.want(index, 0);
        }
        loop.tick(clock.get());
        return loop;
    }

    private static String basic(String user, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    private record Response(int status, String body, java.net.http.HttpHeaders headers) {
        String header(String name) {
            return headers.firstValue(name).orElse(null);
        }
    }

    private static Response send(ServerlessNode node, String method, String path, String authorization, String body) throws Exception {
        return sendTo(node.boundHttpAddress().publishAddress(), method, path, authorization, body);
    }

    private static Response sendTo(
        org.opensearch.core.common.transport.TransportAddress address,
        String method,
        String path,
        String authorization,
        String body
    ) throws Exception {
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
            return new Response(response.statusCode(), response.body(), response.headers());
        }
    }
}
