/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.auth;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.identity.PluginSubject;
import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.opensearch.identity.tokens.OnBehalfOfClaims;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestHeaderDefinition;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The shell's own authentication, written as a plugin against the shell's own plugin host.
 *
 * <p><b>Why the shell's authentication is a plugin at all.</b> M23 built a host: components, routes and a
 * request wrapper, with no injector. It was proved by a test plugin, which is a plugin whose behaviour was
 * chosen to make the host look good. This is the same host carrying something the system genuinely needs,
 * on the critical path of every request, storing its state in an index through the client. If the host
 * cannot carry this, it cannot carry OpenSearch Security either, and it is better to find that out from
 * code we wrote than from code we did not.
 *
 * <p><b>All three hooks, for real reasons rather than for coverage.</b> {@code createComponents} is where
 * the account store gets the client it reads through; {@code getRestHandlers} serves the endpoint that
 * creates accounts, because a system whose credentials can only be seeded by hand is not one anybody runs;
 * {@code getRestHandlerWrapper} is the seam every request passes through, which is the only place a shell
 * with no action layer can enforce anything at all.
 *
 * <p><b>What this is not.</b> It authenticates and it does not authorize. Every authenticated caller can
 * do everything, because privilege evaluation in OpenSearch is keyed on action names at the
 * {@code ActionFilter} layer, and R4/&sect;6.3 leaves that layer unbuilt. The single exception is one
 * hard-coded rule — only the configured account may manage accounts — which exists so that account
 * management is not self-service, and which is deliberately one rule rather than the beginnings of a role
 * model. Calling it "roles" when it is one {@code if} would be the kind of half-implemented security that
 * is worse than none, because it invites people to rely on it.
 *
 * <p><b>Two more things it does not do, said out loud.</b> Node-to-node forwarding carries no identity:
 * a request authenticated on the node that received it is forwarded to the shard's owner as an internal
 * transport call, so the transport port must be treated as trusted and kept off any network a client can
 * reach. And a path with no registered handler answers 404 without being authenticated, because the
 * wrapper wraps handlers; that leaks which endpoints exist and nothing else.
 */
public final class ServerlessAuthPlugin extends Plugin implements org.opensearch.plugins.SystemIndexPlugin, IdentityPlugin {

    /** Where accounts are kept. */
    public static final Setting<String> INDEX = Setting.simpleString(
        "serverless.auth.index",
        ".serverless_auth",
        Setting.Property.NodeScope
    );

    /** The account that is configured rather than stored, and is checked before the store. */
    public static final Setting<String> BOOTSTRAP_USER = Setting.simpleString(
        "serverless.auth.bootstrap.username",
        "admin",
        Setting.Property.NodeScope
    );

    /**
     * Its password, which lives in the keystore and nowhere else.
     *
     * <p><b>A {@code SecureSetting}, not a plain one.</b> A password written into {@code opensearch.yml}
     * is a password in every backup, every configuration-management repository and every process listing
     * that ever touched that file. The shell's bootstrap already folds a keystore into its settings for
     * the object store's credentials, so there was a right place to put this and no reason to invent a
     * weaker one.
     *
     * <p>It also fails loudly the other way round: core refuses a secure setting found in
     * {@code opensearch.yml} rather than reading it, so configuring it in the clear is an error at
     * startup instead of a quiet downgrade.
     */
    public static final Setting<org.opensearch.core.common.settings.SecureString> BOOTSTRAP_PASSWORD =
        org.opensearch.common.settings.SecureSetting.secureString("serverless.auth.bootstrap.password", null);

    /** The derivation work factor for new and changed passwords. */
    public static final Setting<Integer> ITERATIONS = Setting.intSetting(
        "serverless.auth.hash.iterations",
        PasswordHash.DEFAULT_ITERATIONS,
        10_000,
        Setting.Property.NodeScope
    );

    /** How long a verified credential is trusted without being checked again. */
    public static final Setting<TimeValue> CACHE_TTL = Setting.timeSetting(
        "serverless.auth.cache.ttl",
        TimeValue.timeValueSeconds(60),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /** How many verified credentials a node remembers at once. */
    public static final Setting<Integer> CACHE_SIZE = Setting.intSetting(
        "serverless.auth.cache.size",
        10_000,
        0,
        Setting.Property.NodeScope
    );

    /** How long a lookup in the account store may take before it is treated as unreachable. */
    public static final Setting<TimeValue> LOOKUP_TIMEOUT = Setting.timeSetting(
        "serverless.auth.lookup.timeout",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    /**
     * Where the authenticated principal is left for the rest of the request to find.
     *
     * <p>A transient rather than a header, because a header would be copied onto outgoing requests and
     * this must not be: an identity that travels is an identity another node would have to trust without
     * checking.
     */
    public static final String PRINCIPAL = "serverless_auth_principal";

    private static final String REALM = "opensearch-serverless";

    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(ServerlessAuthPlugin.class);

    private final CredentialStore store;
    private final AtomicReference<Client> client = new AtomicReference<>();
    private final AtomicReference<ThreadContext> context = new AtomicReference<>();
    private final AtomicReference<java.util.concurrent.ExecutorService> checkers = new AtomicReference<>();

    /**
     * Creates the plugin.
     *
     * <p>Takes {@link Settings} at construction because {@code getRestHandlerWrapper} is called while the
     * node is still being built — the REST controller takes the wrapper in its constructor — and so the
     * configured account has to be known before {@code createComponents} ever runs. That ordering is also
     * what lets the configured account work during startup, before the store exists.
     *
     * @param settings the node settings
     */
    public ServerlessAuthPlugin(Settings settings) {
        this.store = new CredentialStore(settings, client::get, System::currentTimeMillis);
    }

    /**
     * Creates the plugin with a clock of the caller's choosing, so a test can age the credential cache
     * without sleeping through its TTL.
     *
     * <p><b>A factory rather than a second constructor, and that is not a style preference.</b> Core's
     * loader refuses a plugin class with more than one public constructor — {@code loadPlugin} throws "no
     * unique public constructor" — so a second one would make this plugin impossible to install from disk
     * while working perfectly in every test that constructed it directly.
     *
     * @param settings the node settings
     * @param clock the millisecond clock
     * @return the plugin
     */
    public static ServerlessAuthPlugin withClock(Settings settings, java.util.function.LongSupplier clock) {
        return new ServerlessAuthPlugin(settings, clock);
    }

    private ServerlessAuthPlugin(Settings settings, java.util.function.LongSupplier clock) {
        this.store = new CredentialStore(settings, client::get, clock);
    }

    /**
     * Returns the account store, so a test can drive it without going through HTTP.
     *
     * @return the store
     */
    public CredentialStore store() {
        return store;
    }

    /**
     * Declares the account index as this plugin's own.
     *
     * <p>Core's hook, used the way any plugin uses it. What the shell does with it is refuse to route any
     * REST request to a declared index at all — which is what stops an authenticated caller from reading
     * the password records out of it, or from writing a record of their own choosing under somebody else's
     * account name and becoming them. The plugin still reaches it through the {@link Client}, which is the
     * path it was always using.
     *
     * @param settings the node settings
     * @return the account index
     */
    @Override
    public Collection<org.opensearch.indices.SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            new org.opensearch.indices.SystemIndexDescriptor(INDEX.get(settings), "accounts for the serverless shell's authentication")
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(INDEX, BOOTSTRAP_USER, BOOTSTRAP_PASSWORD, ITERATIONS, CACHE_TTL, CACHE_SIZE, LOOKUP_TIMEOUT);
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.client.set(client);
        this.context.compareAndSet(null, threadPool.getThreadContext());
        this.checkers.compareAndSet(null, newCheckerPool(environment.settings(), threadPool.getThreadContext()));
        return List.of(store);
    }

    /**
     * The pool that pays for checking a credential.
     *
     * <p><b>Its own, rather than the node's {@code GENERIC}, for two reasons.</b> The first is a deadlock:
     * checking an account blocks on a client call, and the shell's client runs that call on {@code GENERIC};
     * a checker holding a {@code GENERIC} thread while waiting for a {@code GENERIC} task is a hazard that
     * only shows up under load, which is the worst time to find it. The second is that a small pool is a
     * budget. Every unrecognised credential costs a deliberately slow derivation, so an attacker sending
     * many wrong passwords is asking this node to spend CPU; bounding the threads that can be doing that at
     * once bounds what they can take, at the price of queueing logins during such a flood -- which is the
     * right way round.
     *
     * @param settings the node settings, for thread naming
     * @param threadContext the context checkers run under
     * @return the pool
     */
    private static java.util.concurrent.ExecutorService newCheckerPool(Settings settings, ThreadContext threadContext) {
        return org.opensearch.common.util.concurrent.OpenSearchExecutors.newScaling(
            "serverless_auth",
            1,
            4,
            30,
            java.util.concurrent.TimeUnit.SECONDS,
            org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory(settings, "serverless_auth"),
            threadContext
        );
    }

    @Override
    public void close() {
        final java.util.concurrent.ExecutorService pool = checkers.getAndSet(null);
        if (pool != null) {
            pool.shutdownNow();
        }
    }

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
        return List.of(new UsersHandler(store, context::get, checkers::get));
    }

    /**
     * The seam. Every request passes through here before its handler sees it.
     *
     * <p><b>Two paths, because checking a credential is two very different amounts of work.</b> A
     * credential this node checked moments ago costs a hash and a map lookup, and runs on the thread that
     * read the request. One it has not seen costs a deliberately slow derivation and possibly a read from
     * the object store, and blocking the HTTP event loop on either would trade the safety of every request
     * for the convenience of writing this in one branch. So a miss is handed to {@link #newCheckerPool a
     * pool of this plugin's own} and the handler runs from there.
     *
     * <p>Refusals that need no work at all -- no header, the wrong scheme, an unreadable one -- are
     * answered inline, because dispatching in order to say "no credentials were offered" would let an
     * unauthenticated caller queue work on this node.
     *
     * @param threadContext the node's thread context, where an authenticated principal is left
     * @param headersToCopy the headers the controller preserves, none of which this uses
     * @return the wrapper every handler is registered behind
     */
    @Override
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext, Set<RestHeaderDefinition> headersToCopy) {
        this.context.set(threadContext);
        return original -> (RestHandler) (request, channel, client) -> {
            final String header = request.header("Authorization");
            if (header == null || header.isBlank()) {
                refuse(channel, RestStatus.UNAUTHORIZED, "no_credentials", "no credentials were offered", true);
                return;
            }
            if (header.regionMatches(true, 0, "Basic ", 0, "Basic ".length()) == false) {
                // Bearer is in core's token vocabulary and nothing here issues one, so accepting the header
                // and ignoring it would be worse than saying which scheme this node understands.
                refuse(
                    channel,
                    RestStatus.UNAUTHORIZED,
                    "unsupported_scheme",
                    "this node authenticates with HTTP Basic; no other scheme is configured",
                    true
                );
                return;
            }
            final String[] offered = decodeBasic(header);
            if (offered == null) {
                refuse(channel, RestStatus.UNAUTHORIZED, "malformed_credentials", "the Basic credentials could not be read", true);
                return;
            }

            final char[] password = offered[1].toCharArray();
            final boolean known;
            try {
                known = store.isCached(offered[0], password);
            } finally {
                Arrays.fill(password, '\0');
            }
            if (known) {
                admit(threadContext, offered[0]);
                original.handleRequest(request, channel, client);
                return;
            }

            final java.util.concurrent.ExecutorService pool = checkers.get();
            if (pool == null) {
                // The HTTP transport binds before plugins build their components, so there is a window at
                // startup where requests arrive and there is nowhere to check them. Brief, and a 503.
                refuse(channel, RestStatus.SERVICE_UNAVAILABLE, "authentication_unavailable", "this node has not finished starting", false);
                return;
            }
            pool.execute(() -> {
                try {
                    if (check(threadContext, offered[0], offered[1], channel)) {
                        original.handleRequest(request, channel, client);
                    }
                } catch (Exception e) {
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    } catch (Exception nested) {
                        LOGGER.error("failed to report an authentication failure", nested);
                    }
                }
            });
        };
    }

    /**
     * Checks a credential that was not already known, and refuses if it does not hold up.
     *
     * @param threadContext where the principal is left on success
     * @param user the username offered
     * @param secret the password offered
     * @param channel the channel to refuse on
     * @return true if the handler should run
     * @throws Exception if the refusal cannot be written
     */
    private boolean check(ThreadContext threadContext, String user, String secret, RestChannel channel) throws Exception {
        final char[] password = secret.toCharArray();
        final CredentialStore.Verdict verdict;
        try {
            verdict = store.verify(user, password);
        } finally {
            Arrays.fill(password, '\0');
        }

        if (verdict.unavailable() != null) {
            // Not a 401. The caller may well have offered a perfectly good credential, and telling them it
            // was rejected would send them to change a password that was never the problem.
            return refuse(channel, RestStatus.SERVICE_UNAVAILABLE, "authentication_unavailable", verdict.unavailable(), false);
        }
        if (verdict.authenticated() == false) {
            return refuse(channel, RestStatus.UNAUTHORIZED, "authentication_failed", "the credentials offered were not accepted", true);
        }
        admit(threadContext, verdict.principal());
        return true;
    }

    /** Records who the caller is, for the handler and for any plugin that asks the identity service. */
    private static void admit(ThreadContext threadContext, String principal) {
        // Each request runs in a stashed context, so this is set once per request and never overwrites a
        // value from another one -- but a wrapped handler that retries would, and putTransient throws on a
        // second write rather than quietly replacing an identity.
        if (threadContext.getTransient(PRINCIPAL) == null) {
            threadContext.putTransient(PRINCIPAL, principal);
        }
    }

    /**
     * Reads a Basic header into a username and a password.
     *
     * <p><b>Standard Base64, not the URL-safe alphabet.</b> RFC 7617 says {@code base64(user:pass)} in the
     * alphabet with {@code +} and {@code /}; core's {@link BasicAuthToken} decodes with
     * {@code Base64.getUrlDecoder()}, which rejects exactly those two characters, so roughly one header in
     * two with a random password would fail to parse. Using core's class here would have inherited that,
     * so this decodes the header itself and leaves the token type to {@link ServerlessSubject}, where it
     * is the caller who constructed it.
     *
     * @param header the Authorization header
     * @return the username and password, or null if the header is not readable
     */
    private static String[] decodeBasic(String header) {
        try {
            final byte[] decoded = Base64.getDecoder().decode(header.substring("Basic ".length()).trim());
            final String pair = new String(decoded, StandardCharsets.UTF_8);
            final int colon = pair.indexOf(':');
            if (colon <= 0) {
                return null;
            }
            return new String[] { pair.substring(0, colon), pair.substring(colon + 1) };
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static boolean refuse(RestChannel channel, RestStatus status, String type, String reason, boolean challenge) {
        final org.opensearch.core.xcontent.XContentBuilder body;
        final BytesRestResponse response;
        try {
            body = channel.newErrorBuilder()
                .startObject()
                .startObject("error")
                .field("type", type)
                .field("reason", reason)
                .endObject()
                .field("status", status.getStatus())
                .endObject();
            response = new BytesRestResponse(status, body);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("could not write an authentication refusal", e);
        }
        if (challenge) {
            // Without this a browser or a client library has no way to know it should offer a credential,
            // and a 401 with no challenge reads as a broken server rather than a locked door.
            response.addHeader("WWW-Authenticate", "Basic realm=\"" + REALM + "\", charset=\"UTF-8\"");
        }
        channel.sendResponse(response);
        return false;
    }

    @Override
    public Subject getCurrentSubject() {
        return new ServerlessSubject(context.get(), store);
    }

    @Override
    public TokenManager getTokenManager() {
        return new TokenManager() {
            @Override
            public AuthToken issueOnBehalfOfToken(Subject subject, OnBehalfOfClaims claims) {
                throw new UnsupportedOperationException(unsupported("on-behalf-of tokens"));
            }

            @Override
            public AuthToken issueServiceAccountToken(String audience) {
                throw new UnsupportedOperationException(unsupported("service account tokens"));
            }
        };
    }

    private static String unsupported(String what) {
        // D2, one layer down: refuse and say why, rather than hand back something that looks like a token
        // and authenticates nobody. Both of these delegate authority, and delegating authority is only
        // meaningful where there is authorization to delegate.
        return "the serverless shell does not issue "
            + what
            + ": they delegate authority, and this deployment authenticates without authorizing, so there "
            + "is no authority to delegate";
    }

    @Override
    public PluginSubject getPluginSubject(Plugin plugin) {
        return new PluginSubject() {
            @Override
            public java.security.Principal getPrincipal() {
                // Named after the plugin, not after whoever happened to make the request. A plugin doing
                // its own work is not acting as a user, and a subject that said otherwise would be a
                // confused-deputy waiting for an authorization layer to arrive.
                return new org.opensearch.identity.NamedPrincipal("plugin:" + plugin.getClass().getName());
            }

            @Override
            public <E extends Exception> void runAs(org.opensearch.common.CheckedRunnable<E> r) throws E {
                final ThreadContext threadContext = context.get();
                if (threadContext == null) {
                    r.run();
                    return;
                }
                try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                    r.run();
                }
            }
        };
    }

}
