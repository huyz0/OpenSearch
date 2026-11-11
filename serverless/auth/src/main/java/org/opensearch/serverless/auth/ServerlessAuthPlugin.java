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
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.identity.PluginSubject;
import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.OnBehalfOfClaims;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestHeaderDefinition;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The shell's own authentication, written as a plugin against the shell's own plugin host.
 *
 * <p><b>Why the shell's authentication is a plugin at all.</b> The shell's plugin host supplies components,
 * routes and a request wrapper, with no injector. A test plugin alone would prove it with behaviour
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
 * {@code ActionFilter} layer, and the shell deliberately has no action layer. The single exception is one
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
 *
 * <p><b>Basic credentials need TLS on the HTTP layer, and this plugin does not supply it.</b> A Basic
 * header is the password in Base64, which is to say in the clear: on a plain-text port every credential
 * this plugin checks has already crossed the network readable by anyone on the path, and nothing here can
 * make that safe after the fact. TLS is a network plugin's job. One implements
 * {@link org.opensearch.plugins.NetworkPlugin#getHttpTransports}, registers an {@code HttpServerTransport}
 * that terminates TLS -- the security plugin's netty transport is the shape -- and the operator selects
 * it with {@code http.type}; this plugin leaves the transport and HTTP defaults exactly as it found them.
 * Until such a plugin is installed, the HTTP port must be reachable only over a network that is itself
 * trusted, which is the rule the transport port already lives under.
 *
 * <p><b>What an unauthenticated caller can make this node do, and the bounds on it.</b> Every
 * unrecognised credential costs a slow derivation, which is the point of the derivation, and a caller
 * with no credential at all can ask for as many as they like. So: the {@link #newCheckerPool checker
 * pool} is small and its queue is bounded, and a request that finds the queue full is answered 503 with a
 * {@code Retry-After} rather than queued without limit; a run of failed attempts from one address against
 * one account earns a {@link LoginThrottle wait} answered 429 at the door with no derivation and no read
 * of the store, an account's failures across every address earn a delay that is scheduled and never a
 * refusal, and an address that floods across many names exhausts a budget -- none of which can refuse a
 * credential this node has already verified, and none of which can refuse the configured account, only
 * slow it; a stored record cannot ask for more derivation than this node is prepared to pay; and a name
 * that does not exist costs the same derivation a wrong password does, so the answer's timing does not say
 * which names are real. An unknown name also costs one read of the account index, which may forward to
 * the node owning its shard, so a flood at one node is RPC load on that owner; bounded by the same pool.
 *
 * <p><b>All of that is per node.</b> The throttle and the credential cache are this process's own: a
 * fleet of N nodes gives a guesser N times the budget, and a wait earned here says nothing about the node
 * next door. Sharing either through the account index would cost a read per failure on a path an
 * unauthenticated caller drives, which is a worse trade than the multiplier.
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
        PasswordHash.MAX_ITERATIONS,
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
     * How many consecutive failures an account, or a remote address, may make before attempts are made
     * to wait.
     */
    public static final Setting<Integer> THROTTLE_FAILURES = Setting.intSetting(
        "serverless.auth.throttle.failures",
        5,
        1,
        Setting.Property.NodeScope
    );

    /**
     * The longest wait a run of failures can earn. It starts at one second and doubles per failure up to
     * this; zero switches the throttle off.
     */
    public static final Setting<TimeValue> THROTTLE_MAX_DELAY = Setting.timeSetting(
        "serverless.auth.throttle.max_delay",
        TimeValue.timeValueSeconds(60),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * How many failures one address may produce, across every account, within a minute before uncached
     * attempts from it are refused for the rest of that minute.
     *
     * <p>Well above {@link #THROTTLE_FAILURES}, because one bad account behind a shared address must not
     * reach it: once that account's pair is refused at the door its attempts are no longer failures, so
     * only a flood across many names gets here. A caller this node has already verified is never asked.
     */
    public static final Setting<Integer> THROTTLE_ADDRESS_FAILURES = Setting.intSetting(
        "serverless.auth.throttle.address_failures",
        50,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Addresses, or CIDR blocks, whose {@code X-Forwarded-For} header is believed.
     *
     * <p>Empty by default, and then the header is ignored: a client can write anything into it, and an
     * address key a client chooses is no key at all. Behind a load balancer every connection arrives from
     * the balancer, so without this the throttle would see one caller; with it, a connection from a
     * listed proxy is attributed to the rightmost forwarded address that is not itself a listed proxy --
     * the one the nearest proxy of ours actually saw.
     */
    public static final Setting<List<String>> TRUSTED_PROXIES = Setting.listSetting(
        "serverless.auth.throttle.trusted_proxies",
        List.of(),
        java.util.function.Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * How many uncached checks may wait for a checker thread before the node says it is busy.
     *
     * <p>Sized as a wait rather than as a count: the pool has {@link #CHECKER_THREADS} threads and a check
     * costs a derivation, so the default is a few seconds of backlog at the default work factor, after
     * which a 503 with a {@code Retry-After} is a better answer than a request that is served a minute
     * late.
     */
    public static final Setting<Integer> CHECK_QUEUE_SIZE = Setting.intSetting(
        "serverless.auth.check.queue_size",
        64,
        1,
        Setting.Property.NodeScope
    );

    /** Threads that may be deriving at once, which is the CPU an unauthenticated caller can occupy. */
    static final int CHECKER_THREADS = 4;

    /**
     * Where the authenticated principal is left for the rest of the request to find.
     *
     * <p>A transient rather than a header, because a header would be copied onto outgoing requests and
     * this must not be: an identity that travels is an identity another node would have to trust without
     * checking.
     */
    public static final String PRINCIPAL = "serverless_auth_principal";

    /**
     * Where a plugin doing its own work leaves its name, so a filter can tell it from a user.
     *
     * <p>Set by {@link #getPluginSubject}'s {@code runAs}, where core's contract is that the plugin acts
     * as itself and the caller's context is stashed, and by this plugin's own store for every call it
     * makes through the client, where the caller's principal is <em>kept</em> and this is added beside it:
     * an authorizer deciding account management needs to know it is the administrator asking, and needs
     * to know the write is the plugin's own. The value is {@link #pluginPrincipal}, the same name the
     * plugin subject's principal carries, and {@link ServerlessSubject#getPrincipal} reports it only when
     * no user is set -- so a filter that only knows {@code IdentityService#getCurrentSubject()} sees
     * {@code plugin:<class>} during a login lookup rather than nobody, and still sees the user during
     * account management. A transient rather than a header, for the reason {@link #PRINCIPAL} is: a
     * plugin's identity must not travel to a node that would have to take it on trust.
     */
    public static final String PLUGIN_SUBJECT = "serverless_plugin_subject";

    /**
     * Names a plugin the way its subject does.
     *
     * @param plugin the plugin class
     * @return the principal name, {@code plugin:<class>}
     */
    public static String pluginPrincipal(Class<?> plugin) {
        return "plugin:" + plugin.getName();
    }

    static final String REALM = "opensearch-serverless";

    static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(ServerlessAuthPlugin.class);

    final CredentialStore store;
    final LoginThrottle throttle;
    final List<TrustedNetwork> trustedProxies;
    final AtomicReference<Client> client = new AtomicReference<>();
    final AtomicReference<ThreadContext> context = new AtomicReference<>();
    final AtomicReference<ThreadPool> threadPool = new AtomicReference<>();
    final AtomicReference<java.util.concurrent.ExecutorService> checkers = new AtomicReference<>();

    /** Checks waiting out a delay; each holds a channel and a credential, so there is a cap. */
    final AtomicInteger pendingDelayed = new AtomicInteger();
    final int delayedCap;
    /** Authenticates REST requests. */
    final RestAuthentication restAuthentication = new RestAuthentication(this);

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
        this(settings, System::currentTimeMillis);
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
        this.store = new CredentialStore(settings, client::get, context::get, clock);
        this.throttle = new LoginThrottle(
            THROTTLE_FAILURES.get(settings),
            THROTTLE_ADDRESS_FAILURES.get(settings),
            THROTTLE_MAX_DELAY.get(settings).millis(),
            clock
        );
        this.delayedCap = CHECK_QUEUE_SIZE.get(settings);
        final List<TrustedNetwork> proxies = new java.util.ArrayList<>();
        for (String entry : TRUSTED_PROXIES.get(settings)) {
            try {
                proxies.add(TrustedNetwork.parse(entry.trim()));
            } catch (IllegalArgumentException e) {
                // At construction, and therefore at node start: a proxy list with a typo in it would
                // otherwise quietly count a whole load balancer as one caller.
                throw new IllegalArgumentException(
                    "[" + TRUSTED_PROXIES.getKey() + "] has an entry that is neither an address nor a CIDR block: [" + entry + "]",
                    e
                );
            }
        }
        this.trustedProxies = List.copyOf(proxies);
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
        return List.of(
            INDEX,
            BOOTSTRAP_USER,
            BOOTSTRAP_PASSWORD,
            ITERATIONS,
            CACHE_TTL,
            CACHE_SIZE,
            LOOKUP_TIMEOUT,
            THROTTLE_FAILURES,
            THROTTLE_MAX_DELAY,
            THROTTLE_ADDRESS_FAILURES,
            TRUSTED_PROXIES,
            CHECK_QUEUE_SIZE
        );
    }

    /**
     * Takes the client and the pools, and exports nothing.
     *
     * <p>Core binds what a plugin returns here into its injector, for that plugin's own actions. The shell
     * has no injector and resolves a plugin action's constructor against <em>every</em> plugin's
     * components, so a component returned here is handed to any other installed plugin that declares its
     * type in a constructor -- and the store has {@code put}. Returning it would let a second plugin
     * rewrite the configured account by asking for a {@code CredentialStore}. This plugin's own handlers
     * hold the store directly and need nothing from the list.
     *
     * @param client the node client, kept so a handler can act as this plugin
     * @param clusterService core's cluster service, unused here
     * @param threadPool the node's pools, for the thread context and the credential-checking pool
     * @param resourceWatcherService core's watcher service, unused here
     * @param scriptService core's script service, unused here
     * @param xContentRegistry core's parser registry, unused here
     * @param environment the node's environment, for the settings the checker pool is sized from
     * @param nodeEnvironment the node's paths, unused here
     * @param namedWriteableRegistry core's writeable registry, unused here
     * @param indexNameExpressionResolver core's resolver, unused here
     * @param repositoriesServiceSupplier core's repositories, unused here
     * @return nothing, deliberately -- see above
     */
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
        this.threadPool.compareAndSet(null, threadPool);
        this.checkers.compareAndSet(null, newCheckerPool(environment.settings(), threadPool.getThreadContext()));
        return List.of();
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
     * <p><b>Fixed, with a bounded queue.</b> The earlier shape was a scaling pool over an unbounded queue,
     * which bounded the CPU and left the memory open: every request past the fourth waited in a list that
     * grew as fast as an attacker could send, each one holding a channel and a decoded credential. A
     * fixed pool with a queue of {@link #CHECK_QUEUE_SIZE} rejects the next one instead, and the request
     * path turns that rejection into a 503 with a {@code Retry-After}. Four threads sitting idle is the
     * cost, and it is not much of one.
     *
     * @param settings the node settings, for thread naming and the queue bound
     * @param threadContext the context checkers run under
     * @return the pool
     */
    private static java.util.concurrent.ExecutorService newCheckerPool(Settings settings, ThreadContext threadContext) {
        return org.opensearch.common.util.concurrent.OpenSearchExecutors.newFixed(
            "serverless_auth",
            CHECKER_THREADS,
            CHECK_QUEUE_SIZE.get(settings),
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
     * credential this node checked moments ago costs a keyed hash and a map lookup, and runs on the thread
     * that read the request. One it has not seen costs a deliberately slow derivation and possibly a read
     * from the object store, and blocking the HTTP event loop on either would trade the safety of every
     * request for the convenience of writing this in one branch. So a miss is handed to
     * {@link #newCheckerPool a pool of this plugin's own} and the handler runs from there.
     *
     * <p><b>The cache before the throttle, on purpose.</b> The throttle prices guesses, and a cache hit is
     * not a guess: it is a caller this node verified within the TTL. Consulting the throttle first was
     * what let one bad account behind a shared address refuse every good one behind it. The cost of this
     * order is small and stated: a guesser whose pair is waiting learns at once, rather than after the
     * wait, if a guess happens to be the password some other caller verified here in the last minute --
     * a guesser who by then holds the password either way.
     *
     * <p><b>A wait is a refusal or a delay, and the throttle says which.</b> Refusals -- no header, the
     * wrong scheme, an unreadable one, a pair or an address that has earned a wait -- are answered inline
     * with no work, because dispatching in order to say "not yet" would let an unauthenticated caller queue
     * work on this node. A delay is scheduled on the node's scheduler and submitted to the checker pool
     * when it ends, so the wait holds a connection and nothing else: no checker thread sleeps through it,
     * which would have let four requests a minute idle the whole pool. Delays are bounded by
     * {@link #CHECK_QUEUE_SIZE}, since each holds a channel and a credential, and past the bound the
     * answer is the 503 a full queue gets. The configured account is only ever delayed, which is what
     * makes it a recovery path rather than a name anyone can lock.
     *
     * @param threadContext the node's thread context, where an authenticated principal is left
     * @param headersToCopy the headers the controller preserves, none of which this uses
     * @return the wrapper every handler is registered behind
     */
    @Override
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext, Set<RestHeaderDefinition> headersToCopy) {
        return restAuthentication.getRestHandlerWrapper(threadContext, headersToCopy);
    }

    /** One entry of {@link #TRUSTED_PROXIES}: an address, or a CIDR block. */
    record TrustedNetwork(byte[] network, int prefixBits) {

        static TrustedNetwork parse(String entry) {
            if (entry.indexOf('/') >= 0) {
                final var cidr = InetAddresses.parseCidr(entry);
                return new TrustedNetwork(cidr.v1().getAddress(), cidr.v2());
            }
            final byte[] address = InetAddresses.forString(entry).getAddress();
            return new TrustedNetwork(address, address.length * 8);
        }

        boolean contains(InetAddress address) {
            final byte[] bytes = address.getAddress();
            if (bytes.length != network.length) {
                return false;
            }
            int remaining = prefixBits;
            for (int i = 0; i < bytes.length && remaining > 0; i++, remaining -= 8) {
                final int mask = remaining >= 8 ? 0xFF : (0xFF << (8 - remaining)) & 0xFF;
                if ((bytes[i] & mask) != (network[i] & mask)) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public Subject getCurrentSubject() {
        return new ServerlessSubject(context.get(), store, throttle);
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
        // The REST surface's rule, one layer down: refuse and say why, rather than hand back something that
        // looks like a token and authenticates nobody. Both of these delegate authority, and delegating authority is only
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
                return new org.opensearch.identity.NamedPrincipal(pluginPrincipal(plugin.getClass()));
            }

            @Override
            public <E extends Exception> void runAs(org.opensearch.common.CheckedRunnable<E> r) throws E {
                final ThreadContext threadContext = context.get();
                if (threadContext == null) {
                    r.run();
                    return;
                }
                // The caller's principal and headers are stashed and restored after; the thread carries
                // the plugin's name instead, which is what a filter reads to tell plugin-internal access
                // from a user's, and what the identity service reports while this runs.
                try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                    threadContext.putTransient(PLUGIN_SUBJECT, pluginPrincipal(plugin.getClass()));
                    r.run();
                }
            }
        };
    }

}
