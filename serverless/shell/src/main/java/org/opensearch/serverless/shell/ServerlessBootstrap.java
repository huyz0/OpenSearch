/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/**
 * A serverless node as a process.
 *
 * <p>This closes the last of M13's headline gap. The scheduler made a node capable of running by itself;
 * until this existed nothing ever built one outside a test, so "a node runs unattended" was true of an
 * arrangement that only ever occurred inside a JUnit method.
 *
 * <p><b>What a fresh node serves.</b> A booted node is handed no assignments, because there is nothing
 * to hand them out — that is the point of the design. So it starts with
 * {@link BackgroundReconciler#setDemandDrivenActivation demand-driven activation on}: it takes an
 * unowned shard when somebody asks for one, up to its cap, and refuses past that so the next node asked
 * takes it instead. That default is the opposite of the library's, deliberately. A library that silently
 * changed placement policy would be a trap; a daemon that started owning nothing and never acquired
 * anything would be useless. Both defaults are visible, and
 * {@code serverless.activation.on_demand} turns this one off.
 *
 * <p><b>Per D5, filesystem-backed only.</b> {@code serverless.store.path} is a local directory standing
 * in for an object store. That is not a deployment story — it is the same restriction every test in this
 * project runs under, and R11 is what would lift it. A process that pretended otherwise by accepting an
 * S3 bucket it has never been run against would be worse than one that says what it is.
 */
public final class ServerlessBootstrap implements Closeable {

    private static final Logger logger = LogManager.getLogger(ServerlessBootstrap.class);

    /** Where the stand-in object store lives. Required. */
    public static final String STORE_PATH = "serverless.store.path";

    /** How long a lease stays valid after a renewal. */
    public static final String LEASE_TTL = "serverless.lease.ttl_millis";

    /** Whether to take unowned shards that somebody asks for. */
    public static final String ON_DEMAND = "serverless.activation.on_demand";

    /** How many shards this node will take on demand before refusing. */
    public static final String MAX_SHARDS = "serverless.activation.max_shards";

    /**
     * How long writes are gathered before one publish covers them all. Exposed because it is the knob
     * that decides how much a crashed node leaves for its successor to replay from the log.
     */
    public static final String PUBLISH_DEBOUNCE = "serverless.publish.debounce_millis";

    /** Whether shard liveness is derived from node leases rather than per-head expiry. */
    public static final String NODE_LEASE_LIVENESS = "serverless.lease.node_liveness";

    /** The first token of the readiness line, so the file is self-describing. */
    public static final String READY_MARKER = "SERVERLESS_READY";

    /** Where to write the readiness line once the node is serving. Optional. */
    public static final String READY_FILE = "serverless.ready.file";

    /**
     * Where {@code opensearch.keystore} lives. Defaults to {@code path.home/config}, as for a real node.
     */
    public static final String CONFIG_PATH = "path.conf";

    /** The default lease TTL: long enough to survive a slow object store, short enough to fail over. */
    public static final long DEFAULT_LEASE_TTL_MILLIS = 30_000L;

    /**
     * System property prefixes {@link #main} forwards into settings. An allowlist rather than everything,
     * because passing the whole JVM environment to a settings parser turns an unrelated {@code -D} into a
     * startup failure.
     */
    private static final java.util.List<String> SETTING_PREFIXES = java.util.List.of(
        "serverless.",
        "node.",
        "cluster.",
        "path.",
        "http.",
        "transport.",
        "network."
    );

    private final ServerlessNode node;
    private final MetadataPlane plane;
    private final BackgroundReconciler loop;
    private final ReconcileScheduler scheduler;
    private final org.opensearch.serverless.store.ObjectStores.Handle store;

    private ServerlessBootstrap(
        ServerlessNode node,
        MetadataPlane plane,
        BackgroundReconciler loop,
        ReconcileScheduler scheduler,
        org.opensearch.serverless.store.ObjectStores.Handle store
    ) {
        this.node = node;
        this.plane = plane;
        this.loop = loop;
        this.scheduler = scheduler;
        this.store = store;
    }

    /**
     * Builds and starts a node: metadata plane, data plane, HTTP, and the three drivers.
     *
     * <p>Ordering matters in one place. The scheduler starts <em>last</em>, after the node is serving and
     * the plane is attached, because a renewal that ran before the node could answer would advertise a
     * lease pointing at an address that refuses connections — and peers resolve forwarding targets from
     * exactly that lease.
     *
     * @param settings the node's settings, including {@link #STORE_PATH}
     * @return the running node
     * @throws Exception if any part of startup fails
     */
    public static ServerlessBootstrap start(Settings settings) throws Exception {
        final String storeType = settings.get(org.opensearch.serverless.store.ObjectStores.TYPE, "fs");
        if ("s3".equals(storeType) && System.getProperty("opensearch.path.conf") == null) {
            // The AWS SDK v2 is told where the node's config lives so it does not go looking in the home
            // directory, and the s3 plugin passes that property straight into System.setProperty. Unset,
            // it throws a bare NullPointerException from inside the SDK during client construction, which
            // says nothing about the cause. A launcher script normally sets it; say so rather than let a
            // node die anonymously.
            throw new IllegalStateException(
                "an s3 store requires the system property opensearch.path.conf to be set (the launcher normally sets it); "
                    + "start this process with -Dopensearch.path.conf=<config dir>"
            );
        }
        if ("fs".equals(storeType) && (settings.get(STORE_PATH) == null || settings.get(STORE_PATH).isBlank())) {
            throw new IllegalArgumentException(STORE_PATH + " is required: it is where the metadata plane and segments live");
        }
        final long ttl = settings.getAsLong(LEASE_TTL, DEFAULT_LEASE_TTL_MILLIS);

        // Secure settings, before anything reads them. An object store's credentials are SecureSettings
        // and live in the keystore, so without this a deployed node has no way to be given any: the shell
        // could talk to S3 in a test that supplied MockSecureSettings by hand, and not in production.
        final Path configPath = configPath(settings);
        final Settings complete = withSecureSettings(settings, configPath);

        // The node comes first, and it has to: an s3 store is built through the repository plugin, which
        // wants a ClusterService, and the node is what owns one. The plane is attached immediately
        // afterwards, so nothing observable happens in between.
        final ServerlessNode node = new ServerlessNode(complete);
        node.start();

        final org.opensearch.serverless.store.ObjectStores.Handle store = org.opensearch.serverless.store.ObjectStores.create(
            complete,
            node.clusterService(),
            configPath
        );
        final MetadataPlane plane = new MetadataPlane(
            store.blobStore(),
            BlobPath.cleanPath(),
            System::currentTimeMillis,
            ttl,
            complete.getAsBoolean(NODE_LEASE_LIVENESS, false)
        );
        node.setMetadataPlane(plane);

        final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(
            complete.getAsBoolean(ON_DEMAND, true)
        ).setMaxShardsHeld(complete.getAsInt(MAX_SHARDS, BackgroundReconciler.DEFAULT_MAX_SHARDS_HELD));

        final ReconcileScheduler scheduler = new ReconcileScheduler(
            loop,
            node.threadPool(),
            plane.clock(),
            org.opensearch.common.unit.TimeValue.timeValueMillis(Math.max(1L, ttl / ReconcileScheduler.RENEWALS_PER_TTL)),
            org.opensearch.common.unit.TimeValue.timeValueMillis(
                complete.getAsLong(PUBLISH_DEBOUNCE, ReconcileScheduler.DEFAULT_PUBLISH_DEBOUNCE.millis())
            ),
            ReconcileScheduler.DEFAULT_BACKSTOP_INTERVAL
        );
        node.setSignals(scheduler);
        scheduler.start();

        logger.info(
            "serverless node [{}] running: {} store, lease ttl {}ms, http {}, on-demand activation {}",
            node.localNode().getName(),
            storeType,
            ttl,
            node.boundHttpAddress().publishAddress(),
            complete.getAsBoolean(ON_DEMAND, true)
        );
        return new ServerlessBootstrap(node, plane, loop, scheduler, store);
    }

    private static Path configPath(Settings settings) {
        final String configured = settings.get(CONFIG_PATH);
        if (configured != null && configured.isBlank() == false) {
            return Path.of(configured);
        }
        return Path.of(settings.get("path.home")).resolve("config");
    }

    /**
     * Folds the keystore into the settings, if there is one.
     *
     * <p>Absent keystore is not an error: a node backed by the filesystem needs no secrets, and demanding
     * one would make the simplest configuration the one that fails. A keystore that exists and cannot be
     * read <em>is</em> an error, because the alternative is starting a node that silently cannot reach
     * its object store and only says so on the first write.
     */
    private static Settings withSecureSettings(Settings settings, Path configPath) throws Exception {
        final org.opensearch.common.settings.KeyStoreWrapper keystore = org.opensearch.common.settings.KeyStoreWrapper.load(configPath);
        if (keystore == null) {
            return settings;
        }
        if (keystore.hasPassword()) {
            throw new IllegalStateException(
                "the keystore at " + configPath + " is password-protected, and this bootstrap has no way to prompt for one"
            );
        }
        keystore.decrypt(new char[0]);
        logger.info("loaded {} secure setting(s) from {}", keystore.getSettingNames().size(), configPath);
        return Settings.builder().put(settings).setSecureSettings(keystore).build();
    }

    /**
     * Returns the running node.
     *
     * @return the node
     */
    public ServerlessNode node() {
        return node;
    }

    /**
     * Returns the metadata plane this node reconciles against.
     *
     * @return the plane
     */
    public MetadataPlane plane() {
        return plane;
    }

    /**
     * Returns the reconciler the scheduler drives.
     *
     * @return the reconciler
     */
    public BackgroundReconciler reconciler() {
        return loop;
    }

    /**
     * Returns the scheduler running this node's three drivers.
     *
     * @return the scheduler
     */
    public ReconcileScheduler scheduler() {
        return scheduler;
    }

    /**
     * Stops the drivers, then the node.
     *
     * <p>In that order, and it is not tidiness: a driver that ran against a closing node would try to
     * publish or renew through a half-shut data plane. Stopping the clocks first means everything after
     * this point is quiescent.
     */
    @Override
    public void close() {
        // Stop the clocks first, and it is not tidiness: a driver running against a closing node would
        // publish or renew through a half-shut data plane, and a renewal racing the release below would
        // put the lease back after we dropped it.
        scheduler.close();
        loop.close();
        try {
            // Drop the lease rather than let it lapse. Without this a node that shut down cleanly stays
            // alive to every peer for a full TTL: its shards are unowned and unclaimable that whole time,
            // because a live lease is never stolen. Releasing is what makes a restart a handover instead
            // of a failover, and it costs one delete.
            plane.membership().release(node.localNode().getId());
        } catch (Exception e) {
            logger.warn("could not release this node's lease; peers will treat it as alive until the TTL elapses", e);
        }
        node.close();
        try {
            // Last, because everything above may still be talking to it.
            store.close();
        } catch (Exception e) {
            logger.warn("could not close the object store cleanly", e);
        }
    }

    /**
     * Runs a node until the process is signalled.
     *
     * <p>Settings come from system properties, so {@code -Dserverless.store.path=/data} works and there
     * is no configuration file format to invent before there is anything to configure.
     *
     * @param args ignored
     * @throws Exception if startup fails
     */
    public static void main(String[] args) throws Exception {
        final Settings.Builder settings = Settings.builder();
        // BootstrapInfo rather than System.getProperties(): the latter hands out a mutable view of the
        // JVM's properties and is forbidden across this codebase for that reason.
        final java.util.Dictionary<Object, Object> properties = org.opensearch.bootstrap.BootstrapInfo.getSystemProperties();
        for (java.util.Enumeration<Object> names = properties.keys(); names.hasMoreElements();) {
            final String name = String.valueOf(names.nextElement());
            if (SETTING_PREFIXES.stream().anyMatch(name::startsWith)) {
                settings.put(name, String.valueOf(properties.get(name)));
            }
        }

        final ServerlessBootstrap bootstrap = start(settings.build());

        // A readiness file, the way a daemon tells an orchestrator it is up. Written last, after the node
        // is actually serving, so its existence means something. A file rather than a line on stdout
        // because stdout is forbidden in production code here -- and it is the better mechanism anyway:
        // it does not race with log output and it survives whatever is or is not tailing the process.
        final String readyFile = System.getProperty(READY_FILE);
        if (readyFile != null) {
            java.nio.file.Files.writeString(
                Path.of(readyFile),
                READY_MARKER
                    + " http="
                    + bootstrap.node().boundHttpAddress().publishAddress()
                    + " transport="
                    + bootstrap.node().boundTransportAddress().publishAddress()
                    + " node="
                    + bootstrap.node().localNode().getId()
                    + System.lineSeparator()
            );
        }

        final CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Closing here rather than only counting down: a node killed without releasing its lease
            // stays "alive" to everyone else for a full TTL, and its shards sit unowned for that long.
            // Shutting down cleanly is what turns a restart from a failover into a handover.
            try {
                bootstrap.close();
            } catch (Exception e) {
                logger.warn("unclean shutdown", e);
            } finally {
                stopped.countDown();
            }
        }, "serverless-shutdown"));
        stopped.await();
    }
}
