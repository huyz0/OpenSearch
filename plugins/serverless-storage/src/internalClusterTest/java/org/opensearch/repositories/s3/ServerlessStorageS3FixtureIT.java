/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import com.sun.net.httpserver.HttpServer;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStorageIntegTestCase;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import fixture.s3.S3HttpHandler;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * rfc-serverless-opensearch.md &sect;16 Phase 1's own remaining status note: "one real
 * cloud-object-store integration test (currently FS/mock only)." Every other real-cluster IT in
 * this plugin (including {@code ServerlessStorageRepositoryBackedContainerIT}, which proves the
 * {@code serverless_storage.repository} seam itself) only ever exercises a repository-backed
 * container against the built-in {@code fs} repository type -- correct for proving the seam is
 * wired right, but never actually walks a real cloud object-store's own HTTP wire protocol.
 *
 * <p>This class closes that gap using {@code repository-s3}'s own production {@link
 * S3RepositoryPlugin} against {@code test:fixtures:s3-fixture}'s real (in-process, JDK {@link
 * HttpServer}-backed) S3-API-compliant {@link S3HttpHandler} -- the exact same seam {@code
 * S3BlobStoreRepositoryTests} (that plugin's own equivalent proof) uses, just driving this
 * plugin's own write&rarr;publish&rarr;read cycle instead of snapshot/restore. Deliberately
 * package-scoped to {@code org.opensearch.repositories.s3} (not {@code
 * org.opensearch.serverless.storage}, where every other test in this plugin lives): {@code
 * S3ClientSettings}' own client-configuration settings (endpoint override, region, throttle-retry
 * toggle, ...) are package-private in {@code repository-s3} -- true production settings a real
 * deployment configures via {@code opensearch.yml}/the keystore, never touched directly by Java
 * code outside that plugin, so this test reaches them the same way any other same-package test
 * fixture would, rather than resorting to fragile raw setting-key string literals.
 *
 * <p>{@code @ThreadLeakFilters}, matching {@code S3BlobStoreRepositoryTests}' own {@code
 * EventLoopThreadFilter} annotation (a class in {@code repository-s3}'s own {@code src/test},
 * not reachable from a different project's {@code internalClusterTest} sourceSet, so this class's
 * own {@link EventLoopThreadFilter} is a local copy, not a reuse of that one): confirmed the hard
 * way, not copied on faith -- an earlier version of this test without it hit a real, reproducible
 * {@code ThreadLeakError} on an {@code AwsEventLoop} thread the AWS CRT client's own native
 * event-loop group spins up, a known, upstream-tracked
 * (github.com/awslabs/aws-crt-java/issues/905) leak with no clean fix available, not something
 * either plugin's own code can close correctly.
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
@ThreadLeakFilters(filters = ServerlessStorageS3FixtureIT.EventLoopThreadFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageS3FixtureIT extends ServerlessStorageIntegTestCase {

    /** Local copy of {@code repository-s3}'s own same-named, same-purpose class -- see this outer class's own javadoc for why. */
    public static class EventLoopThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("AwsEventLoop");
        }
    }

    private static final String CLIENT_NAME = "test";
    private static final String BUCKET = "bucket";
    private static final String REPO_NAME = "s3-fixture-it-repo";
    private static final String IDX = "s3-fixture-it-idx";

    private static HttpServer httpServer;
    private String previousOpenSearchPathConf;

    @org.junit.BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.createContext("/" + BUCKET, new S3HttpHandler(BUCKET));
        httpServer.start();
    }

    @org.junit.AfterClass
    public static void stopHttpServer() {
        httpServer.stop(0);
        httpServer = null;
    }

    /**
     * {@code S3Service#setDefaultAwsProfilePath} needs {@code opensearch.path.conf} set (via
     * {@code System.setProperty}, itself gated behind a security-manager {@code doPrivileged} call
     * that otherwise NPEs when the property was never set) -- the same setup {@code
     * S3BlobStoreRepositoryTests} already needs for exactly this reason, confirmed here the hard
     * way: an earlier version of this test omitted it and hit a real {@code
     * RepositoryVerificationException}/{@code NullPointerException} the moment repository
     * verification tried to write through a real {@code S3BlobContainer}.
     */
    @Override
    public void setUp() throws Exception {
        previousOpenSearchPathConf = org.opensearch.secure_sm.AccessController.doPrivileged(
            () -> System.setProperty("opensearch.path.conf", "config")
        );
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        if (previousOpenSearchPathConf != null) {
            org.opensearch.secure_sm.AccessController.doPrivileged(
                () -> System.setProperty("opensearch.path.conf", previousOpenSearchPathConf)
            );
        } else {
            org.opensearch.secure_sm.AccessController.doPrivileged(() -> System.clearProperty("opensearch.path.conf"));
        }
        super.tearDown();
    }

    private static String httpServerUrl() {
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class, TestS3RepositoryPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(S3ClientSettings.ACCESS_KEY_SETTING.getConcreteSettingForNamespace(CLIENT_NAME).getKey(), "access");
        secureSettings.setString(
            S3ClientSettings.SECRET_KEY_SETTING.getConcreteSettingForNamespace(CLIENT_NAME).getKey(),
            "secret_password"
        );

        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey(), REPO_NAME)
            .put(S3ClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace(CLIENT_NAME).getKey(), httpServerUrl())
            // Real S3 requires HTTPS in production; the fixture is a plain in-process HTTP server,
            // same reasoning S3BlobStoreRepositoryTests' own nodeSettings already established.
            .put(S3ClientSettings.USE_THROTTLE_RETRIES_SETTING.getConcreteSettingForNamespace(CLIENT_NAME).getKey(), false)
            .put(S3ClientSettings.PROXY_TYPE_SETTING.getConcreteSettingForNamespace(CLIENT_NAME).getKey(), ProxySettings.ProxyType.DIRECT)
            .put(S3ClientSettings.REGION.getConcreteSettingForNamespace(CLIENT_NAME).getKey(), "test-region")
            .setSecureSettings(secureSettings)
            .build();
    }

    public void testWriteThenPublishThenReadAgainstARealS3ApiSurface() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        assertTrue(
            "repository registration must be acknowledged before it's usable",
            client().admin()
                .cluster()
                .preparePutRepository(REPO_NAME)
                .setType(S3Repository.TYPE)
                .setSettings(
                    Settings.builder()
                        .put(S3Repository.BUCKET_SETTING.getKey(), BUCKET)
                        .put(S3Repository.CLIENT_NAME.getKey(), CLIENT_NAME)
                        .build()
                )
                .get()
                .isAcknowledged()
        );

        createIndex(
            IDX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(IDX);

        client().prepareIndex(IDX).setId("1").setSource("field", "value1").get();
        // Publishing (rfc-serverless-opensearch.md §7.1) only happens on flush -- this is the real
        // bundle-upload + manifest-write pair actually exercising the fixture's S3 HTTP handler,
        // not just repository registration.
        client().admin().indices().prepareFlush(IDX).get();
        refresh(IDX);

        // The doc must be genuinely searchable back out of a real S3 API surface, not just written somewhere.
        assertHitCount(client().prepareSearch(IDX).setSize(0).get(), 1);

        // A second document, on top of the first (not a fresh index), proves this isn't merely a
        // one-shot write -- the reader materializes an already-populated real S3 bucket cleanly on
        // the very next generation too.
        client().prepareIndex(IDX).setId("2").setSource("field", "value2").get();
        client().admin().indices().prepareFlush(IDX).get();
        refresh(IDX);
        assertHitCount(client().prepareSearch(IDX).setSize(0).get(), 2);
    }

    /**
     * The plain, real production {@link S3RepositoryPlugin} on its own leaks a background AWS SDK
     * event-loop thread in this test harness -- confirmed the hard way, not assumed: an earlier
     * version of this test registered {@code S3RepositoryPlugin} directly and hit a real {@code
     * ThreadLeakError} ("AwsEventLoop"). Root cause: {@code S3Service#close}/{@code
     * S3AsyncService#close} only call {@code releaseCachedClients()}, never touching the
     * package-private {@code clientExecutorService} each one also owns -- a genuine, if minor,
     * production resource-cleanup gap in {@code repository-s3} itself, not something specific to
     * this plugin. {@code repository-s3}'s own {@code S3BlobStoreRepositoryTests} independently
     * needed the exact same workaround (its own {@code TestS3RepositoryPlugin}), confirming this
     * isn't a one-off: it explicitly terminates both services' own executor after the plugin's
     * normal {@link #close()}. This subclass does the same, minus the async-transfer-manager
     * overrides that test's own version layers on top for unrelated reasons this test doesn't need.
     */
    public static class TestS3RepositoryPlugin extends S3RepositoryPlugin {

        public TestS3RepositoryPlugin(final Settings settings, final Path configPath) {
            super(
                settings,
                configPath,
                new S3Service(configPath, Executors.newSingleThreadScheduledExecutor()),
                new S3AsyncService(configPath, Executors.newSingleThreadScheduledExecutor())
            );
        }

        @Override
        public void close() throws IOException {
            super.close();
            Stream.of(service.getClientExecutorService(), s3AsyncService.getClientExecutorService())
                .forEach(e -> assertTrue(e == null || org.opensearch.threadpool.ThreadPool.terminate(e, 5, TimeUnit.SECONDS)));
        }
    }
}
