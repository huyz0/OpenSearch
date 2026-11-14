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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A plugin's dynamic setting changed through {@code /_cluster/settings} reaches the running plugin: on the node that served the
 * write when it returns, on the other nodes soon after, and back to the default when removed.
 */
public class ServerlessPluginClusterSettingsTests extends OpenSearchTestCase {

    static final Setting<Boolean> ONLY_ON_ML = Setting.boolSetting(
        "plugins.probe.only_run_on_ml_node",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Keeps the value its consumer was last given, as plugins do. */
    public static final class DynamicPlugin extends Plugin {
        volatile boolean onlyOnMl = true;
        volatile ServerlessNode node;

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(ONLY_ON_ML);
        }

        void register(ServerlessNode on) {
            on.clusterService().getClusterSettings().addSettingsUpdateConsumer(ONLY_ON_ML, value -> onlyOnMl = value);
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(TransportAddress address, String method, String path, String body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-plugin-cluster-settings")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.plugin_cluster_settings.refresh_interval", "100ms")
            .put(extra)
            .build();
    }

    private ServerlessNode started(String name, MetadataPlane plane, DynamicPlugin plugin, Settings extra) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name, extra), List.of(plugin));
        plugin.register(node);
        node.startup().start();
        node.setMetadataPlane(plane);
        return node;
    }

    public void testAppliedHereAtOnceElsewhereSoonAndRevertedOnRemoval() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(1024, createTempDir(), false),
            BlobPath.cleanPath(),
            clock::get,
            30_000L
        );
        final DynamicPlugin onA = new DynamicPlugin();
        final DynamicPlugin onB = new DynamicPlugin();
        try (
            ServerlessNode a = started("settings-a", plane, onA, Settings.EMPTY);
            ServerlessNode b = started("settings-b", plane, onB, Settings.EMPTY)
        ) {
            final TransportAddress http = a.boundHttpAddress().publishAddress();
            final Response refused = send(
                http,
                "PUT",
                "/_cluster/settings",
                "{\"persistent\":{\"plugins.probe.only_run_on_ml_node\":\"maybe\"}}"
            );
            assertEquals(refused.body(), 400, refused.status());
            assertTrue("a refused value is not stored", send(http, "GET", "/_cluster/settings", null).body().contains("maybe") == false);

            final Response set = send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"plugins.probe.only_run_on_ml_node\":false}}");
            assertEquals(set.body(), 200, set.status());
            assertFalse("the serving node's plugin sees the change when the write returns", onA.onlyOnMl);
            assertBusy(() -> assertFalse("the other node's plugin follows", onB.onlyOnMl), 30, TimeUnit.SECONDS);

            assertEquals(
                200,
                send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"plugins.probe.only_run_on_ml_node\":null}}").status()
            );
            assertTrue("a removed setting returns to its default", onA.onlyOnMl);
            assertBusy(() -> assertTrue(onB.onlyOnMl), 30, TimeUnit.SECONDS);
        }
    }

    public void testTheSwitchLeavesPluginsAsTheyStarted() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(1024, createTempDir(), false),
            BlobPath.cleanPath(),
            clock::get,
            30_000L
        );
        final DynamicPlugin plugin = new DynamicPlugin();
        try (
            ServerlessNode node = started(
                "settings-off",
                plane,
                plugin,
                Settings.builder().put("serverless.plugin_cluster_settings.enabled", false).build()
            )
        ) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            assertEquals(
                200,
                send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"plugins.probe.only_run_on_ml_node\":false}}").status()
            );
            assertTrue("stored, and not applied", plugin.onlyOnMl);
        }
    }
}
