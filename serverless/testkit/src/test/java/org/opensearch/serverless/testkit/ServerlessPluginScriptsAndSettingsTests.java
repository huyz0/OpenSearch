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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptType;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * What k-NN needed from the shell beyond the client: its script language and contexts, its own index settings,
 * and a route the shell itself refuses. Each is pinned by a plugin written the way k-NN and the painless module
 * are.
 */
public class ServerlessPluginScriptsAndSettingsTests extends OpenSearchTestCase {

    static final Setting<Boolean> TOY_INDEX_SETTING = Setting.boolSetting(
        "index.toy.enabled",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /** A script whose result is its own source. */
    public abstract static class ToyScript {
        public static final String[] PARAMETERS = {};

        public abstract Object execute();

        public interface Factory {
            ToyScript newInstance();
        }

        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("toy", Factory.class);
    }

    static final class ToyEngine implements ScriptEngine {
        @Override
        public String getType() {
            return "toy";
        }

        @Override
        public <T> T compile(String name, String code, ScriptContext<T> context, Map<String, String> params) {
            final ToyScript.Factory factory = () -> new ToyScript() {
                @Override
                public Object execute() {
                    return code;
                }
            };
            return context.factoryClazz.cast(factory);
        }

        @Override
        public Set<ScriptContext<?>> getSupportedContexts() {
            return Set.of(ToyScript.CONTEXT);
        }
    }

    /** A plugin with a script language and context, an index setting, and a route the shell refuses. */
    public static final class ToyPlugin extends Plugin implements ScriptPlugin, ActionPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ToyEngine();
        }

        @Override
        public List<ScriptContext<?>> getContexts() {
            return List.of(ToyScript.CONTEXT);
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(TOY_INDEX_SETTING);
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
            return List.of(new RestHandler() {
                @Override
                public List<Route> routes() {
                    return List.of(new Route(org.opensearch.rest.RestRequest.Method.GET, "/_cat"));
                }

                @Override
                public void handleRequest(
                    org.opensearch.rest.RestRequest request,
                    org.opensearch.rest.RestChannel channel,
                    org.opensearch.transport.client.node.NodeClient client
                ) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "served by the plugin"));
                }
            });
        }
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-plugin-scripts")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane plane() throws java.io.IOException {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), System::currentTimeMillis, 30_000L);
    }

    /** The plugin's language compiles in its own context, and the built-in languages are still there. */
    public void testAPluginsScriptLanguageAndContextAreRegistered() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("scripts"), List.of(new ToyPlugin()))) {
            node.startup().start();
            node.setMetadataPlane(plane());
            final var scripts = node.services().scriptService();
            final ToyScript.Factory factory = scripts.compile(new Script(ScriptType.INLINE, "toy", "hello", Map.of()), ToyScript.CONTEXT);
            assertEquals("hello", factory.newInstance().execute());
            assertTrue("painless is still built in", scripts.isLangSupported("painless"));
            assertTrue("mustache is still built in", scripts.isLangSupported("mustache"));
        }
    }

    /** A route the shell refuses is served by the plugin that registers it, and the node starts. */
    public void testAPluginRouteWinsOverARefusal() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("routes"), List.of(new ToyPlugin()))) {
            node.startup().start();
            final Response served = send(node, "GET", "/_cat", null);
            assertEquals(served.body(), 200, served.status());
            assertEquals("served by the plugin", served.body());
        }
    }

    /** The plugin's index setting is accepted at creation and on update; a misspelt one is still refused. */
    public void testAPluginsIndexSettingIsAccepted() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("index-settings"), List.of(new ToyPlugin()))) {
            node.startup().start();
            node.setMetadataPlane(plane());
            final Response created = send(node, "PUT", "/toys", "{\"settings\":{\"index.toy.enabled\":true}}");
            assertEquals(created.body(), 200, created.status());
            final Response updated = send(node, "PUT", "/toys/_settings", "{\"index.toy.enabled\":false}");
            assertEquals(updated.body(), 200, updated.status());
            final Response misspelt = send(node, "PUT", "/toys/_settings", "{\"index.toyy.enabled\":false}");
            assertEquals(misspelt.body(), 400, misspelt.status());
            assertTrue(misspelt.body(), misspelt.body().contains("index.toyy.enabled"));
        }
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
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
