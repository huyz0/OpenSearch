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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.script.ScriptService;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * What the alerting and anomaly-detection plugins needed to create and run their monitors and detectors: their
 * named objects read from a REST body, a search of the system index they just created, and their lifecycle
 * components started. Each is pinned by a plugin written the way those are.
 */
public class ServerlessPluginMonitorsTests extends OpenSearchTestCase {

    /** A named object a plugin reads from a REST body, as alerting reads a monitor's inputs. */
    public record Input(String value) implements NamedWriteable {
        @Override
        public String getWriteableName() {
            return "test_input";
        }

        @Override
        public void writeTo(StreamOutput out) throws java.io.IOException {
            out.writeString(value);
        }

        static Input parse(XContentParser parser) throws java.io.IOException {
            parser.nextToken();
            final Input input = new Input(parser.text());
            parser.nextToken();
            return input;
        }
    }

    /** A component that does its work only once started, as alerting's monitor runner does. */
    public static final class Runner extends AbstractLifecycleComponent {
        volatile boolean running;

        @Override
        protected void doStart() {
            running = true;
        }

        @Override
        protected void doStop() {
            running = false;
        }

        @Override
        protected void doClose() {}
    }

    public static final class MonitorLikePlugin extends Plugin implements ActionPlugin {
        final Runner runner = new Runner();

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(new NamedXContentRegistry.Entry(Input.class, new ParseField("test_input"), Input::parse));
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(new NamedWriteableRegistry.Entry(Input.class, "test_input", in -> new Input(in.readString())));
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
            return List.of(runner);
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
                    return List.of(new Route(RestRequest.Method.POST, "/_test_monitors"));
                }

                @Override
                public void handleRequest(
                    RestRequest request,
                    org.opensearch.rest.RestChannel channel,
                    org.opensearch.transport.client.node.NodeClient client
                ) throws Exception {
                    try (XContentParser parser = request.contentParser()) {
                        parser.nextToken();
                        parser.nextToken();
                        final Input input = parser.namedObject(Input.class, parser.currentName(), null);
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, input.value()));
                    }
                }
            });
        }
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-plugin-monitors")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ServerlessBootstrap.STORE_PATH, createTempDir().toString())
            .build();
    }

    /** A plugin's named object is read from a REST body, and its named writeable is in the node's registry. */
    public void testAPluginsNamedObjectsAreKnown() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("named"), List.of(new MonitorLikePlugin()))) {
            node.startup().start();
            final Response parsed = send(node, "POST", "/_test_monitors", "{\"test_input\":\"read\"}");
            assertEquals(parsed.body(), 200, parsed.status());
            assertEquals("read", parsed.body());
            assertNotNull(node.services().namedWriteableRegistry().getReader(Input.class, "test_input"));
        }
    }

    /** A plugin's lifecycle component is started with the node and stopped with it. */
    public void testAPluginsLifecycleComponentRuns() throws Exception {
        final MonitorLikePlugin plugin = new MonitorLikePlugin();
        try (ServerlessNode node = new ServerlessNode(nodeSettings("lifecycle"), List.of(plugin))) {
            assertFalse(plugin.runner.running);
            node.startup().start();
            assertTrue("started with the node", plugin.runner.running);
        }
        assertFalse("stopped with the node", plugin.runner.running);
    }

    /** A search of an index nothing was ever written to answers empty, as core's does. */
    public void testASearchOfAnIndexNeverWrittenIsEmpty() throws Exception {
        try (ServerlessBootstrap boot = ServerlessBootstrap.start(nodeSettings("never-written"))) {
            assertEquals(200, send(boot.node(), "PUT", "/fresh", "{\"settings\":{\"index.number_of_shards\":2}}").status());
            final Response searched = send(boot.node(), "POST", "/fresh/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals(searched.body(), 200, searched.status());
            assertTrue(searched.body(), searched.body().contains("\"total\":{\"value\":0"));
            assertTrue(searched.body(), searched.body().contains("\"failed\":0"));

            // Once written, the shard is searched as usual.
            assertEquals(201, send(boot.node(), "PUT", "/fresh/_doc/1?refresh=true", "{\"msg\":\"m\"}").status());
            final Response found = send(boot.node(), "POST", "/fresh/_search", "{\"query\":{\"match_all\":{}}}");
            assertTrue(found.body(), found.body().contains("\"total\":{\"value\":1"));
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
