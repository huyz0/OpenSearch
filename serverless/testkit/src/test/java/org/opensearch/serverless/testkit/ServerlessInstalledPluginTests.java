/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import javax.tools.JavaCompiler;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

/**
 * A plugin installed on disk, the way an operator installs one.
 *
 * <p>Everything up to here handed the node plugin <em>instances</em>. That proved the seams and left the
 * question that actually decides whether the shell can host the OpenSearch ecosystem: can it take a
 * directory under {@code plugins/} containing a {@code plugin-descriptor.properties} and a jar, and run
 * what is in it? A plugin nobody can install is a plugin nobody has.
 *
 * <p><b>The plugin under test is compiled by the test, into a jar, from source.</b> That is not
 * theatre — it is the only way the assertion means anything. A class already on the test classpath would
 * be found by the parent classloader whether or not the shell built a classloader of its own, so the test
 * would pass with the loading removed. This class exists nowhere until the test writes it, and the node
 * can only reach it through the loader.
 *
 * <p><b>D5:</b> no object store is involved.
 */
/*
 * ExtrasFS, the test framework's mock filesystem, plants a stray entry in every directory it creates --
 * including this test's plugins/ directory. Core then finds a directory there with no descriptor and
 * refuses to start, which is exactly the behaviour testAnIncompleteInstallationIsRefused asserts and
 * exactly what we do not want randomly injected. Core's own PluginsServiceTests suppresses it for the
 * same reason.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class ServerlessInstalledPluginTests extends OpenSearchTestCase {

    private static final String PLUGIN_PACKAGE = "org.opensearch.serverless.installed";
    private static final String PLUGIN_CLASS = PLUGIN_PACKAGE + ".GreetingPlugin";

    /**
     * The plugin's source.
     *
     * <p>It uses two of the three hooks, and the interesting one is {@code createComponents}: the client it
     * is handed was constructed by classes its own classloader cannot see, so a client that arrives usable
     * across that boundary is the thing worth proving. The route reports what it got.
     */
    private static final String SOURCE = """
        package org.opensearch.serverless.installed;

        import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
        import org.opensearch.cluster.node.DiscoveryNodes;
        import org.opensearch.cluster.service.ClusterService;
        import org.opensearch.common.settings.ClusterSettings;
        import org.opensearch.common.settings.IndexScopedSettings;
        import org.opensearch.common.settings.Settings;
        import org.opensearch.common.settings.SettingsFilter;
        import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
        import org.opensearch.core.rest.RestStatus;
        import org.opensearch.core.xcontent.NamedXContentRegistry;
        import org.opensearch.env.Environment;
        import org.opensearch.env.NodeEnvironment;
        import org.opensearch.indices.SystemIndexDescriptor;
        import org.opensearch.plugins.Plugin;
        import org.opensearch.plugins.SystemIndexPlugin;
        import org.opensearch.repositories.RepositoriesService;
        import org.opensearch.rest.BytesRestResponse;
        import org.opensearch.rest.RestChannel;
        import org.opensearch.rest.RestHandler;
        import org.opensearch.rest.RestRequest;
        import org.opensearch.script.ScriptService;
        import org.opensearch.threadpool.ThreadPool;
        import org.opensearch.transport.client.Client;
        import org.opensearch.transport.client.node.NodeClient;
        import org.opensearch.watcher.ResourceWatcherService;

        import java.util.Collection;
        import java.util.List;
        import java.util.function.Supplier;

        public class GreetingPlugin extends Plugin implements SystemIndexPlugin {

            private volatile String got = "nothing";

            public GreetingPlugin(Settings settings) {
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
                got = (client == null ? "no-client" : "client") + "/" + (threadPool == null ? "no-pool" : "pool");
                return List.of(new Object());
            }

            @Override
            public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
                return List.of(new SystemIndexDescriptor(".greeting_state", "an installed plugin's own index"));
            }

            @Override
            public List<RestHandler> getRestHandlers(
                Settings settings,
                org.opensearch.rest.RestController restController,
                ClusterSettings clusterSettings,
                IndexScopedSettings indexScopedSettings,
                SettingsFilter settingsFilter,
                IndexNameExpressionResolver indexNameExpressionResolver,
                Supplier<DiscoveryNodes> nodesInCluster
            ) {
                return List.of(new RestHandler() {
                    @Override
                    public List<Route> routes() {
                        return List.of(new Route(RestRequest.Method.GET, "/_installed/hello"));
                    }

                    @Override
                    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                        channel.sendResponse(
                            new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, "installed:" + got)
                        );
                    }
                });
            }
        }
        """;

    private static final String QUERY_PLUGIN_CLASS = PLUGIN_PACKAGE + ".NothingQueryPlugin";

    /**
     * A plugin that registers a query.
     *
     * <p>A custom query is the one {@code SearchPlugin} contribution the shell's search surface can
     * actually reach: aggregations, suggesters, highlighting and rescoring are all refused with 501, so a
     * plugin offering one of those would be wired correctly and still have nothing to show. This one is
     * deliberately trivial — it matches nothing — because the point is whether the shell parsed it, built
     * it and ran it, not what it computes.
     */
    private static final String QUERY_SOURCE = """
        package org.opensearch.serverless.installed;

        import org.apache.lucene.search.MatchNoDocsQuery;
        import org.apache.lucene.search.Query;
        import org.opensearch.common.settings.Settings;
        import org.opensearch.core.common.io.stream.StreamInput;
        import org.opensearch.core.common.io.stream.StreamOutput;
        import org.opensearch.core.xcontent.ToXContent;
        import org.opensearch.core.xcontent.XContentBuilder;
        import org.opensearch.core.xcontent.XContentParser;
        import org.opensearch.index.query.AbstractQueryBuilder;
        import org.opensearch.index.query.QueryShardContext;
        import org.opensearch.plugins.Plugin;
        import org.opensearch.plugins.SearchPlugin;

        import java.io.IOException;
        import java.util.List;

        public class NothingQueryPlugin extends Plugin implements SearchPlugin {

            public NothingQueryPlugin(Settings settings) {
            }

            @Override
            public List<QuerySpec<?>> getQueries() {
                return List.of(
                    new QuerySpec<>(NothingQueryBuilder.NAME, NothingQueryBuilder::new, NothingQueryBuilder::fromXContent)
                );
            }

            public static class NothingQueryBuilder extends AbstractQueryBuilder<NothingQueryBuilder> {

                public static final String NAME = "matches_nothing";

                public NothingQueryBuilder() {
                }

                public NothingQueryBuilder(StreamInput in) throws IOException {
                    super(in);
                }

                @Override
                protected void doWriteTo(StreamOutput out) throws IOException {
                }

                @Override
                protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
                    builder.startObject(NAME);
                    printBoostAndQueryName(builder);
                    builder.endObject();
                }

                public static NothingQueryBuilder fromXContent(XContentParser parser) throws IOException {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        // a query with no options; anything inside is ignored
                    }
                    return new NothingQueryBuilder();
                }

                @Override
                protected Query doToQuery(QueryShardContext context) throws IOException {
                    return new MatchNoDocsQuery();
                }

                @Override
                protected boolean doEquals(NothingQueryBuilder other) {
                    return true;
                }

                @Override
                protected int doHashCode() {
                    return 0;
                }

                @Override
                public String getWriteableName() {
                    return NAME;
                }
            }
        }
        """;

    private Settings nodeSettings(String name, Path home) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-installed")
            .put("path.home", home)
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** A directory under {@code plugins/}, a descriptor, a jar, and the plugin runs. */
    public void testAPluginInstalledOnDiskIsLoadedAndRun() throws Exception {
        final Path home = createTempDir();
        install(home, "greeting", Version.CURRENT.toString());

        try (ServerlessNode node = new ServerlessNode(nodeSettings("installed-ok", home))) {
            node.start();
            assertEquals("the installed plugin must be loaded", 1, node.plugins().plugins().size());

            final var plugin = node.plugins().plugins().get(0);
            assertEquals(PLUGIN_CLASS, plugin.getClass().getName());
            // The thing that makes the rest of this test mean something: the class did not come from here.
            assertNotSame(
                "an installed plugin must be loaded through its own classloader, not found on the node's classpath",
                getClass().getClassLoader(),
                plugin.getClass().getClassLoader()
            );
            expectThrows(ClassNotFoundException.class, () -> Class.forName(PLUGIN_CLASS));

            final Response hello = send(node, "GET", "/_installed/hello");
            assertEquals("its route must be served: " + hello.body(), 200, hello.status());
            // A client and a thread pool built by classes the plugin's loader cannot see, arriving usable.
            assertEquals("installed:client/pool", hello.body());

            // And what it declared as its own is out of reach from the request path, exactly as it is for a
            // plugin the shell was handed directly.
            assertEquals(403, send(node, "GET", "/.greeting_state/_search?q=*:*").status());
        }
    }

    /**
     * A plugin built for a different OpenSearch stops the node.
     *
     * <p>Core's rule, and worth a test here because the shell could have got the loading right and the
     * verification wrong. A plugin compiled against a different version is a plugin that will fail
     * somewhere less convenient than startup.
     */
    public void testAPluginBuiltForAnotherVersionIsRefused() throws Exception {
        final Path home = createTempDir();
        install(home, "greeting", "1.2.3");

        final var failure = expectThrows(Exception.class, () -> new ServerlessNode(nodeSettings("installed-old", home)).close());
        final String message = rootMessage(failure);
        assertTrue("the refusal must name the version mismatch: " + message, message.contains("1.2.3"));
    }

    /** A directory under {@code plugins/} with no descriptor stops the node rather than being ignored. */
    public void testAnIncompleteInstallationIsRefused() throws Exception {
        final Path home = createTempDir();
        Files.createDirectories(home.resolve("plugins").resolve("half-installed"));

        final var failure = expectThrows(Exception.class, () -> new ServerlessNode(nodeSettings("installed-half", home)).close());
        final String message = rootMessage(failure);
        assertTrue(
            "the refusal must point at the directory that is not a plugin: " + message,
            message.contains("half-installed") || message.contains("plugin-descriptor.properties")
        );
    }

    /**
     * An installed search plugin's query is parsed, built and run.
     *
     * <p>The third extension point, and the one where being wired wrongly would be hardest to notice: a
     * custom query has to reach two separate registries — the {@code NamedXContentRegistry} that parses a
     * search body and the {@code SearchModule} that owns the query itself — and either could have been
     * left with an empty plugin list independently of the other.
     *
     * <p>The query matches nothing on purpose. Against an index holding one document, {@code match_all}
     * finds it and this finds none: the difference is the plugin's code running, and no wiring accident
     * produces it.
     */
    public void testAnInstalledSearchPluginsQueryIsParsedAndRun() throws Exception {
        final Path home = createTempDir();
        install(home, "nothing-query", Version.CURRENT.toString(), QUERY_SOURCE, QUERY_PLUGIN_CLASS);

        final java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong(1_000L);
        final org.opensearch.serverless.metadata.MetadataPlane plane = new org.opensearch.serverless.metadata.MetadataPlane(
            new org.opensearch.common.blobstore.fs.FsBlobStore(1024, createTempDir(), false),
            org.opensearch.common.blobstore.BlobPath.cleanPath(),
            clock::get,
            30_000L
        );
        plane.createIndex(
            new org.opensearch.serverless.cluster.IndexDescriptor(
                "docs",
                "uuid-docs-0000000000",
                1,
                "{\"properties\":{\"msg\":{\"type\":\"text\"}}}",
                null
            )
        );

        try (ServerlessNode node = new ServerlessNode(nodeSettings("query-plugin", home))) {
            node.start();
            node.setMetadataPlane(plane);
            final var loop = new org.opensearch.serverless.reconcile.BackgroundReconciler(node, plane);
            loop.want("docs", 0);
            loop.tick(clock.get());

            assertEquals(201, send(node, "PUT", "/docs/_doc/1?refresh=true", "{\"msg\":\"here\"}").status());
            assertTrue(
                "the document must be there for the comparison to mean anything",
                send(node, "POST", "/docs/_search", "{\"query\":{\"match_all\":{}}}").body().contains("\"value\":1")
            );

            final Response custom = send(node, "POST", "/docs/_search", "{\"query\":{\"matches_nothing\":{}}}");
            assertEquals("a plugin's query must be parsed rather than rejected: " + custom.body(), 200, custom.status());
            assertTrue("and must actually run: " + custom.body(), custom.body().contains("\"value\":0"));
        }

        // The control: without the plugin the same body is not a query this node knows.
        try (ServerlessNode bare = new ServerlessNode(nodeSettings("query-none", createTempDir()))) {
            bare.start();
            bare.setMetadataPlane(plane);
            final Response rejected = send(bare, "POST", "/docs/_search", "{\"query\":{\"matches_nothing\":{}}}");
            assertEquals("without the plugin the query must be refused: " + rejected.body(), 400, rejected.status());
        }
    }

    /** A node whose home has no plugins directory at all is unaffected, which is nearly every node. */
    public void testANodeWithNoPluginsDirectoryIsUnchanged() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("installed-none", createTempDir()))) {
            node.start();
            assertTrue(node.plugins().plugins().isEmpty());
            assertEquals(200, send(node, "GET", "/").status());
        }
    }

    private static String rootMessage(Throwable e) {
        Throwable cause = e;
        final StringBuilder all = new StringBuilder();
        while (cause != null) {
            all.append(cause).append(" | ");
            cause = cause.getCause() == cause ? null : cause.getCause();
        }
        return all.toString();
    }

    /** Writes a complete installation: a directory, a descriptor, and a jar built from {@link #SOURCE}. */
    private void install(Path home, String name, String opensearchVersion) throws Exception {
        install(home, name, opensearchVersion, SOURCE, PLUGIN_CLASS);
    }

    private void install(Path home, String name, String opensearchVersion, String source, String className) throws Exception {
        final Path pluginDir = home.resolve("plugins").resolve(name);
        Files.createDirectories(pluginDir);
        Files.writeString(
            pluginDir.resolve("plugin-descriptor.properties"),
            String.join(
                "\n",
                "name=" + name,
                "description=a plugin the test compiled",
                "version=1.0.0",
                "opensearch.version=" + opensearchVersion,
                "java.version=" + System.getProperty("java.specification.version"),
                "classname=" + className
            ),
            StandardCharsets.UTF_8
        );
        jar(compile(source, className), pluginDir.resolve(name + ".jar"));
    }

    /** Compiles {@link #SOURCE} against this JVM's classpath and returns the directory of class files. */
    private Path compile(String source, String className) throws IOException {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull("this test needs a JDK, not a JRE: no system java compiler is available", compiler);

        final Path classes = createTempDir();
        try (StandardJavaFileManager files = compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8)) {
            files.setLocation(StandardLocation.CLASS_OUTPUT, List.of(classes.toFile()));
            final var unit = new SimpleJavaFileObject(
                URI.create("string:///" + className.substring(className.lastIndexOf('.') + 1) + ".java"),
                javax.tools.JavaFileObject.Kind.SOURCE
            ) {
                @Override
                public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                    return source;
                }
            };
            final var task = compiler.getTask(
                null,
                files,
                diagnostic -> logger.warn("compiling the test plugin: {}", diagnostic),
                List.of("-classpath", System.getProperty("java.class.path")),
                null,
                List.of(unit)
            );
            assertTrue("the test plugin must compile", task.call());
        }
        return classes;
    }

    /** Packs a directory of class files into a jar, preserving package directories as entry names. */
    private static void jar(Path classes, Path target) throws IOException {
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(target)); Stream<Path> walk = Files.walk(classes)) {
            for (Path file : walk.filter(Files::isRegularFile).toList()) {
                out.putNextEntry(new JarEntry(classes.relativize(file).toString().replace('\\', '/')));
                out.write(Files.readAllBytes(file));
                out.closeEntry();
            }
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path) throws Exception {
        return send(node, method, path, null);
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
