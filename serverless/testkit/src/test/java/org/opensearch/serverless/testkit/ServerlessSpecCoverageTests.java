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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Every path in the REST API spec is served, or refused with a reason. Never core's default.
 *
 * <p>D2 says an endpoint this design does not implement answers 501 with a reason. That was checked by
 * hand, endpoint by endpoint, and each time a new one was checked another sibling of it turned up
 * answering core's own {@code 400 no handler found for uri} -- the index-less form, the {@code {name}}
 * form, the {@code {metric}} form. M50 found twenty-two by driving a node by hand; the audit after M60
 * found forty-one more the same way, and one of them ({@code POST /_index_template/_simulate}) was not
 * merely unrouted but misrouted onto a placeholder, storing a live template.
 *
 * <p>So this walks the spec itself -- copied onto the test classpath from {@code :rest-api-spec}, the
 * same files a client library is generated from -- and sends every non-deprecated method and path to a
 * running node. A served path answers whatever it answers; a refused one answers 501 with the shell's own
 * envelope. What none of them may answer is the two shapes a caller reads as "this server does not know
 * this URL at all": core's no-handler 400, and a 405 for a method the path was registered without.
 */
public class ServerlessSpecCoverageTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([^}]+)}");

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-spec")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** One method and path from the spec, with its placeholders filled in. */
    private record Call(String api, String method, String template, String path) {
    }

    public void testEverySpecPathIsServedOrRefusedWithAReason() throws Exception {
        final List<Call> calls = specCalls();
        assertTrue("the spec must be on the test classpath; see serverless/testkit/build.gradle", calls.size() > 300);

        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, "{\"properties\":{\"msg\":{\"type\":\"text\"}}}", null));
        final List<String> unanswered = new ArrayList<>();
        try (ServerlessNode node = new ServerlessNode(nodeSettings("spec-walk"))) {
            node.start();
            node.setMetadataPlane(plane);
            for (Call call : calls) {
                final Response answer = send(node, call.method(), call.path(), null);
                if (answer.status() == 405 || answer.body().contains("no handler found for uri")) {
                    unanswered.add(call.api() + ": " + call.method() + " " + call.template() + " -> " + answer.status());
                }
            }
        }
        assertTrue(
            "every spec path must be served or refused with a reason; these fell through to core's default:\n"
                + String.join("\n", unanswered),
            unanswered.isEmpty()
        );
    }

    /**
     * The spec, read from the classpath copy. Deprecated paths are skipped: a client generated from the
     * spec does not call them.
     */
    @SuppressWarnings("unchecked")
    private List<Call> specCalls() throws Exception {
        final URL directory = getClass().getClassLoader().getResource("rest-api-spec/api");
        assertNotNull("rest-api-spec/api must be on the test classpath", directory);
        final List<Call> calls = new ArrayList<>();
        final List<Path> files;
        try (Stream<Path> listing = Files.list(Path.of(directory.toURI()))) {
            files = listing.filter(p -> p.toString().endsWith(".json")).sorted().toList();
        }
        for (Path file : files) {
            final String api = file.getFileName().toString().replace(".json", "");
            if ("_common".equals(api)) {
                continue;
            }
            final Map<String, Object> parsed;
            try (InputStream in = Files.newInputStream(file)) {
                parsed = XContentHelper.convertToMap(new BytesArray(in.readAllBytes()), false, XContentType.JSON).v2();
            }
            final Map<String, Object> definition = (Map<String, Object>) parsed.get(api);
            final Map<String, Object> url = (Map<String, Object>) definition.get("url");
            for (Object each : (List<Object>) url.get("paths")) {
                final Map<String, Object> path = (Map<String, Object>) each;
                if (path.get("deprecated") != null) {
                    continue;
                }
                final String template = (String) path.get("path");
                for (Object method : (List<Object>) path.get("methods")) {
                    calls.add(new Call(api, (String) method, template, fill(template)));
                }
            }
        }
        return calls;
    }

    /** Placeholders filled with names a node will read as names, not as APIs. */
    private static String fill(String template) {
        final Matcher matcher = PLACEHOLDER.matcher(template);
        final StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            final String name = matcher.group(1);
            final String value = switch (name) {
                case "index", "alias", "target", "new_index" -> "alpha";
                case "metric", "index_metric" -> "os";
                case "id", "task_id", "node_id", "index_uuid", "snapshot", "target_snapshot", "repository", "name", "context", "block",
                    "fields", "thread_pool_patterns", "attribute", "awareness_attribute_name", "awareness_attribute_value", "shard_id" ->
                    "x1";
                default -> "x1";
            };
            matcher.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws IOException, InterruptedException {
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
