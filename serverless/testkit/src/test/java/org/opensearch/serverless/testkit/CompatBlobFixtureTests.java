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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Writes the indices of the compatibility fixtures through the shell, so a reader of the shell's blobs can serve the
 * fixtures' searches from what the shell published. Each fixture's {@code PUT /<index>} step gives the mapping and
 * each {@code _bulk} step the documents, indexed in order with their explicit ids; every index is published once, as
 * segment blobs, and the descriptor, commit and segment blobs are copied to {@code tests.compat_blobs.out}. Skipped
 * unless {@code tests.compat_blobs.fixtures} (a directory of fixture files) and that property are set.
 */
public class CompatBlobFixtureTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private record Doc(String id, String source) {}

    private record Index(String name, String mapping, List<Doc> docs) {}

    private static String json(Object value) throws Exception {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.value(value);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    @SuppressWarnings("unchecked")
    private static Index read(Path file) throws Exception {
        final Map<String, Object> fixture;
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                Files.newInputStream(file)
            )
        ) {
            fixture = parser.mapOrdered();
        }
        String name = null;
        String mapping = null;
        final List<Doc> docs = new ArrayList<>();
        for (Object s : (List<Object>) fixture.get("steps")) {
            final Map<String, Object> request = (Map<String, Object>) ((Map<String, Object>) s).get("request");
            final String method = (String) request.get("method");
            final String path = (String) request.get("path");
            if ("PUT".equals(method) && path.indexOf('/', 1) < 0) {
                name = path.substring(1);
                final Map<String, Object> body = (Map<String, Object>) request.get("body");
                mapping = json(body.get("mappings"));
            } else if ("/_bulk".equals(path)) {
                final List<Object> lines = (List<Object>) request.get("ndjson");
                for (int i = 0; i + 1 < lines.size(); i += 2) {
                    final Map<String, Object> action = (Map<String, Object>) ((Map<String, Object>) lines.get(i)).get("index");
                    docs.add(new Doc((String) action.get("_id"), json(lines.get(i + 1))));
                }
            }
        }
        return name == null ? null : new Index(name, mapping, docs);
    }

    public void testWriteCompatIndices() throws Exception {
        final String fixtures = System.getProperty("tests.compat_blobs.fixtures");
        final String out = System.getProperty("tests.compat_blobs.out");
        assumeTrue("set tests.compat_blobs.fixtures and tests.compat_blobs.out", fixtures != null && out != null);
        final List<Index> indices = new ArrayList<>();
        try (Stream<Path> files = Files.list(Path.of(fixtures))) {
            for (Path file : (Iterable<Path>) files.filter(f -> f.toString().endsWith(".json")).sorted()::iterator) {
                final Index index = read(file);
                if (index != null && index.docs().isEmpty() == false) {
                    indices.add(index);
                }
            }
        }
        final Path root = createTempDir();
        final FsBlobStore store = new FsBlobStore(8192, root, false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
        for (Index index : indices) {
            plane.createIndex(new IndexDescriptor(index.name(), index.name() + "-uuid", 1, index.mapping(), null));
        }
        final Settings settings = Settings.builder()
            .put("node.name", "compat-fixtures")
            .put("cluster.name", "compat-blob-fixtures")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.segments.node_pack.enabled", false)
            .put("serverless.wal.node_log.window_millis", 20)
            .build();
        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (Index index : indices) {
                loop.want(index.name(), 0);
            }
            loop.tick(System.currentTimeMillis());
            for (Index index : indices) {
                final ShardId shardId = node.reconciler()
                    .openShards()
                    .stream()
                    .filter(s -> s.getIndexName().equals(index.name()))
                    .findFirst()
                    .orElseThrow();
                for (Doc doc : index.docs()) {
                    node.writes().index(shardId, doc.id(), doc.source());
                }
            }
            loop.publishAll();
        }
        final Path target = Path.of(out);
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path file : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
                final String relative = root.relativize(file).toString().replace('\\', '/');
                if ((relative.startsWith("indices/") || relative.startsWith("segments/")) && relative.contains("extra") == false) {
                    final Path copy = target.resolve(relative);
                    Files.createDirectories(copy.getParent());
                    Files.copy(file, copy, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }
}
