/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Heap per open index when many indices share one realistic mapping, the case shared schemas are for: a
 * multi-tenant fleet where most tenants' indices were created from the same template.
 *
 * <p>Opens {@code tests.schema.indices} single-shard writer indices (default 2,000) in one node, all with
 * the same mapping of about 150 fields, and logs the heap per index at checkpoints along the way, plus a
 * class histogram of the biggest consumers at the end. Run once with {@code tests.schema.mapping=trivial}
 * to separate what an open index costs whatever its mapping from what the mapping adds.
 *
 * <p>A measurement, not a regression check: it asserts only that the indices opened. Run it with
 * {@code ./gradlew :serverless:testkit:schemaHeapTest -Dtests.schema.indices=6000}. Each open writer shard holds three file
 * descriptors, so the count is bounded by the process's open-file limit.
 */
// Thousands of open shards hold three descriptors each, far past the test file system's handle limit.
@LuceneTestCase.SuppressFileSystems("*")
public class SharedSchemaHeapTests extends OpenSearchTestCase {

    private static final long TTL = 3_600_000L;

    /**
     * A mapping shaped like a typical application's: nested objects, text with keyword sub-fields, dates,
     * numbers, a nested type and a few dynamic templates. 150 leaf fields or so.
     */
    static String realisticMapping() {
        final StringBuilder b = new StringBuilder();
        b.append("{\"dynamic_templates\":[");
        b.append("{\"strings_as_keywords\":{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":\"keyword\",\"ignore_above\":256}}},");
        b.append("{\"longs\":{\"match_mapping_type\":\"long\",\"mapping\":{\"type\":\"long\"}}}");
        b.append("],\"properties\":{");
        b.append("\"@timestamp\":{\"type\":\"date\"},");
        b.append("\"message\":{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\",\"ignore_above\":1024}}},");
        final String[] groups = { "user", "host", "event", "http", "source", "destination", "service", "cloud", "process", "file" };
        for (int g = 0; g < groups.length; g++) {
            b.append('"').append(groups[g]).append("\":{\"properties\":{");
            for (int f = 0; f < 14; f++) {
                if (f > 0) {
                    b.append(',');
                }
                final String name = "f" + f;
                switch (f % 7) {
                    case 0 -> b.append('"').append(name).append("\":{\"type\":\"keyword\"}");
                    case 1 -> b.append('"')
                        .append(name)
                        .append("\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}");
                    case 2 -> b.append('"').append(name).append("\":{\"type\":\"long\"}");
                    case 3 -> b.append('"').append(name).append("\":{\"type\":\"date\"}");
                    case 4 -> b.append('"').append(name).append("\":{\"type\":\"boolean\"}");
                    case 5 -> b.append('"').append(name).append("\":{\"type\":\"ip\"}");
                    default -> b.append('"').append(name).append("\":{\"type\":\"double\"}");
                }
            }
            b.append("}}");
            b.append(',');
        }
        b.append("\"tags\":{\"type\":\"keyword\"},");
        b.append("\"items\":{\"type\":\"nested\",\"properties\":{\"sku\":{\"type\":\"keyword\"},\"qty\":{\"type\":\"integer\"},");
        b.append("\"price\":{\"type\":\"float\"},\"title\":{\"type\":\"text\"}}}");
        b.append("}}");
        return b.toString();
    }

    private static final String TRIVIAL_MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "shared-schema-heap")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.activation.max_shards", 100_000)
            .put(
                "serverless.schema.sharing.enabled",
                org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.schema.sharing", "false"))
            )
            .build();
    }

    private static long usedHeapBytes() {
        for (int i = 0; i < 3; i++) {
            System.gc();
        }
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        final Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * Has a share of the open indices add dynamic fields, then measures again. {@code tests.schema.drift} is the
     * share (0 to 1); {@code tests.schema.drift_mode} is {@code unique} (each drifting index adds fields no other
     * index has) or {@code common} (every drifting index adds the same three fields).
     */
    private void drift(ServerlessNode node, int indices, long baseline) throws Exception {
        final double share = Double.parseDouble(System.getProperty("tests.schema.drift", "0"));
        if (share <= 0) {
            return;
        }
        final String mode = System.getProperty("tests.schema.drift_mode", "unique");
        final boolean common = "common".equals(mode);
        // "none" writes the same documents with mapped fields only: the cost of a write alone, to subtract.
        final boolean none = "none".equals(mode);
        final int drifting = (int) Math.round(indices * share);
        final int port = node.boundHttpAddress().publishAddress().getPort();
        final long[] nanos = new long[drifting];
        try (var http = java.net.http.HttpClient.newHttpClient()) {
            for (int i = 1; i <= drifting; i++) {
                // Spread over the population rather than the first ones opened.
                final int index = 1 + (int) ((long) (i - 1) * indices / drifting);
                final String doc = none ? "{\"tags\":\"x\",\"user\":{\"f0\":\"x\",\"f2\":1,\"f4\":true}}"
                    : common ? "{\"extra\":{\"a\":\"x\",\"b\":1,\"c\":true}}"
                    : "{\"extra_" + index + "\":{\"a\":\"x\",\"b\":1,\"c\":true}}";
                final long sent = System.nanoTime();
                final var response = http.send(
                    java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:" + port + "/tenant-" + index + "/_doc"))
                        .header("Content-Type", "application/json")
                        .POST(java.net.http.HttpRequest.BodyPublishers.ofString(doc))
                        .build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString()
                );
                nanos[i - 1] = System.nanoTime() - sent;
                assertEquals(response.body(), 201, response.statusCode());
            }
        }
        java.util.Arrays.sort(nanos);
        logger.info(
            "schema heap: first write to each of {} indices, p50 {} ms, p99 {} ms",
            drifting,
            nanos[drifting / 2] / 1e6,
            nanos[Math.min(drifting - 1, (int) (drifting * 0.99))] / 1e6
        );
        final long used = usedHeapBytes();
        logger.info(
            "schema heap: after {} of {} indices drifted ({}), heap {} MB, {} KB/index overall",
            drifting,
            indices,
            mode,
            Math.round(used / 1_048_576.0),
            Math.round((used - baseline) / 1024.0 / indices)
        );
    }

    private static long openFileDescriptors() {
        try (var listing = java.nio.file.Files.list(org.opensearch.common.io.PathUtils.get("/proc/self/fd"))) {
            return listing.count();
        } catch (Exception e) {
            return -1;
        }
    }

    static String classHistogram(int lines) throws Exception {
        final Object out = ManagementFactory.getPlatformMBeanServer()
            .invoke(
                new ObjectName("com.sun.management:type=DiagnosticCommand"),
                "gcClassHistogram",
                new Object[] { new String[0] },
                new String[] { String[].class.getName() }
            );
        final String[] all = out.toString().split("\n");
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < Math.min(lines, all.length); i++) {
            b.append(all[i]).append('\n');
        }
        return b.toString();
    }

    public void testHeapPerOpenIndexWithOneSharedMapping() throws Exception {
        final int indices = Integer.getInteger("tests.schema.indices", 2_000);
        final boolean trivial = "trivial".equals(System.getProperty("tests.schema.mapping"));
        final String mapping = trivial ? TRIVIAL_MAPPING : realisticMapping();
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        logger.info(
            "schema heap: {} indices, mapping {} ({} bytes), sharing {}",
            indices,
            trivial ? "trivial" : "realistic",
            mapping.length(),
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.schema.sharing", "false"))
        );

        try (ServerlessNode node = new ServerlessNode(nodeSettings("schema-heap"))) {
            node.startup().start();
            node.setMetadataPlane(plane);

            // One warm-up index, so class loading is not billed to the first checkpoint.
            plane.createIndex(new IndexDescriptor("warmup", "uuid-warmup-0000000", 1, mapping, null));
            node.activation().activateWriter(plane, "warmup", 0).orElseThrow();

            final long baseline = usedHeapBytes();
            long previous = baseline;
            int previousCount = 0;
            final long started = System.nanoTime();
            for (int i = 1; i <= indices; i++) {
                final String name = "tenant-" + i;
                plane.createIndex(new IndexDescriptor(name, "uuid-" + name + "-0000", 1, mapping, null));
                node.activation().activateWriter(plane, name, 0).orElseThrow();
                if (i == 100 || i == 500 || i % 1_000 == 0 || i == indices) {
                    final long used = usedHeapBytes();
                    logger.info(
                        "schema heap: {} open indices, heap {} MB, {} KB/index overall, {} KB/index since last ({} s elapsed, {} fds)",
                        i,
                        Math.round(used / 1_048_576.0),
                        Math.round((used - baseline) / 1024.0 / i),
                        Math.round((used - previous) / 1024.0 / (i - previousCount)),
                        (System.nanoTime() - started) / 1_000_000_000L,
                        openFileDescriptors()
                    );
                    previous = used;
                    previousCount = i;
                }
            }
            assertEquals(indices + 1, node.reconciler().openShards().size());
            drift(node, indices, baseline);
            logger.info("schema heap: histogram after {} indices\n{}", indices, classHistogram(60));
        }
    }
}
