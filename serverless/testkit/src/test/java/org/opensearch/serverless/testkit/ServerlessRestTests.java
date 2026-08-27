/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Phase 1 completion: the shell binds transport and HTTP, and serves its allowlisted REST surface.
 *
 * <p>These go over real HTTP to a really-bound port rather than calling the handlers directly. A
 * handler that returns the right object while the transport fails to bind is not a working node, and
 * only the round trip distinguishes the two.
 */
public class ServerlessRestTests extends OpenSearchTestCase {

    private Settings httpNodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-rest-test")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")          // ephemeral: tests must not fight over ports
            .put("transport.port", "0")
            .build();
    }

    private static String get(TransportAddress address, String path) throws Exception {
        // Closed explicitly: the JDK client keeps worker threads alive, which the suite reports as a leak.
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() + " " + response.body();
        }
    }

    private static int status(TransportAddress address, String path) throws Exception {
        return Integer.parseInt(get(address, path).split(" ", 2)[0]);
    }

    public void testNodeBindsAndServesItsAllowlistedSurface() throws Exception {
        try (ServerlessNode node = new ServerlessNode(httpNodeSettings("rest-a"))) {
            node.start();

            final TransportAddress http = node.boundHttpAddress().publishAddress();
            assertTrue("HTTP did not bind to a real port", http.getPort() > 0);
            assertTrue("transport did not bind to a real port", node.boundTransportAddress().publishAddress().getPort() > 0);

            final String root = get(http, "/");
            assertTrue("GET / did not return 200: " + root, root.startsWith("200 "));
            assertTrue("GET / did not identify the node: " + root, root.contains("\"name\":\"rest-a\""));
            assertTrue("GET / did not identify as serverless: " + root, root.contains("\"flavour\":\"serverless\""));
            assertTrue("GET / did not carry a node id: " + root, root.contains("\"node_id\""));

            final String health = get(http, "/_serverless/health");
            assertTrue("health did not return 200: " + health, health.startsWith("200 "));
            assertTrue("health did not report serving: " + health, health.contains("\"status\":\"serving\""));
            // A node with no membership source sees exactly itself, and says so in those words.
            assertTrue("health did not report its own view: " + health, health.contains("\"members_visible_to_this_node\":1"));
        }
    }

    public void testUnimplementedEndpointsDoNotReturnAConfidentEmptyAnswer() throws Exception {
        try (ServerlessNode node = new ServerlessNode(httpNodeSettings("rest-b"))) {
            node.start();
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            // D2: the surface is an allowlist. These exist in classic OpenSearch and must NOT answer
            // here — an empty-but-successful cluster health is the exact failure HANDOFF.md records
            // eight times, and it is worse than a 404.
            for (String path : new String[] { "/_cluster/health", "/_cat/indices", "/_nodes", "/_cluster/state" }) {
                final String response = get(http, path);
                final int code = Integer.parseInt(response.split(" ", 2)[0]);
                assertNotEquals("unimplemented endpoint " + path + " answered with 200", 200, code);
                assertEquals("unimplemented endpoint " + path + " should refuse explicitly: " + response, 501, code);
                assertTrue("a 501 must say why: " + response, response.contains("no cluster-wide state"));
            }
        }
    }

    public void testHealthReportsTheMembershipSourceRatherThanAConstant() throws Exception {
        try (ServerlessNode node = new ServerlessNode(httpNodeSettings("rest-d"))) {
            node.start();
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            assertTrue(get(http, "/_serverless/health").contains("\"members_visible_to_this_node\":1"));

            // Wire a real lease-backed source with three members and confirm the endpoint reads it.
            // Without this the "1" above is indistinguishable from a hard-coded constant.
            final java.nio.file.Path dir = createTempDir();
            final org.opensearch.common.blobstore.fs.FsBlobStore store = new org.opensearch.common.blobstore.fs.FsBlobStore(
                1024,
                dir,
                false
            );
            final org.opensearch.serverless.membership.BlobLeaseMembership membership =
                new org.opensearch.serverless.membership.BlobLeaseMembership(
                    new org.opensearch.common.blobstore.fs.FsBlobContainer(
                        store,
                        org.opensearch.common.blobstore.BlobPath.cleanPath(),
                        dir
                    ),
                    () -> 1_000L,
                    30_000L
                );
            for (String id : new String[] { "n1", "n2", "n3" }) {
                membership.renew(
                    new org.opensearch.serverless.membership.NodeLease(id, id + "-e", "127.0.0.1:9300", java.util.Set.of("ingest"), 0L)
                );
            }
            membership.refresh();
            node.setMembershipSource(membership);

            final String health = get(http, "/_serverless/health");
            assertTrue("health did not read the membership source: " + health, health.contains("\"members_visible_to_this_node\":3"));
        }
    }

    public void testNodeShutsDownCleanlyAndReleasesItsPort() throws Exception {
        final ServerlessNode node = new ServerlessNode(httpNodeSettings("rest-c"));
        node.start();
        final TransportAddress http = node.boundHttpAddress().publishAddress();
        assertTrue(get(http, "/").startsWith("200 "));
        node.close();
        assertFalse("close() did not clear started", node.isStarted());

        // The port must actually be gone, not merely unreferenced.
        expectThrows(Exception.class, () -> get(http, "/"));
    }
}
