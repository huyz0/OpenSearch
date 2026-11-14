/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

/**
 * The plugin client runs multi-get, bulk and update, and a plugin's first write to an index it just created is
 * written rather than refused: how plugins such as the security plugin load and store their own state.
 */
public class ServerlessPluginClientBatchTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-client-batches")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ServerlessBootstrap.STORE_PATH, createTempDir().toString())
            .put(extra)
            .build();
    }

    /** Create, bulk-write with no activation by hand, multi-get, update and bulk-delete, all through the client. */
    public void testAPluginStoresAndLoadsItsStateInBatches() throws Exception {
        try (ServerlessBootstrap boot = ServerlessBootstrap.start(nodeSettings("batches", Settings.EMPTY))) {
            final Client client = boot.node().services().client();
            client.admin().indices().create(new CreateIndexRequest(".plugin_state").mapping(MAPPING)).actionGet();

            // The first write to a shard nobody holds: taken by this node and written, not refused.
            final BulkResponse written = client.bulk(
                new BulkRequest().add(new IndexRequest(".plugin_state").id("roles").source("{\"msg\":\"r\",\"n\":1}", XContentType.JSON))
                    .add(new IndexRequest(".plugin_state").id("users").source("{\"msg\":\"u\",\"n\":2}", XContentType.JSON))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            ).actionGet();
            assertFalse("every item must be written: " + written.buildFailureMessage(), written.hasFailures());
            assertEquals(DocWriteResponse.Result.CREATED, written.getItems()[0].getResponse().getResult());

            final MultiGetResponse loaded = client.multiGet(
                new MultiGetRequest().add(".plugin_state", "roles").add(".plugin_state", "users").add(".plugin_state", "absent")
            ).actionGet();
            assertTrue(loaded.getResponses()[0].getResponse().isExists());
            assertTrue(loaded.getResponses()[1].getResponse().getSourceAsString().contains("\"msg\":\"u\""));
            assertFalse("a missing document is reported absent, not failed", loaded.getResponses()[2].getResponse().isExists());

            final var updated = client.update(new UpdateRequest(".plugin_state", "roles").doc("{\"n\":5}", XContentType.JSON)).actionGet();
            assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());

            final BulkResponse mixed = client.bulk(
                new BulkRequest().add(new DeleteRequest(".plugin_state", "users"))
                    .add(new IndexRequest("no-such-index").id("x").source("{\"msg\":\"x\"}", XContentType.JSON))
            ).actionGet();
            assertFalse("the delete succeeds", mixed.getItems()[0].isFailed());
            assertTrue("an item that fails is reported in its slot", mixed.getItems()[1].isFailed());
            assertEquals(
                "the failure is core's, as a plugin expects",
                org.opensearch.index.IndexNotFoundException.class,
                mixed.getItems()[1].getFailure().getCause().getClass()
            );
        }
    }

    /** With the switch off, the client refuses the batches by name, as before. */
    public void testTheSwitchRefusesBatches() throws Exception {
        try (
            ServerlessBootstrap boot = ServerlessBootstrap.start(
                nodeSettings("batches-off", Settings.builder().put("serverless.plugin_client.batches.enabled", false).build())
            )
        ) {
            final Exception refused = expectThrows(
                Exception.class,
                () -> boot.node().services().client().multiGet(new MultiGetRequest().add("any", "id")).actionGet()
            );
            assertTrue(refused.toString(), org.opensearch.ExceptionsHelper.stackTrace(refused).contains("does not implement action"));
        }
    }
}
