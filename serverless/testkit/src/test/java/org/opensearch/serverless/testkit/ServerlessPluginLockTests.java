/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

/**
 * A lock document as plugins keep one: created only if absent, then changed only if nobody changed it since it was read.
 */
public class ServerlessPluginLockTests extends OpenSearchTestCase {

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-plugin-lock")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ServerlessBootstrap.STORE_PATH, createTempDir().toString())
            .build();
    }

    /** The alerting plugin's lock: created if absent, released by a conditional update that asks for the source back. */
    public void testCreateThenConditionalUpdate() throws Exception {
        try (ServerlessBootstrap boot = ServerlessBootstrap.start(nodeSettings("lock"))) {
            final Client client = boot.node().services().client();
            client.admin()
                .indices()
                .create(new CreateIndexRequest(".locks").mapping("{\"properties\":{\"released\":{\"type\":\"boolean\"}}}"))
                .actionGet();
            final IndexResponse created = client.index(
                new IndexRequest(".locks").id("job-1")
                    .source("{\"released\":false}", XContentType.JSON)
                    .setIfSeqNo(-2)
                    .setIfPrimaryTerm(0)
                    .create(true)
            ).actionGet();
            final GetResponse read = client.get(new GetRequest(".locks", "job-1")).actionGet();
            assertEquals(created.getSeqNo(), read.getSeqNo());
            assertEquals(created.getPrimaryTerm(), read.getPrimaryTerm());

            final UpdateResponse released = client.update(
                new UpdateRequest(".locks", "job-1").doc("{\"released\":true}", XContentType.JSON)
                    .setIfSeqNo(created.getSeqNo())
                    .setIfPrimaryTerm(created.getPrimaryTerm())
                    .fetchSource(true)
            ).actionGet();
            assertEquals(org.opensearch.action.DocWriteResponse.Result.UPDATED, released.getResult());
            assertNotNull("the source was asked for", released.getGetResult());
            assertEquals(true, released.getGetResult().sourceAsMap().get("released"));
            final GetResponse after = client.get(new GetRequest(".locks", "job-1")).actionGet();
            assertEquals("the release is read back: " + after.getSourceAsString(), true, after.getSourceAsMap().get("released"));
        }
    }
}
