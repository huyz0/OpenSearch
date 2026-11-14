/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.Map;

/**
 * Index administration through the plugin client: how plugins check, upgrade and roll over their own indices.
 */
public class ServerlessPluginIndexAdminTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}";

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-index-admin")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ServerlessBootstrap.STORE_PATH, createTempDir().toString())
            .put(extra)
            .build();
    }

    /** Existence, a date-math name, and an index created with an alias, read back as core shapes it. */
    public void testCreateWithAliasAndReadBack() throws Exception {
        try (ServerlessBootstrap boot = ServerlessBootstrap.start(nodeSettings("admin-read", Settings.EMPTY))) {
            final Client client = boot.node().services().client();
            assertFalse(client.admin().indices().exists(new IndicesExistsRequest("history-1")).actionGet().isExists());

            client.admin()
                .indices()
                .create(new CreateIndexRequest("<history-{now/d}-1>").mapping(MAPPING).alias(new Alias("history-write")))
                .actionGet();
            assertTrue(client.admin().indices().exists(new IndicesExistsRequest("<history-{now/d}-1>")).actionGet().isExists());
            assertTrue(
                "an alias exists as a name",
                client.admin().indices().exists(new IndicesExistsRequest("history-write")).actionGet().isExists()
            );

            final GetAliasesResponse aliases = client.admin().indices().getAliases(new GetAliasesRequest("history-write")).actionGet();
            assertEquals(1, aliases.getAliases().size());
            final String created = aliases.getAliases().keySet().iterator().next();
            assertTrue(created, created.startsWith("history-") && created.endsWith("-1"));

            final GetIndexResponse index = client.admin().indices().getIndex(new GetIndexRequest().indices("history-write")).actionGet();
            assertArrayEquals(new String[] { created }, index.indices());
            assertTrue(index.mappings().get(created).source().string().contains("msg"));
            assertEquals("history-write", index.aliases().get(created).get(0).alias());
        }
    }

    /** A mapping update, a settings update, an alias change and a delete, each read back. */
    public void testChangesAreApplied() throws Exception {
        try (ServerlessBootstrap boot = ServerlessBootstrap.start(nodeSettings("admin-change", Settings.EMPTY))) {
            final Client client = boot.node().services().client();
            client.admin().indices().create(new CreateIndexRequest(".plugin-config").mapping(MAPPING)).actionGet();

            assertTrue(
                client.admin()
                    .indices()
                    .putMapping(
                        new PutMappingRequest(".plugin-config").source(
                            "{\"properties\":{\"added\":{\"type\":\"long\"}}}",
                            org.opensearch.common.xcontent.XContentType.JSON
                        )
                    )
                    .actionGet()
                    .isAcknowledged()
            );
            final var mappings = client.admin().indices().getMappings(new GetMappingsRequest().indices(".plugin-config")).actionGet();
            assertTrue(mappings.mappings().get(".plugin-config").source().string().contains("added"));

            assertTrue(
                client.admin()
                    .indices()
                    .updateSettings(
                        new UpdateSettingsRequest(".plugin-config").settings(Settings.builder().put("index.max_result_window", 500))
                    )
                    .actionGet()
                    .isAcknowledged()
            );
            final var settings = client.admin().indices().getSettings(new GetSettingsRequest().indices(".plugin-config")).actionGet();
            assertEquals("500", settings.getSetting(".plugin-config", "index.max_result_window"));

            assertTrue(
                client.admin()
                    .indices()
                    .aliases(
                        new IndicesAliasesRequest().addAliasAction(
                            IndicesAliasesRequest.AliasActions.add().index(".plugin-config").alias(".plugin-current")
                        )
                    )
                    .actionGet()
                    .isAcknowledged()
            );
            assertEquals(
                Map.of(".plugin-config", 1),
                Map.of(
                    ".plugin-config",
                    client.admin()
                        .indices()
                        .getAliases(new GetAliasesRequest(".plugin-current"))
                        .actionGet()
                        .getAliases()
                        .get(".plugin-config")
                        .size()
                )
            );

            assertTrue(client.admin().indices().delete(new DeleteIndexRequest(".plugin-config")).actionGet().isAcknowledged());
            assertFalse(client.admin().indices().exists(new IndicesExistsRequest(".plugin-config")).actionGet().isExists());
            final Exception missing = expectThrows(
                Exception.class,
                () -> client.admin().indices().delete(new DeleteIndexRequest(".plugin-config")).actionGet()
            );
            assertNotNull(
                "a missing index is core's exception: " + missing,
                org.opensearch.ExceptionsHelper.unwrap(missing, org.opensearch.index.IndexNotFoundException.class)
            );
        }
    }

    /** An alias over one index rolls over to a new one. */
    public void testRollover() throws Exception {
        try (ServerlessBootstrap boot = ServerlessBootstrap.start(nodeSettings("admin-rollover", Settings.EMPTY))) {
            final Client client = boot.node().services().client();
            client.admin()
                .indices()
                .create(new CreateIndexRequest("alerts-000001").mapping(MAPPING).alias(new Alias("alerts-write")))
                .actionGet();
            final RolloverResponse rolled = client.admin().indices().rolloverIndex(new RolloverRequest("alerts-write", null)).actionGet();
            assertTrue(rolled.isRolledOver());
            assertEquals("alerts-000001", rolled.getOldIndex());
            assertEquals("alerts-000002", rolled.getNewIndex());
            assertTrue(client.admin().indices().exists(new IndicesExistsRequest("alerts-000002")).actionGet().isExists());
        }
    }

    /** With the switch off, the client refuses them by name, as before. */
    public void testTheSwitchRefusesThem() throws Exception {
        try (
            ServerlessBootstrap boot = ServerlessBootstrap.start(
                nodeSettings("admin-off", Settings.builder().put("serverless.plugin_client.index_admin.enabled", false).build())
            )
        ) {
            final Exception refused = expectThrows(
                Exception.class,
                () -> boot.node().services().client().admin().indices().exists(new IndicesExistsRequest("any")).actionGet()
            );
            assertTrue(refused.toString(), org.opensearch.ExceptionsHelper.stackTrace(refused).contains("does not implement action"));
        }
    }
}
