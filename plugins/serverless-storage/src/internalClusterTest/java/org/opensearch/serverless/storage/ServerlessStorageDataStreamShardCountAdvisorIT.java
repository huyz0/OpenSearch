/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.resharding.DataStreamShardCountAdvisorCache;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Proves the Elasticsearch-Serverless-style write-load autosharding wiring genuinely reaches core's
 * own real data-stream rollover path -- not just that {@link DataStreamShardCountAdvisorCache}
 * itself works in isolation (rfc-serverless-opensearch.md &sect;16 Phase 4, tracked in
 * <code>write-routing-and-term-authority-progress.md</code>'s Effort A).
 *
 * <p>Deliberately seeds the cache directly via the plugin's own test-only accessor, rather than
 * waiting for a real sustained-high-write-rate evaluation to happen naturally: {@code
 * DataStreamShardCountAdvisorSchedulerTask}'s own decision logic (candidate lookup, growth factor,
 * cap) is already covered by {@code DataStreamShardCountAdvisorSchedulerTaskTests} at the unit
 * level. What this test proves that no unit test can is the one genuinely uncertain, real-system
 * question: does {@code ServerlessStorageIndexSettingProvider} actually get consulted, with the
 * right value, at the exact moment core creates a data stream's next backing index during a real
 * rollover.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ServerlessStorageDataStreamShardCountAdvisorIT extends ServerlessStorageIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    public void testARealRolloverPicksUpTheCachedRecommendation() throws Exception {
        String dataStreamName = "logs-advisor-it";

        // Deliberately no explicit SETTING_NUMBER_OF_SHARDS here -- the provider only overrides the
        // shard count when the template/request left it unset (see
        // ServerlessStorageIndexSettingProvider's own hasValue(...) guard), so this test must not
        // set it explicitly or the override would never fire.
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            List.of(dataStreamName + "*"),
            new Template(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build(), null, null),
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(new DataStream.TimestampField("@timestamp"))
        );
        PutComposableIndexTemplateAction.Request putTemplateRequest = new PutComposableIndexTemplateAction.Request("advisor-it-template");
        putTemplateRequest.indexTemplate(template);
        AcknowledgedResponse putTemplateResponse = client().execute(PutComposableIndexTemplateAction.INSTANCE, putTemplateRequest).get();
        assertTrue(putTemplateResponse.isAcknowledged());

        AcknowledgedResponse createDataStreamResponse = client().admin()
            .indices()
            .createDataStream(new org.opensearch.action.admin.indices.datastream.CreateDataStreamAction.Request(dataStreamName))
            .get();
        assertTrue(createDataStreamResponse.isAcknowledged());
        ensureGreen(DataStream.getDefaultBackingIndexName(dataStreamName, 1));

        // Seed the cache directly -- see class javadoc for why this is the right thing for this
        // test to do, rather than waiting on a real evaluation. IndexSettingProvider only ever runs
        // on the cluster-manager node (MetadataCreateIndexService), and the cache is a per-node
        // instance field, so the seed must land on that specific node's plugin instance.
        ServerlessStoragePlugin plugin = internalCluster().getInstance(
            ServerlessStoragePlugin.class,
            internalCluster().getClusterManagerName()
        );
        DataStreamShardCountAdvisorCache cache = plugin.dataStreamShardCountAdvisorCacheForTesting();
        cache.record(dataStreamName, 5);

        RolloverRequest rolloverRequest = new RolloverRequest(dataStreamName, null);
        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(rolloverRequest).get();
        assertTrue("the rollover itself must succeed", rolloverResponse.isRolledOver());
        String newBackingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        ensureGreen(newBackingIndexName);

        IndexMetadata newBackingIndexMetadata = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(newBackingIndexName);
        assertEquals(
            "the new backing index must have the shard count this plugin's cache recommended, not the template's default",
            5,
            newBackingIndexMetadata.getNumberOfShards()
        );
    }

    public void testARealRolloverUsesTheTemplateDefaultWhenNothingIsCached() throws Exception {
        String dataStreamName = "logs-advisor-it-nocache";

        ComposableIndexTemplate template = new ComposableIndexTemplate(
            List.of(dataStreamName + "*"),
            new Template(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build(),
                null,
                null
            ),
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(new DataStream.TimestampField("@timestamp"))
        );
        PutComposableIndexTemplateAction.Request putTemplateRequest = new PutComposableIndexTemplateAction.Request(
            "advisor-it-template-nocache"
        );
        putTemplateRequest.indexTemplate(template);
        client().execute(PutComposableIndexTemplateAction.INSTANCE, putTemplateRequest).get();

        client().admin()
            .indices()
            .createDataStream(new org.opensearch.action.admin.indices.datastream.CreateDataStreamAction.Request(dataStreamName))
            .get();
        ensureGreen(DataStream.getDefaultBackingIndexName(dataStreamName, 1));

        // Deliberately no cache.record(...) call here -- proves "no recommendation" behaves as a
        // genuine no-op, not an accidental zero/default that would silently corrupt the template's
        // own setting.
        RolloverRequest rolloverRequest = new RolloverRequest(dataStreamName, null);
        client().admin().indices().rolloverIndex(rolloverRequest).get();
        String newBackingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        ensureGreen(newBackingIndexName);

        IndexMetadata newBackingIndexMetadata = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(newBackingIndexName);
        assertEquals(
            "with nothing cached, the template's own shard count must be used unchanged",
            1,
            newBackingIndexMetadata.getNumberOfShards()
        );
    }
}
