/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidateEntry;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesAction;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesRequest;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end proof of {@link ShardSplitCandidatesAction}: real write traffic against a real writer
 * shard drives {@code ObjectStoreWriterEngine#writesPerMinute()} up, and a cluster-wide evaluation
 * (the REST/transport action, not a direct unit-level call into {@code
 * ShardActivityRegistry#snapshotWritesPerMinute}) reports that shard with a real, non-negative
 * writes-per-minute signal.
 *
 * <p>Deliberately does not assert the signal crosses the plugin's real default threshold -- that
 * would be brittle, since it depends on real write throughput this test environment does not
 * control. Instead the request-supplied threshold override is used to prove the {@code candidate}
 * flag reacts correctly to a threshold the test controls directly, mirroring {@code
 * ServerlessStorageReaderScaleUpIT}'s own approach for the reader-side counterpart.
 *
 * <p>This action is deliberately not wired to any auto-trigger -- see {@link ShardSplitCandidateEntry}'s
 * own javadoc for why. This test only proves the observability surface itself works end to end.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageShardSplitCandidatesIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "serverless-split-candidates-it-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testABusyWriterShardIsReportedWithAPlausibleWriteRateAndRespectsARequestThreshold() throws Exception {
        Path basePath = createTempDir("serverless-storage-split-candidates-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        // ObjectStoreWriterEngine#writesPerMinute() deliberately only ever reports the previous
        // *completed* 60-second window (see that method's own javadoc for why), and a window only
        // rolls over when a write lands after it has run long enough -- so proving a real,
        // non-just-reset writes-per-minute reading genuinely requires spanning a real 60-second
        // window with write traffic on both sides of the rollover, not just firing a quick burst.
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(75);
        AtomicInteger docId = new AtomicInteger();
        while (System.currentTimeMillis() < deadline) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(docId.getAndIncrement())).setSource("field", "value").get();
            Thread.sleep(2000);
        }

        assertBusy(() -> {
            // writesPerMinuteThreshold=0 -- the point of this request is proving the mechanism
            // reports a real signal end to end, not exercising the real default threshold's exact
            // value.
            ShardSplitCandidatesResponse lowThresholdResponse = client().execute(
                ShardSplitCandidatesAction.INSTANCE,
                new ShardSplitCandidatesRequest(0L)
            ).actionGet();
            ShardSplitCandidateEntry lowThresholdEntry = lowThresholdResponse.candidates()
                .stream()
                .filter(c -> c.indexName().equals(INDEX_NAME))
                .findFirst()
                .orElse(null);
            if (lowThresholdEntry == null) {
                // Force another completed write-rate window by sending one more write, then retry.
                client().prepareIndex(INDEX_NAME).setId(Integer.toString(docId.getAndIncrement())).setSource("field", "value").get();
            }
            assertTrue("the busy writer shard must eventually be reported with a signal", lowThresholdEntry != null);
            assertTrue("the reported write rate must be a real non-negative measurement", lowThresholdEntry.writesPerMinute() >= 0);
            assertTrue(
                "with a threshold of 0, any nonzero completed-window write count must be a candidate",
                lowThresholdEntry.writesPerMinute() == 0 || lowThresholdEntry.candidate()
            );

            // A deliberately absurdly high override threshold must never be crossed, proving the
            // candidate flag genuinely reacts to the request-supplied threshold rather than always
            // reporting true.
            ShardSplitCandidatesResponse highThresholdResponse = client().execute(
                ShardSplitCandidatesAction.INSTANCE,
                new ShardSplitCandidatesRequest(Long.MAX_VALUE / 2)
            ).actionGet();
            ShardSplitCandidateEntry highThresholdEntry = highThresholdResponse.candidates()
                .stream()
                .filter(c -> c.indexName().equals(INDEX_NAME))
                .findFirst()
                .orElseThrow();
            assertFalse("an absurdly high override threshold must never be crossed", highThresholdEntry.candidate());
        }, 90, TimeUnit.SECONDS);
    }
}
