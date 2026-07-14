/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.resharding.action.CutoverSplitRoutingAction;
import org.opensearch.serverless.storage.resharding.action.CutoverSplitRoutingRequest;
import org.opensearch.serverless.storage.resharding.action.DisableWritePartitionRoutingAction;
import org.opensearch.serverless.storage.resharding.action.DisableWritePartitionRoutingRequest;
import org.opensearch.serverless.storage.resharding.action.EnableWritePartitionRoutingAction;
import org.opensearch.serverless.storage.resharding.action.EnableWritePartitionRoutingRequest;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Proves {@link EnableWritePartitionRoutingAction}/{@code WritePartitionRoutingActionFilter}
 * genuinely route real write traffic against a write-routing-enabled alias to the one correct real
 * target partition over the real transport layer, in a real cluster (rfc-serverless-opensearch.md
 * &sect;16 Phase 4's "real write-side partition routing" gap).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageWritePartitionRoutingActionIT extends ServerlessStorageIntegTestCase {

    @Override
    protected java.util.Collection<Class<? extends Plugin>> nodePlugins() {
        return java.util.Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testWritesAgainstTheAliasLandInTheirOwnCorrectRealPartitionOnly() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            "write-routing-target-a",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        createIndex(
            "write-routing-target-b",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen("write-routing-target-a", "write-routing-target-b");

        List<String> targets = List.of("write-routing-target-a", "write-routing-target-b");

        AcknowledgedResponse cutoverResponse = client().execute(
            CutoverSplitRoutingAction.INSTANCE,
            new CutoverSplitRoutingRequest("write-routing-alias", targets)
        ).get();
        assertTrue("the search-only cutover must be acknowledged first", cutoverResponse.isAcknowledged());

        AcknowledgedResponse enableResponse = client().execute(
            EnableWritePartitionRoutingAction.INSTANCE,
            new EnableWritePartitionRoutingRequest("write-routing-alias", targets)
        ).get();
        assertTrue("enabling write-partition-routing must be acknowledged", enableResponse.isAcknowledged());

        // Index a batch of documents with distinct explicit ids against the alias -- not either
        // target index by name -- and independently predict, via the exact same Murmur3 hash
        // RoutingPartitionFilter already uses read-side, which real target each one must land in.
        List<String> idsExpectedInA = new ArrayList<>();
        List<String> idsExpectedInB = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            String id = "doc-" + i;
            int partition = Math.floorMod(Murmur3HashFunction.hash(id), 2);
            (partition == 0 ? idsExpectedInA : idsExpectedInB).add(id);
            client().prepareIndex("write-routing-alias").setId(id).setSource("field", "value").get();
        }
        // A real split with only one document landing in a given partition is uninteresting --
        // assert the test itself actually exercises both real targets before trusting its result.
        assertFalse("test setup must produce documents in both partitions", idsExpectedInA.isEmpty());
        assertFalse("test setup must produce documents in both partitions", idsExpectedInB.isEmpty());

        refresh("write-routing-target-a", "write-routing-target-b");

        for (String id : idsExpectedInA) {
            boolean existsInA = client().prepareGet("write-routing-target-a", id).get().isExists();
            assertTrue("doc [" + id + "] must be routed into write-routing-target-a", existsInA);
            assertFalse(
                "doc [" + id + "] must not have also landed in write-routing-target-b",
                client().prepareGet("write-routing-target-b", id).get().isExists()
            );
        }
        for (String id : idsExpectedInB) {
            boolean existsInB = client().prepareGet("write-routing-target-b", id).get().isExists();
            assertTrue("doc [" + id + "] must be routed into write-routing-target-b", existsInB);
            assertFalse(
                "doc [" + id + "] must not have also landed in write-routing-target-a",
                client().prepareGet("write-routing-target-a", id).get().isExists()
            );
        }

        assertHitCount(client().prepareSearch("write-routing-alias").setSize(0).get(), 20);
    }

    public void testDirectWritesToAnAssignedTargetIndexAreRefused() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            "fenced-target-a",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        createIndex(
            "fenced-target-b",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen("fenced-target-a", "fenced-target-b");

        List<String> targets = List.of("fenced-target-a", "fenced-target-b");
        client().execute(CutoverSplitRoutingAction.INSTANCE, new CutoverSplitRoutingRequest("fenced-alias", targets)).get();
        client().execute(EnableWritePartitionRoutingAction.INSTANCE, new EnableWritePartitionRoutingRequest("fenced-alias", targets)).get();

        // A direct write to one of the two real assigned targets -- naming it by its own real
        // index name, not through the alias -- must be refused, since it would silently write into
        // just one partition's worth of documents behind the alias's back.
        Exception failure = expectThrows(
            Exception.class,
            () -> client().prepareIndex("fenced-target-a").setId("doc-1").setSource("field", "value").get()
        );
        assertTrue(
            "the refusal must be this plugin's own fencing check, not some other failure: " + failure,
            causedByFencingCheck(failure)
        );

        // The fenced write must never have partially applied.
        refresh("fenced-target-a");
        assertFalse(
            "a refused direct write must never actually land in the target index",
            client().prepareGet("fenced-target-a", "doc-1").get().isExists()
        );
    }

    private static boolean causedByFencingCheck(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains("writes must go through the alias")) {
                return true;
            }
        }
        return false;
    }

    public void testDisablingWriteRoutingRestoresOrdinaryDirectWrites() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            "disable-target-a",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        createIndex(
            "disable-target-b",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen("disable-target-a", "disable-target-b");

        List<String> targets = List.of("disable-target-a", "disable-target-b");
        client().execute(CutoverSplitRoutingAction.INSTANCE, new CutoverSplitRoutingRequest("disable-alias", targets)).get();
        client().execute(EnableWritePartitionRoutingAction.INSTANCE, new EnableWritePartitionRoutingRequest("disable-alias", targets))
            .get();

        // While enabled, a direct write to a target is refused (already proven above) -- confirm
        // it here too, immediately before disabling, so the "restored" half of this test is a real
        // before/after comparison, not just an assumption.
        expectThrows(
            Exception.class,
            () -> client().prepareIndex("disable-target-a").setId("before-disable").setSource("field", "value").get()
        );

        AcknowledgedResponse disableResponse = client().execute(
            DisableWritePartitionRoutingAction.INSTANCE,
            new DisableWritePartitionRoutingRequest(targets)
        ).get();
        assertTrue("disabling write-partition-routing must be acknowledged", disableResponse.isAcknowledged());

        // A direct write to the same target index, by the same real name, must now succeed --
        // this is the actual rollback this action exists to provide.
        client().prepareIndex("disable-target-a").setId("after-disable").setSource("field", "value").get();
        refresh("disable-target-a");
        assertTrue(
            "a direct write must succeed again once write-routing is disabled",
            client().prepareGet("disable-target-a", "after-disable").get().isExists()
        );
    }

    public void testConcurrentWritesRacingEnableAreNeverMisroutedOrDuplicated() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            "race-target-a",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        createIndex(
            "race-target-b",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen("race-target-a", "race-target-b");

        List<String> targets = List.of("race-target-a", "race-target-b");
        client().execute(CutoverSplitRoutingAction.INSTANCE, new CutoverSplitRoutingRequest("race-alias", targets)).get();

        // Writer threads hammer the alias with unique ids concurrently with a single call to
        // EnableWritePartitionRoutingAction landing partway through -- some writes before the
        // enable takes effect are expected to fail (no write-routing assignment yet, so core's own
        // multi-index-alias guard rejects them, the same safe failure mode proven in the main test
        // above), but every write that *succeeds* must be correct: routed to exactly the one real
        // partition its id's hash predicts, never duplicated across both targets, never lost.
        int writerThreads = 8;
        int writesPerThread = 25;
        Set<String> succeededIds = ConcurrentHashMap.newKeySet();
        CountDownLatch startLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(writerThreads + 1);
        try {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < writerThreads; t++) {
                int threadIndex = t;
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 0; i < writesPerThread; i++) {
                        String id = "race-doc-" + threadIndex + "-" + i;
                        try {
                            client().prepareIndex("race-alias").setId(id).setSource("field", "value").get();
                            succeededIds.add(id);
                        } catch (Exception expectedBeforeEnableTakesEffect) {
                            // Fine -- covered by the assertion below that only checks successes.
                        }
                    }
                }));
            }
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    client().execute(
                        EnableWritePartitionRoutingAction.INSTANCE,
                        new EnableWritePartitionRoutingRequest("race-alias", targets)
                    ).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
            startLatch.countDown();
            for (java.util.concurrent.Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        // Every write issued after this point is guaranteed to have write-routing active, so it
        // must succeed -- confirms the race window closed cleanly, not just "some things happened."
        client().prepareIndex("race-alias").setId("race-doc-final").setSource("field", "value").get();
        succeededIds.add("race-doc-final");

        refresh("race-target-a", "race-target-b");

        int foundInA = 0;
        int foundInB = 0;
        for (String id : succeededIds) {
            int expectedPartition = Math.floorMod(Murmur3HashFunction.hash(id), 2);
            boolean existsInA = client().prepareGet("race-target-a", id).get().isExists();
            boolean existsInB = client().prepareGet("race-target-b", id).get().isExists();
            assertFalse("doc [" + id + "] must never land in both targets at once", existsInA && existsInB);
            assertTrue("doc [" + id + "] that succeeded must exist in exactly one target", existsInA || existsInB);
            if (expectedPartition == 0) {
                assertTrue("doc [" + id + "] must land in its predicted partition (race-target-a)", existsInA);
            } else {
                assertTrue("doc [" + id + "] must land in its predicted partition (race-target-b)", existsInB);
            }
            if (existsInA) {
                foundInA++;
            } else {
                foundInB++;
            }
        }
        assertEquals("no document must be duplicated or lost across targets", succeededIds.size(), foundInA + foundInB);
    }
}
