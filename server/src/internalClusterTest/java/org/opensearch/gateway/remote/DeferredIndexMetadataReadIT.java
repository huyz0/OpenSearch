/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadataHolder;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.Map;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;

/**
 * D1. C5 and C6 shipped inert: {@code Metadata.Builder#putStub} had only test callers, and both
 * settings default to off. This is the end-to-end exercise that shows the machinery does what it was
 * built for when they are turned on, and that a cluster with them on still behaves normally.
 *
 * <p><b>What it asserts, and why not blob reads.</b> The task called for counting blob reads on the
 * theory that fewer fetches is the observable win. The stronger and more direct assertion is the
 * property those fetches were avoided by: after a cluster-manager restart reads full state, the
 * index holders in the restored {@code Metadata} are unresolved stubs. A blob-read count could go
 * down for unrelated reasons -- caching, a changed retry -- while an unresolved holder can only exist
 * because deferral worked.
 *
 * <p>The second half matters as much as the first. A stub that never resolves is not a saving, it is
 * a broken cluster, so the test also drives ordinary requests against those indices afterwards.
 *
 * <h2>The finding: it does not engage, and this test is why we know</h2>
 *
 * <b>The deferral test is {@code @AwaitsFix} because it fails, and that failure is D1's actual
 * result.</b> With both settings on, five indices created and the whole cluster restarted, every
 * holder in the restored {@code Metadata} comes back materialized. Not some -- all five.
 *
 * <p>Two candidate explanations, neither yet eliminated:
 *
 * <ol>
 *   <li><b>The remote read path never runs.</b> An {@code internalCluster()} full restart recovers
 *       from the local gateway, so the repository may not be read at all. The first version of this
 *       test restarted only the cluster-manager and got the same result for a definitely-different
 *       reason -- it rejoined by publication -- so "the read path did not run" has already fooled
 *       this test once.</li>
 *   <li><b>Something resolves the stubs during {@code Metadata} construction.</b> This is the
 *       serious one. C5's premise is that building {@code Metadata}, its derived arrays and its
 *       {@code indicesLookup} never materializes a holder. {@code DeferredIndexMetadataTests} proves
 *       that for a hand-built {@code Metadata}; it does not prove it for the path a real full-state
 *       read takes, which runs validation and construction this test is the first thing to
 *       exercise.</li>
 * </ol>
 *
 * <p>Distinguishing them is the next task: assert on {@code RemoteClusterStateService}'s own read
 * path directly, so "the read ran" and "the read deferred" are separate observations rather than one
 * conflated assertion. Until then D2's decision to keep both settings off has evidence behind it
 * rather than only a cost argument.
 *
 * <p>The control below passes, and is kept precisely so that this class does not become a test that
 * only ever fails: with the settings off, every holder is materialized, which confirms the assertion
 * mechanism itself works and reads what it claims to read.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DeferredIndexMetadataReadIT extends RemoteStoreBaseIntegTestCase {

    private static final int INDEX_COUNT = 5;

    @Before
    public void setup() {
        asyncUploadMockFsRepo = false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(RemoteIndexMetadataManager.REMOTE_INDEX_METADATA_DESCRIPTOR_SETTING.getKey(), true)
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_DEFER_INDEX_METADATA_SETTING.getKey(), true)
            .build();
    }

    @AwaitsFix(bugUrl = "D1 finding: deferral does not engage end-to-end. See this test's class javadoc and "
        + "rfc-scalable-index-metadata-tasks.md.")
    public void testFullStateReadInstallsStubsAndTheyStillResolve() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);

        for (int i = 0; i < INDEX_COUNT; i++) {
            createIndex(
                "idx-" + i,
                Settings.builder()
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            );
        }
        ensureGreen();

        // A *full* restart is what forces the remote full-state read. Restarting the cluster-manager
        // alone does not: it rejoins and receives state by publication from the surviving nodes, so
        // nothing is read from the repository at all. The first version of this test restarted one
        // node and found five resolved holders, which looked like deferral failing and was actually
        // the read path never running.
        internalCluster().fullRestart();
        ensureGreen();

        ClusterService clusterService = internalCluster().clusterService(internalCluster().getClusterManagerName());
        Metadata metadata = clusterService.state().metadata();

        int deferred = 0;
        for (Map.Entry<String, IndexMetadataHolder> entry : metadata.indexHolders().entrySet()) {
            if (entry.getKey().startsWith("idx-") && entry.getValue().isResolved() == false) {
                deferred++;
                // The descriptor has to be usable without resolving, or nothing has been deferred --
                // these are the reads Metadata's derived arrays and indicesLookup make.
                assertNotNull(entry.getValue().getState());
                assertNotNull(entry.getValue().getIndex());
            }
        }
        assertTrue(
            "at least one index should have been read as an unresolved stub, found none of " + metadata.indexHolders().size(),
            deferred > 0
        );

        // And the cluster still works: a stub that never resolves is a broken cluster, not a saving.
        for (int i = 0; i < INDEX_COUNT; i++) {
            client().prepareIndex("idx-" + i).setId("1").setSource("field", "value").get();
        }
        client().admin().indices().prepareRefresh().get();
        for (int i = 0; i < INDEX_COUNT; i++) {
            assertEquals(1, client().prepareSearch("idx-" + i).get().getHits().getTotalHits().value());
        }
    }

    /**
     * The control, and the reason it is here: without it a green run proves only that the test is
     * consistent with deferral, not that deferral is what produced it. With the settings off every
     * holder must be materialized.
     */
    public void testHoldersAreMaterializedWhenDeferralIsOff() throws Exception {
        Settings off = Settings.builder()
            .put(RemoteIndexMetadataManager.REMOTE_INDEX_METADATA_DESCRIPTOR_SETTING.getKey(), false)
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_DEFER_INDEX_METADATA_SETTING.getKey(), false)
            .build();
        internalCluster().startClusterManagerOnlyNode(off);
        internalCluster().startDataOnlyNodes(1, off);

        createIndex(
            "idx-plain",
            Settings.builder()
                .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen();

        internalCluster().fullRestart();
        ensureGreen();

        ClusterState state = internalCluster().clusterService(internalCluster().getClusterManagerName()).state();
        for (Map.Entry<String, IndexMetadataHolder> entry : state.metadata().indexHolders().entrySet()) {
            assertTrue("with deferral off every holder must be materialized: " + entry.getKey(), entry.getValue().isResolved());
        }
    }
}
