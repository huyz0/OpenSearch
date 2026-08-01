/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;

import java.util.Collection;
import java.util.List;

/**
 * A gated index created, written, searched and deleted with the object store answering descriptor point
 * reads.
 *
 * <h2>Why this test is the point of the whole exercise</h2>
 *
 * Everything under the descriptor plane had been built and tested in isolation, and none of it had ever
 * served a request. {@code BlobDescriptorBackend} passed against {@code FsBlobContainer}, the change log
 * passed, the name index feed passed, the prefetcher passed, and the only test that starts a real cluster
 * still exercised the system index, because nothing installed the blob backend. Counting production callers
 * is what found that; this is what fixes it.
 *
 * <p>The failure being guarded is not any one of the components. It is that a design can be entirely
 * correct component by component and never have been integrated, which this area's own record calls
 * "correct and unreachable" and has produced repeatedly.
 *
 * <h2>What the fixture actually proves</h2>
 *
 * The premise guard matters more here than usual and this class states it up front, because
 * {@code GatedEndToEndIT} has already shipped a headline result that turned out to be served by ordinary
 * indices auto-creation had put back into cluster state. So every test below asserts the index is absent
 * from cluster state metadata before believing anything about it, and
 * {@link #testTheDescriptorReallyLivesInTheObjectStore} goes further and reads the descriptor out of the
 * store directly.
 */
@org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "I4. Integration got as far as gated creation reaching the blob backend and no further. "
    + "BlobDescriptorBackend.createAsync completes synchronously inside an already-completed future, "
    + "and DescriptorGate.registerCreator is called on the cluster state thread, which the gate's own "
    + "comment says must not block: 'registering the blocking put hung the node instead of failing, "
    + "which is how the constraint was found'. The blob backend states that limitation in its own "
    + "javadoc and this is the first caller to actually need it lifted. Fixing it means threading an "
    + "executor into the backend, which is its own task. The four failures below are all downstream of "
    + "it: the descriptor never lands in the store, so the wildcard finds nothing, the delete finds no "
    + "index, and the write leaves a shard locked.")
public class BlobBackedDescriptorIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    /**
     * The plugin supplies a real engine factory once it is enabled, and the test framework's mock one
     * collides with it. Every other IT in this plugin that actually turns the plugin on does the same.
     */
    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath().toString())
            // The gate installs itself from createComponents only when this is on, and it is read from
            // node settings there even though it is declared index-scoped. The sibling class leaves it off
            // and calls DescriptorGate.install by hand inside each test, which bypasses the plugin wiring
            // and is precisely what this class must not do: the thing under test is that the plugin
            // installs the blob backend, not that a backend works when a test installs it.
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_NODE_ENABLED_SETTING.getKey(), true)
            // The whole reason this class exists, and the one line that differs from GatedEndToEndIT.
            .put(ServerlessStoragePlugin.DESCRIPTOR_BACKEND_SETTING.getKey(), "blob")
            // Without this a gated index has no routing table at all, since computed placement supplies
            // one and is off by default. T36 found every earlier run of the sibling class lacked it.
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    private void createGated(String name) {
        assertTrue(
            client().admin()
                .indices()
                .create(
                    new CreateIndexRequest(name).settings(
                        Settings.builder()
                            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_NODE_ENABLED_SETTING.getKey(), true)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                )
                .actionGet()
                .isAcknowledged()
        );
    }

    /**
     * The premise, asserted before anything is concluded from it. An index that is in cluster state is an
     * ordinary index, and every result about it would be a result about the path this test is not testing.
     */
    private void assertGated(String name) {
        assertFalse(
            "[" + name + "] is in cluster state metadata, so it is not gated and nothing below is about the blob backend",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex(name)
        );
    }

    public void testAGatedIndexCanBeCreatedResolvedAndSearchedOnTheBlobBackend() {
        createGated("tenant-alpha");
        assertGated("tenant-alpha");

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 25; i++) {
            bulk.add(client().prepareIndex("tenant-alpha").setId("doc-" + i).setSource("value", i));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());

        client().admin().indices().prepareRefresh("tenant-alpha").get();
        SearchResponse search = client().prepareSearch("tenant-alpha").setQuery(QueryBuilders.matchAllQuery()).setSize(0).get();
        assertEquals(25, search.getHits().getTotalHits().value());
    }

    /**
     * Reads the descriptor straight out of the object store, so the claim is about where the bytes are and
     * not merely about whether a request succeeded. A request succeeding proves the system index could
     * have served it too.
     */
    public void testTheDescriptorReallyLivesInTheObjectStore() throws Exception {
        createGated("tenant-bravo");
        assertGated("tenant-bravo");

        org.opensearch.common.blobstore.fs.FsBlobStore store = new org.opensearch.common.blobstore.fs.FsBlobStore(1024, basePath(), false);
        DescriptorEnumerator enumerator = new DescriptorEnumerator(
            store::blobContainer,
            org.opensearch.common.blobstore.BlobPath.cleanPath().add("descriptors-root")
        );

        assertTrue(
            "the descriptor must be readable from the object store alone, which is invariant I2",
            enumerator.allNames().contains("tenant-bravo")
        );
    }

    /**
     * Wildcards must keep working while point reads move, which is the whole reason the backend is a
     * composite rather than a swap. T3 established a bucket cannot serve a prefix search; if the switch
     * had simply replaced the store, this would return nothing and would do so without an error.
     */
    public void testWildcardsStillResolveWithPointReadsOnTheObjectStore() {
        createGated("logs-one");
        createGated("logs-two");
        createGated("metrics-one");
        assertGated("logs-one");

        SearchResponse search = client().prepareSearch("logs-*").setQuery(QueryBuilders.matchAllQuery()).setSize(0).get();
        assertEquals("a wildcard must reach both gated tenants and neither more nor fewer", 2, search.getTotalShards());
    }

    /** Deletion has to remove the name from the object store's live prefix, not merely from a request's view. */
    public void testDeletionRemovesTheNameFromTheObjectStore() throws Exception {
        createGated("tenant-doomed");
        assertGated("tenant-doomed");

        assertTrue(client().admin().indices().prepareDelete("tenant-doomed").get().isAcknowledged());

        org.opensearch.common.blobstore.fs.FsBlobStore store = new org.opensearch.common.blobstore.fs.FsBlobStore(1024, basePath(), false);
        DescriptorEnumerator enumerator = new DescriptorEnumerator(
            store::blobContainer,
            org.opensearch.common.blobstore.BlobPath.cleanPath().add("descriptors-root")
        );

        assertFalse(
            "a deleted name must leave the live prefix, or a rebuild resurrects it",
            enumerator.allNames().contains("tenant-doomed")
        );
    }
}
