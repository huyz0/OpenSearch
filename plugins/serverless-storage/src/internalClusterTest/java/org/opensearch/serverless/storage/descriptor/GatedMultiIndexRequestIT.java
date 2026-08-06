/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A request that names several indices must reach all of them, not the ones it finds easiest.
 *
 * <h2>Why this is not covered by what exists</h2>
 *
 * {@code GatedEndToEndIT} proves two things: one gated index answers by name, and one wildcard reaches
 * twenty of them. Neither is this. The wildcard resolves through prefix expansion and then fans out to
 * whatever expansion returned, so it exercises the expansion; naming indices explicitly resolves each one
 * through the exact-name seam instead, and a request carrying several names exercises a loop that the
 * single-name case never enters.
 *
 * <p>The mixed case is the one worth the most. A request naming a gated index and an ordinary one has to
 * resolve two names by two different mechanisms and combine them, and the failure mode there is not an
 * error. Resolution that quietly drops what it cannot find returns a smaller result set with a success
 * status, and a search that returns nothing for a tenant looks exactly like a tenant with no data. This
 * area has produced that answer three times: the settings lookup that returned empty with a success
 * status, alias resolution that resolved to nothing under the options most clients use, and the
 * create-time mapping that was accepted and discarded.
 *
 * <p>So every case here counts documents. A count is the only assertion that separates "reached every
 * index" from "reached the ones it could and said nothing about the rest".
 */
public class GatedMultiIndexRequestIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final int TENANTS = 5;

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

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                org.opensearch.serverless.storage.ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(),
                basePath().toString()
            )
            // Without this a gated index has no routing table at all, because computed placement is off by
            // default and is what supplies one, so every write here fails with "no such index". T36 found
            // that the hard way in GatedEndToEndIT, where its absence meant the writes that appeared to
            // work were served by indices auto-creation had put back into cluster state.
            .put(org.opensearch.serverless.storage.ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    /** Several gated indices named explicitly in one request, each holding one document. */
    public void testEveryExplicitlyNamedGatedIndexIsReached() throws Exception {
        installBlobBackedDescriptorPlane();

        List<String> names = new ArrayList<>();
        for (int i = 0; i < TENANTS; i++) {
            String name = "multi-tenant-" + i;
            assertTrue(client().admin().indices().create(new CreateIndexRequest(name).settings(gated())).actionGet().isAcknowledged());
            client().prepareIndex(name).setId("0").setSource("tenant", name).get();
            names.add(name);
        }
        client().admin().indices().prepareRefresh(names.toArray(new String[0])).get();

        SearchResponse response = client().prepareSearch(names.toArray(new String[0]))
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(0)
            .get();

        assertEquals(
            "a request naming "
                + TENANTS
                + " gated indices must return a document from each. A short count "
                + "here is a tenant silently missing from its own query",
            TENANTS,
            response.getHits().getTotalHits().value()
        );
    }

    /**
     * A gated index and an ordinary one in the same request.
     *
     * <p>The sharpest case, because the two names resolve by different mechanisms and the result has to
     * combine them. Kept separate from the all-gated case above so a failure says which arrangement broke:
     * if both fail, resolution of multiple names is wrong; if only this one does, it is the combining.
     */
    public void testAMixOfGatedAndOrdinaryIndicesIsReached() throws Exception {
        installBlobBackedDescriptorPlane();

        assertTrue(client().admin().indices().create(new CreateIndexRequest("mixed-gated").settings(gated())).actionGet().isAcknowledged());
        assertTrue(
            client().admin()
                .indices()
                .create(
                    new CreateIndexRequest("mixed-ordinary").settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .build()
                    )
                )
                .actionGet()
                .isAcknowledged()
        );

        client().prepareIndex("mixed-gated").setId("0").setSource("which", "gated").get();
        client().prepareIndex("mixed-ordinary").setId("0").setSource("which", "ordinary").get();
        client().admin().indices().prepareRefresh("mixed-gated", "mixed-ordinary").get();

        SearchResponse response = client().prepareSearch("mixed-gated", "mixed-ordinary")
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(0)
            .get();

        assertEquals(
            "a request naming one gated and one ordinary index must return both documents. Returning one "
                + "with a success status is the failure this area keeps producing: a wrong answer that "
                + "looks like a right one",
            2L,
            response.getHits().getTotalHits().value()
        );
    }

    /**
     * A multi-index request naming one index that does not exist must say so.
     *
     * <p>The counterpart to the two above, and it is what stops them being satisfied by a resolution that
     * never fails. If a missing name is quietly dropped, then a typo in a tenant id returns a smaller
     * result set with a success status rather than an error, and the caller cannot tell the difference
     * between a tenant with no data and a tenant it failed to ask about.
     */
    public void testANamedIndexThatDoesNotExistIsStillAnError() throws Exception {
        installBlobBackedDescriptorPlane();

        assertTrue(
            client().admin().indices().create(new CreateIndexRequest("present-gated").settings(gated())).actionGet().isAcknowledged()
        );
        client().prepareIndex("present-gated").setId("0").setSource("which", "gated").get();
        client().admin().indices().prepareRefresh("present-gated").get();

        expectThrows(
            Exception.class,
            () -> client().prepareSearch("present-gated", "absent-tenant").setQuery(QueryBuilders.matchAllQuery()).setSize(0).get()
        );
    }

    private static Settings gated() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
