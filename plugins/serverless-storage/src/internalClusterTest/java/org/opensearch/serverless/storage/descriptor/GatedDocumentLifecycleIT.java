/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The whole document lifecycle on a gated index, against an ordinary index running the identical sequence.
 *
 * <h2>Why this exists</h2>
 *
 * Asked plainly -- is a gated index writable, updatable, deletable and searchable like a normal one? -- the
 * suite could not answer. Creation, get-by-id, bulk indexing and search were each covered somewhere, but a
 * survey of every integration test found that **no test had ever issued a document update or a document
 * delete against a gated index**: every {@code prepareUpdate} in the suite was {@code prepareUpdateSettings},
 * and every {@code prepareDelete} was {@code admin().indices().prepareDelete}, which deletes the index. Two of
 * the four operations in the question had no coverage at all.
 *
 * <p>That is the shape of gap this area keeps producing, and it is worth naming: each operation looked
 * covered from the outside, because a file mentioning "update" and a gated index was in the list.
 *
 * <h2>Why the control is the assertion</h2>
 *
 * "Like a normal index" is a comparison, so this makes the comparison rather than encoding what normal is
 * believed to be. The same sequence runs against a gated index and an ordinary one, each observation is
 * recorded, and the two lists must be equal. A change in engine behaviour that alters both equally is not
 * this test's business; one that alters only the gated side is exactly what it is for.
 *
 * <p>Update is the operation most likely to break here and the reason this is not just a smoke test. It is a
 * read-then-write against the live index -- a realtime get that must see a document not yet refreshed, then a
 * write conditioned on the sequence number that get returned. A gated index reaches its shard through
 * computed placement and opens it on demand, so both halves travel a different road than an ordinary index's.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GatedDocumentLifecycleIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

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
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath().toString())
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    public void testAGatedIndexSupportsTheSameDocumentLifecycleAsAnOrdinaryOne() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        assertTrue(
            client().admin()
                .indices()
                .create(new CreateIndexRequest("serverless_lifecycle").settings(serverlessStorage()))
                .actionGet()
                .isAcknowledged()
        );
        assertTrue(
            client().admin().indices().create(new CreateIndexRequest("ordinary-lifecycle").settings(plain())).actionGet().isAcknowledged()
        );

        List<String> gated = lifecycleOf("serverless_lifecycle");
        List<String> ordinary = lifecycleOf("ordinary-lifecycle");

        for (int i = 0; i < ordinary.size(); i++) {
            assertEquals("step " + i + " must behave the same on a gated index as on an ordinary one", ordinary.get(i), gated.get(i));
        }
        // The count too, so a sequence that stopped early on one side cannot pass by matching a prefix.
        assertEquals("both sides must have completed every step", ordinary.size(), gated.size());
        logger.info("document lifecycle, identical on both planes: {}", gated);

        // And the gated index must still be gated, or all of the above was measured on two ordinary indices.
        assertNull(
            "the gated arm must have no cluster state entry, or this test proved nothing about gating",
            client().admin().cluster().prepareState().get().getState().metadata().index("serverless_lifecycle")
        );
    }

    /**
     * One pass of the lifecycle, returning what each step observed rather than asserting it, so the two
     * planes can be compared against each other instead of against expectations written here.
     */
    private List<String> lifecycleOf(String index) throws Exception {
        List<String> observed = new ArrayList<>();

        // A gated index opens its shard on demand, so the first write is the one that may have to wait.
        // Bounded rather than unbounded: "not open yet" and "never opening" are indistinguishable to a write.
        assertBusy(() -> client().prepareIndex(index).setId("1").setSource("name", "first", "n", 1).get(), 60, TimeUnit.SECONDS);

        IndexResponse indexed = client().prepareIndex(index).setId("2").setSource("name", "second", "n", 2).get();
        observed.add("index=" + indexed.getResult());

        // Realtime get: not refreshed yet, so this must come from the translog.
        GetResponse got = client().prepareGet(index, "2").get();
        observed.add("get.exists=" + got.isExists() + " name=" + got.getSourceAsMap().get("name"));

        // Update, the operation that had no coverage at all. A partial document merge, which is a realtime
        // get followed by a write conditioned on what that get returned.
        UpdateResponse updated = client().prepareUpdate(index, "2").setDoc(Map.of("name", "second-updated")).get();
        observed.add("update=" + updated.getResult());
        GetResponse afterUpdate = client().prepareGet(index, "2").get();
        observed.add("update.merged name=" + afterUpdate.getSourceAsMap().get("name") + " n=" + afterUpdate.getSourceAsMap().get("n"));

        // Upsert: the same call against a document that does not exist must create it.
        UpdateResponse upserted = client().prepareUpdate(index, "3").setDoc(Map.of("name", "third", "n", 3)).setDocAsUpsert(true).get();
        observed.add("upsert=" + upserted.getResult());

        // Optimistic concurrency, which is what an update is built on: a write against a stale sequence
        // number must be refused rather than silently applied.
        long staleSeqNo = afterUpdate.getSeqNo() - 1;
        try {
            client().prepareIndex(index)
                .setId("2")
                .setSource("name", "conflicting")
                .setIfSeqNo(staleSeqNo < 0 ? 0 : staleSeqNo)
                .setIfPrimaryTerm(afterUpdate.getPrimaryTerm())
                .get();
            observed.add("staleWrite=accepted");
        } catch (VersionConflictEngineException e) {
            observed.add("staleWrite=refused");
        }

        // Bulk, mixing the three write kinds in one request, which is how a real client uses this.
        BulkResponse bulk = client().prepareBulk()
            .add(client().prepareIndex(index).setId("4").setSource("name", "fourth", "n", 4))
            .add(client().prepareUpdate(index, "3").setDoc(Map.of("name", "third-updated")))
            .add(client().prepareDelete(index, "1"))
            .get();
        observed.add("bulk.failures=" + bulk.hasFailures());
        for (var item : bulk.getItems()) {
            observed.add(
                "bulk[" + item.getId() + "]=" + (item.isFailed() ? "FAILED:" + item.getFailureMessage() : item.getResponse().getResult())
            );
        }

        // Delete, the other operation that had no coverage.
        DocWriteResponse deleted = client().prepareDelete(index, "4").get();
        observed.add("delete=" + deleted.getResult());
        observed.add("get.afterDelete.exists=" + client().prepareGet(index, "4").get().isExists());
        observed.add("delete.missing=" + client().prepareDelete(index, "does-not-exist").get().getResult());

        // Search sees exactly what the writes left behind: 1 deleted by bulk, 4 deleted above, so 2 and 3.
        client().admin().indices().prepareRefresh(index).get();
        SearchResponse search = client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).get();
        observed.add("search.hits=" + search.getHits().getTotalHits().value());
        List<String> names = new ArrayList<>();
        for (var hit : search.getHits().getHits()) {
            names.add(String.valueOf(hit.getSourceAsMap().get("name")));
        }
        names.sort(String::compareTo);
        observed.add("search.names=" + names);

        // A phrase query rather than a match, and the difference is the assertion. "name" is analysed, so a
        // match for "second-updated" also matches "third-updated" on the shared token and answers 2 -- which
        // is equal on both planes and therefore passes while asserting nothing about the updated value being
        // indexed. The phrase picks out the one document, so this fails if an update wrote the new source
        // without reindexing it. (A query for the *old* value proves nothing either way: the updated document
        // still contains that token, so a stale index and a correct one both answer one.)
        SearchResponse phrase = client().prepareSearch(index).setQuery(QueryBuilders.matchPhraseQuery("name", "second updated")).get();
        observed.add("search.updatedValue.hits=" + phrase.getHits().getTotalHits().value());

        return observed;
    }

    private static Settings serverlessStorage() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }

    private static Settings plain() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
    }
}
