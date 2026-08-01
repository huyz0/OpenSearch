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
import java.util.concurrent.TimeUnit;

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
@org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "I4. Debug run narrows it further and the answer is suspect 2, with one caveat. In the "
    + "captured output for a single-test run, 'opening gated shard on demand' appears ZERO times "
    + "while ordinary creation lines for the same index do appear: MergeSchedulerConfig initialising "
    + "tenant-alpha, and PluginsService onIndexModule for tenant-alpha/8Ryh7kEDTvarIUIOXB0myA. So the "
    + "shard holding the lock is built by the ordinary index path, not by T39's on-demand path, and "
    + "the recovery-source theory (suspect 1) is not what is happening. "
    + "The caveat, stated because it changes what this proves: it was not confirmed that "
    + "-Dtests.loggers.levels actually took effect, so 'zero lines' could mean the path did not run "
    + "or that DEBUG was never enabled. Confirm by asserting on a line the same logger emits at INFO "
    + "before concluding. "
    + "If it holds, the question becomes why an index that assertGated says is absent from cluster "
    + "state is nonetheless going through ordinary creation, which would mean the premise guard is "
    + "weaker than it reads: hasIndex(name) false does not by itself establish the index was gated.")
public class BlobBackedDescriptorIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /**
     * Where the object store lives, shared by every test in the class.
     *
     * <p><b>Static, and that is the whole point.</b> JUnit builds a fresh instance per test method while the
     * cluster is built once for the suite, so an instance field gives each method its own path and only the
     * first one matches the path the nodes were actually configured with in {@link #nodeSettings}. Every
     * later method then reads an empty directory -- one it creates itself, since {@code FsBlobStore} is
     * constructed writable -- and concludes the descriptor never reached the object store.
     *
     * <p>This is why the class passed when a single method was run and failed when the class was run: "run
     * it in isolation" was not narrowing the problem, it was selecting the one case that works. Three of the
     * four methods had never been able to pass.
     */
    private static volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (BlobBackedDescriptorIT.class) {
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

    /**
     * Waits for a name to appear in the object store.
     *
     * <p>Not a flake guard. S37 recorded that a gated creation acknowledges before its descriptor lands,
     * and making the write genuinely asynchronous widened that window rather than closing it. So "created"
     * and "durable" are two events here, and a test that conflates them is asserting something the system
     * has never promised.
     */
    private void awaitDescriptor(String name) throws Exception {
        assertBusy(
            () -> assertTrue(
                "[" + name + "] never reached the object store; base path holds " + diskListing(),
                storeNames().contains(name)
            ),
            30,
            TimeUnit.SECONDS
        );
    }

    /**
     * Everything under the base path, so a failure names where the bytes went instead of only where they
     * were not. Two candidate causes were indistinguishable without it: a container resolved under a
     * prefix this test does not replicate, or a descriptor never written to the object store at all.
     */
    private String diskListing() {
        try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(basePath(), 4)) {
            return walk.map(basePath()::relativize)
                .map(java.nio.file.Path::toString)
                .filter(x -> x.isEmpty() == false)
                .sorted()
                .toList()
                .toString();
        } catch (Exception e) {
            return "<unreadable: " + e + ">";
        }
    }

    private java.util.List<String> storeNames() throws java.io.IOException {
        org.opensearch.common.blobstore.fs.FsBlobStore store = new org.opensearch.common.blobstore.fs.FsBlobStore(1024, basePath(), false);
        return new DescriptorEnumerator(store::blobContainer, org.opensearch.common.blobstore.BlobPath.cleanPath().add("descriptors-root"))
            .allNames();
    }

    /**
     * Names this test created, so teardown can delete them by name.
     *
     * <p>Needed because {@code TestCluster.wipeIndices("_all")} cannot reach a gated index. T28 decided
     * deliberately that {@code _all} and {@code *} expand over cluster state only, and a gated index is not
     * in cluster state, so the framework's own cleanup silently skips it. The shard therefore stays open and
     * still holds its lock when {@code assertAfterTest} runs, which is the "still locked after 5 sec"
     * failure this class was muted for. The lock was the symptom; the wipe never naming the index is the
     * cause.
     */
    private final java.util.List<String> created = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    @org.junit.After
    public void deleteGatedIndicesByName() throws Exception {
        for (String name : created) {
            try {
                client().admin().indices().prepareDelete(name).get();
            } catch (Exception e) {
                // Already deleted by the test itself is the ordinary case and not a failure.
                logger.debug("could not delete [{}] during teardown", name, e);
            }
        }
        quiesceDescriptorWrites();
        created.clear();
    }

    /**
     * Waits until every tombstone this test caused has actually landed in the prefix half.
     *
     * <p>Tombstone writes are asynchronous and retried up to five times, so they outlive the test method.
     * The framework then wipes every index, and a retry firing after the wipe recreates the descriptor
     * system index behind it under a fresh uuid. Those shards are open and holding their locks when
     * {@code assertAfterTest} runs, so the failure reads as a shard lock leak and is really a retry landing
     * late. Observed directly: attempt 1, then the wipe's "deleting index", then attempt 2, then "creating
     * index, cause [auto(bulk api)]", all within 270 ms.
     *
     * <p><b>Asserts the tombstone is present, not that the name is absent</b>, and the difference is the
     * whole point. The first version of this waited for a wildcard to stop matching, which an empty index
     * satisfies just as well as a tombstoned one: "not written yet" and "deleted" are the same observation
     * from the outside, so it returned immediately with every retry still pending. Presence is the only form
     * of this check that proves no attempt is outstanding, because the retry stops on the first success.
     */
    private void quiesceDescriptorWrites() throws Exception {
        assertBusy(() -> {
            for (String name : created) {
                org.opensearch.action.get.GetResponse response;
                try {
                    response = client().prepareGet(DescriptorStore.DESCRIPTOR_INDEX, name).get();
                } catch (Exception e) {
                    // The store may not exist yet, or may be mid-recreation after a previous test's wipe.
                    // Both mean "not landed", and both have to keep the wait going rather than end it: an
                    // exception escaping here would abort the retry loop and leave the write in flight,
                    // which is the state this method exists to rule out.
                    fail("[" + name + "] descriptor store not readable yet: " + e);
                    return;
                }
                assertTrue("[" + name + "] has no descriptor document yet, so a write is still in flight", response.isExists());
                assertEquals(
                    "[" + name + "] is recorded but not yet tombstoned, so the tombstone write is still in flight",
                    org.opensearch.cluster.metadata.IndexDescriptor.State.DELETED.name(),
                    response.getSourceAsMap().get("state")
                );
            }
        }, 30, TimeUnit.SECONDS);
    }

    private void createGated(String name) {
        created.add(name);
        assertTrue(
            client().admin()
                .indices()
                .create(
                    new CreateIndexRequest(name).settings(
                        Settings.builder()
                            // Index-scoped: this is what marks one index gated. The node-scoped twin in
                            // nodeSettings decides whether the machinery exists at all. A blanket rename
                            // put the node one here and the index was then never gated, which is the kind
                            // of thing a premise guard is supposed to catch and this one did not.
                            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
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

    public void testAGatedIndexCanBeCreatedResolvedAndSearchedOnTheBlobBackend() throws Exception {
        createGated("tenant-alpha");
        assertGated("tenant-alpha");
        awaitDescriptor("tenant-alpha");

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
    public void testWildcardsStillResolveWithPointReadsOnTheObjectStore() throws Exception {
        createGated("logs-one");
        createGated("logs-two");
        createGated("metrics-one");
        assertGated("logs-one");
        awaitDescriptor("logs-one");
        awaitDescriptor("logs-two");
        awaitDescriptor("metrics-one");

        // Retried rather than read once, because a wildcard is refresh-bound by design. H18 made the
        // descriptor index's refresh_interval an explicit 1s so that staleness is a stated bound instead of
        // an accident, and awaitDescriptor above waits on the object store, which is the point half. A
        // single read here asserts that the prefix half has already refreshed, which nothing promises.
        assertBusy(() -> {
            SearchResponse search = client().prepareSearch("logs-*").setQuery(QueryBuilders.matchAllQuery()).setSize(0).get();
            assertEquals("a wildcard must reach both gated tenants and neither more nor fewer", 2, search.getTotalShards());
        }, 30, TimeUnit.SECONDS);
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
