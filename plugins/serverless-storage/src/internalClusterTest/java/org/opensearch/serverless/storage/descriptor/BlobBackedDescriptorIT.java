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
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;

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
            // Deliberately not the default of 5, so the test below can tell a configured value from an
            // unconsulted one. This class is the only place the plugin's own install path runs, and that
            // path is the only thing that reads the setting.
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_MAPPING_INDEX_SHARDS_SETTING.getKey(), 3)
            // This class used to select the blob backend explicitly here. There is no longer a choice to
            // make: the object store is the only descriptor backend, so what the class asserts is now the
            // only arrangement there is.
            // Without this a gated index has no routing table at all, since computed placement supplies
            // one and is off by default. T36 found every earlier run of the sibling class lacked it.
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            // A gated delete tells no node anything, so the shard it opened on demand is reclaimed by a
            // sweep. The production default is a minute; a second here so a test does not spend one waiting
            // for a reclamation whose latency is not what it is testing.
            .put(org.opensearch.serverless.storage.ServerlessStoragePlugin.GATED_SHARD_SWEEP_INTERVAL_SETTING.getKey(), "1s")
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
        awaitGatedShardsClosed();
        created.clear();
    }

    /**
     * Waits until no node still holds an index service for a deleted gated index.
     *
     * <p>Asserted rather than left to the framework's five second lock wait, because the two failures read
     * identically and mean opposite things: a shard that has not been reclaimed yet and one that never will
     * be both surface as "still locked". This names which one it is, and it is the only assertion in the
     * class that covers the deletion path reaching the node that holds the data.
     */
    private void awaitGatedShardsClosed() throws Exception {
        assertBusy(() -> {
            for (org.opensearch.indices.IndicesService indices : internalCluster().getInstances(
                org.opensearch.indices.IndicesService.class
            )) {
                for (String name : created) {
                    for (org.opensearch.index.IndexService indexService : indices) {
                        assertNotEquals(
                            "[" + name + "] is deleted and a node still holds its index service, so its shards leak",
                            name,
                            indexService.index().getName()
                        );
                    }
                }
            }
        }, 30, TimeUnit.SECONDS);
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
                // Read through the installed supplier rather than out of an index. There is one keyspace
                // now, so "the write landed" is answerable at the same place a request would ask -- and a
                // tombstone is readable there by design, because a get falls back to the tombstone prefix
                // when the live key is gone.
                org.opensearch.cluster.metadata.IndexDescriptor descriptor = org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers
                    .supply(name);
                assertNotNull("[" + name + "] has no descriptor yet, so a write is still in flight", descriptor);
                assertFalse(
                    "[" + name + "] is recorded but not yet tombstoned, so the tombstone write is still in flight",
                    descriptor.exists()
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
        createGated("serverless_tenant-alpha");
        assertGated("serverless_tenant-alpha");
        awaitDescriptor("serverless_tenant-alpha");

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 25; i++) {
            bulk.add(client().prepareIndex("serverless_tenant-alpha").setId("doc-" + i).setSource("value", i));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());

        client().admin().indices().prepareRefresh("serverless_tenant-alpha").get();
        SearchResponse search = client().prepareSearch("serverless_tenant-alpha").setQuery(QueryBuilders.matchAllQuery()).setSize(0).get();
        assertEquals(25, search.getHits().getTotalHits().value());
    }

    /**
     * A get by id, which is the one read path the other tests here never exercise.
     *
     * <p>G9's residency audit found {@code TransportGetAction.getExecutor} calling
     * {@code metadata().getIndexSafe(shardId.getIndex())} purely to decide which thread pool to use. For a
     * gated index that is an index cluster state has no entry for, so the accessor that throws on absence is
     * consulted on every single-document get. Bulk and search were covered and this was not, which is why it
     * survived: the failure needs a get by id specifically.
     */
    public void testAGatedIndexCanBeReadByIdAndNotJustSearched() throws Exception {
        createGated("serverless_tenant-getbyid");
        assertGated("serverless_tenant-getbyid");
        awaitDescriptor("serverless_tenant-getbyid");

        client().prepareIndex("serverless_tenant-getbyid").setId("doc-1").setSource("value", 7).get();
        client().admin().indices().prepareRefresh("serverless_tenant-getbyid").get();

        org.opensearch.action.get.GetResponse response = client().prepareGet("serverless_tenant-getbyid", "doc-1").get();
        assertTrue("a document written to a gated index must be readable by id", response.isExists());
        assertEquals(7, response.getSourceAsMap().get("value"));
    }

    /**
     * The three operations that a gated index cannot support must say so, rather than say "no such index".
     *
     * <p>G9's audit found all three reaching {@code Metadata.getIndexSafe} unguarded, which fails with
     * "no such index" for an index that exists perfectly well and is simply not in cluster state. That
     * message sends an operator looking for a deleted index rather than telling them the operation is
     * unsupported, which is a worse failure than the unsupported operation itself.
     *
     * <p>Update-settings is the one that cannot be implemented rather than merely is not: a descriptor
     * carries no arbitrary settings, so there is nowhere to put them. Open and close are missing features,
     * since {@code IndexDescriptor.State} can represent CLOSE.
     */
    public void testOperationsAGatedIndexCannotSupportFailClearly() throws Exception {
        createGated("serverless_tenant-unsupported");
        assertGated("serverless_tenant-unsupported");
        awaitDescriptor("serverless_tenant-unsupported");

        Exception settings = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("serverless_tenant-unsupported")
                .setSettings(Settings.builder().put("index.number_of_replicas", 1))
                .get()
        );
        assertThat(
            "an operator must be told the operation is unsupported, not that the index is missing",
            settings.getMessage() + String.valueOf(settings.getCause()),
            containsString("serverless")
        );

        // Close and open are no longer in this list, and the reason is the point of the test rather than an
        // exception to it. They stopped being unsupported when they became expressible: IndexDescriptor
        // carries State, so MetadataIndexStateService's gated paths write descriptor.withState(CLOSE) and
        // withState(OPEN) and mean it. Settings has no such home, which is why it is still refused above.
        //
        // "Unsupported" therefore has to mean "cannot be represented", not "has no cluster state entry", or
        // this test would freeze the feature set at whatever existed the day it was written -- which is what
        // it had been doing: both of these were implemented and the assertion that they fail was left
        // standing, so the suite has been red here rather than telling anyone the capability arrived.
        assertTrue(client().admin().indices().prepareClose("serverless_tenant-unsupported").get().isAcknowledged());
        assertBusy(
            () -> assertEquals(IndexDescriptor.State.CLOSE, descriptorOf("serverless_tenant-unsupported").state()),
            30,
            TimeUnit.SECONDS
        );

        assertTrue(client().admin().indices().prepareOpen("serverless_tenant-unsupported").get().isAcknowledged());
        assertBusy(
            () -> assertEquals(IndexDescriptor.State.OPEN, descriptorOf("serverless_tenant-unsupported").state()),
            30,
            TimeUnit.SECONDS
        );
    }

    /**
     * The descriptor as it currently stands in the object store.
     *
     * <p>Read through the resolution seam rather than from a cached copy, so a state change that was written
     * but never became visible fails this rather than passing on a stale read.
     */
    private IndexDescriptor descriptorOf(String name) {
        IndexDescriptor descriptor = AbsentIndexDescriptorSuppliers.supply(name);
        assertNotNull("[" + name + "] no longer resolves through the descriptor seam", descriptor);
        return descriptor;
    }

    /**
     * Reads the descriptor straight out of the object store, so the claim is about where the bytes are and
     * not merely about whether a request succeeded. A request succeeding proves the system index could
     * have served it too.
     */
    public void testTheDescriptorReallyLivesInTheObjectStore() throws Exception {
        createGated("serverless_tenant-bravo");
        assertGated("serverless_tenant-bravo");

        org.opensearch.common.blobstore.fs.FsBlobStore store = new org.opensearch.common.blobstore.fs.FsBlobStore(1024, basePath(), false);
        DescriptorEnumerator enumerator = new DescriptorEnumerator(
            store::blobContainer,
            org.opensearch.common.blobstore.BlobPath.cleanPath().add("descriptors-root")
        );

        assertTrue(
            "the descriptor must be readable from the object store alone, which is invariant I2",
            enumerator.allNames().contains("serverless_tenant-bravo")
        );
    }

    /**
     * Wildcards must keep working while point reads move, which is the whole reason the backend is a
     * composite rather than a swap. T3 established a bucket cannot serve a prefix search; if the switch
     * had simply replaced the store, this would return nothing and would do so without an error.
     */
    public void testWildcardsStillResolveWithPointReadsOnTheObjectStore() throws Exception {
        createGated("serverless_logs-one");
        createGated("serverless_logs-two");
        createGated("serverless_metrics-one");
        assertGated("serverless_logs-one");
        awaitDescriptor("serverless_logs-one");
        awaitDescriptor("serverless_logs-two");
        awaitDescriptor("serverless_metrics-one");

        // Retried rather than read once, because what a wildcard can see lags what a point read can. H18
        // reasoned about that as an index's refresh interval; since descriptors became blobs it is the
        // object store's list consistency instead, which is unmeasured and therefore not something to
        // assert in one read. awaitDescriptor above waits on the point half, and a single read here would
        // be asserting that a listing has already caught up, which nothing promises.
        assertBusy(() -> {
            SearchResponse search = client().prepareSearch("serverless_logs-*").setQuery(QueryBuilders.matchAllQuery()).setSize(0).get();
            assertEquals("a wildcard must reach both gated tenants and neither more nor fewer", 2, search.getTotalShards());
        }, 30, TimeUnit.SECONDS);
    }

    /** Deletion has to remove the name from the object store's live prefix, not merely from a request's view. */
    public void testDeletionRemovesTheNameFromTheObjectStore() throws Exception {
        createGated("serverless_tenant-doomed");
        assertGated("serverless_tenant-doomed");

        assertTrue(client().admin().indices().prepareDelete("serverless_tenant-doomed").get().isAcknowledged());

        org.opensearch.common.blobstore.fs.FsBlobStore store = new org.opensearch.common.blobstore.fs.FsBlobStore(1024, basePath(), false);
        DescriptorEnumerator enumerator = new DescriptorEnumerator(
            store::blobContainer,
            org.opensearch.common.blobstore.BlobPath.cleanPath().add("descriptors-root")
        );

        assertFalse(
            "a deleted name must leave the live prefix, or a rebuild resurrects it",
            enumerator.allNames().contains("serverless_tenant-doomed")
        );
    }

    /**
     * The configured shard count reaches the mapping index the plugin's own store creates.
     *
     * <p>Round 004's T45 made the count a setting, and the unit test for it constructs the store directly,
     * which proves the constructor argument is used and nothing about whether anything passes it. The wiring
     * runs only here: every other IT installs the gate by hand with a default-constructed store, so
     * reverting the plugin to `new IndexBackedMappingStore(client)` left the entire suite green. That is the
     * "configured, registered, and never consulted" shape this area has shipped before.
     */
    /**
     * The node setting decides the mapping index's geometry rather than the literal it replaced (T45).
     *
     * <p><b>Driven through the store rather than through a gated creation, and that changed for a reason.</b>
     * This used to create a gated index with a declared mapping and then read the geometry, which worked
     * while a creation wrote its mapping through {@code MappingGenerationStore}. Since T58 it does not: the
     * mapping travels inside the descriptor, and the mapping index is written behind the creation, off-thread,
     * only so the gated population stays aggregatable. Asserting geometry through that path measures a race
     * between an asynchronous projection and the test framework's own index wipe, not the setting.
     *
     * <p>So the store is asked to create it directly, which is exactly the code path the setting feeds.
     */
    public void testTheConfiguredMappingIndexShardCountIsUsed() throws Exception {
        new IndexBackedMappingStore(client(), ServerlessStoragePlugin.SERVERLESS_STORAGE_MAPPING_INDEX_SHARDS_SETTING.get(nodeSettings(0)))
            .compareAndSwap(
                "geometry-uuid",
                0L,
                new org.opensearch.cluster.metadata.MappingGenerationStore.MappingGeneration(
                    1L,
                    java.util.Map.of("tenant", java.util.Map.of("type", "keyword"))
                )
            );

        var settings = client().admin()
            .indices()
            .prepareGetSettings(org.opensearch.serverless.storage.descriptor.IndexBackedMappingStore.MAPPING_INDEX)
            .get();
        assertEquals(
            "the node setting must decide the mapping index's geometry, not the literal it replaced",
            "3",
            settings.getSetting(
                org.opensearch.serverless.storage.descriptor.IndexBackedMappingStore.MAPPING_INDEX,
                org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS
            )
        );
    }

}
