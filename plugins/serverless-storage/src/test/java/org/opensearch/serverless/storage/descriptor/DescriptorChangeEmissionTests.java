/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexDescriptorPublisher;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.UnknownFieldRefresh;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/**
 * Every descriptor write must put an entry in the change log, because the cache's window assumes it does.
 *
 * <h2>Why this is a property worth its own suite</h2>
 *
 * {@code BlobDescriptorBackend}'s freshness window is sixty seconds, and its javadoc justifies that number
 * by saying the window is "a backstop for a change log entry that was lost, not the mechanism by which
 * deletes become visible". That argument holds only if every change produces an entry. Two of the five
 * descriptor write paths recorded one. The other three -- creation, the mapping compare-and-swap, and the
 * tombstone -- recorded nothing, so the window was not a backstop but the only mechanism, and for a delete
 * that meant every other node in the cluster went on resolving a deleted index as live for a minute and
 * accepting acknowledged writes against it.
 *
 * <p>None of that was visible from any one of those paths. It is visible from here, which is why the test
 * is stated over the set of paths rather than inside each of them.
 */
public class DescriptorChangeEmissionTests extends OpenSearchTestCase {

    private FsBlobStore descriptorStore;
    private BlobDescriptorBackend backend;
    private BlobDescriptorChangeLog changeLog;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        descriptorStore = new FsBlobStore(1024, createTempDir(), false);
        backend = new BlobDescriptorBackend(descriptorStore.blobContainer(BlobPath.cleanPath()));
        FsBlobStore logStore = new FsBlobStore(1024, createTempDir(), false);
        changeLog = new BlobDescriptorChangeLog(logStore::blobContainer, BlobPath.cleanPath());
        DescriptorGate.install(
            backend,
            mock(DescriptorPrefixBackend.class),
            mock(MappingGenerationStore.Store.class),
            mock(GatedMappingStatsAggregator.Aggregator.class),
            mock(UnknownFieldRefresh.Refresher.class),
            true
        );
        DescriptorGate.setChangeFeed(changeLog);
    }

    @After
    public void alwaysUninstall() {
        DescriptorGate.uninstall();
    }

    private static IndexMetadata metadata(String name) {
        return metadata(name, name + "-uuid");
    }

    private static IndexMetadata metadata(String name, String uuid) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    /** The kinds recorded for a name, which is what a tailer on another node would see. */
    private EnumSet<DescriptorChange.Kind> kindsFor(String name) throws Exception {
        EnumSet<DescriptorChange.Kind> kinds = EnumSet.noneOf(DescriptorChange.Kind.class);
        assertBusy(() -> {
            EnumSet<DescriptorChange.Kind> seen = EnumSet.noneOf(DescriptorChange.Kind.class);
            for (DescriptorChange change : changeLog.since(null)) {
                if (change.name().equals(name)) {
                    seen.add(change.kind());
                }
            }
            assertFalse("nothing was recorded for [" + name + "] at all", seen.isEmpty());
            kinds.addAll(seen);
        }, 10, TimeUnit.SECONDS);
        return kinds;
    }

    /**
     * A gated creation tells the rest of the cluster the name exists.
     *
     * <p>Mostly harmless while a name has never been used, since absence is not cached. Not harmless after a
     * delete: a node that resolved the name while it was deleted holds the tombstone in its cache -- the
     * store answers a deleted name with one deliberately -- and with nothing recorded it answers "deleted"
     * for the whole freshness window after the index has been recreated.
     */
    public void testAGatedCreationIsRecorded() throws Exception {
        CompletableFuture<Boolean> created = IndexDescriptorPublisher.createGated(metadata("serverless_tenant-new"));
        assertNotNull("a creator must be installed, or nothing records the index at all", created);
        assertTrue(created.get(10, TimeUnit.SECONDS));

        assertEquals(EnumSet.of(DescriptorChange.Kind.CREATED), kindsFor("serverless_tenant-new"));
    }

    /** A creation that lost the race records nothing, because it does not hold the name. */
    public void testACreationThatLostTheRaceRecordsNothing() throws Exception {
        assertTrue(IndexDescriptorPublisher.createGated(metadata("serverless_tenant-contested")).get(10, TimeUnit.SECONDS));
        assertEquals(EnumSet.of(DescriptorChange.Kind.CREATED), kindsFor("serverless_tenant-contested"));

        IndexMetadata sameNameDifferentIndex = metadata("serverless_tenant-contested", "somebody-elses-uuid");
        assertFalse(IndexDescriptorPublisher.createGated(sameNameDifferentIndex).get(10, TimeUnit.SECONDS));

        long recorded = changeLog.since(null).stream().filter(change -> change.name().equals("serverless_tenant-contested")).count();
        assertEquals("only the winner may record, or every node invalidates on behalf of a name it lost", 1, recorded);
    }

    /**
     * A gated delete tells the rest of the cluster the index is gone, which it never did.
     *
     * <p>The publisher has an {@code exists()==false} branch that records a DELETED and nothing reaches it:
     * {@code publish} is only called for an index that is in cluster state, and an all-gated delete does not
     * go through a cluster state update at all. So no node was told. Every other node kept serving the live
     * descriptor from cache for the freshness window and accepted acknowledged writes against a deleted
     * index, and {@code DescriptorChangeTailer}'s delete-driven shard release was unreachable in production.
     */
    public void testAGatedDeleteIsRecordedAsDeleted() throws Exception {
        assertTrue(IndexDescriptorPublisher.createGated(metadata("serverless_tenant-doomed")).get(10, TimeUnit.SECONDS));

        new DescriptorBackedIndexLifecycle().removeIndices(List.of(metadata("serverless_tenant-doomed")))
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

        assertTrue(
            "a delete must be recorded, or other nodes serve the live descriptor until their window expires",
            kindsFor("serverless_tenant-doomed").contains(DescriptorChange.Kind.DELETED)
        );
    }

    /** A close is recorded as a close, so a node holding the shard is told to let it go. */
    public void testAGatedCloseIsRecordedAsClosed() throws Exception {
        assertTrue(IndexDescriptorPublisher.createGated(metadata("serverless_tenant-closing")).get(10, TimeUnit.SECONDS));

        assertTrue(
            IndexDescriptorPublisher.updateGated("serverless_tenant-closing", current -> current.withState(IndexDescriptor.State.CLOSE))
                .get(10, TimeUnit.SECONDS)
        );

        assertTrue(kindsFor("serverless_tenant-closing").contains(DescriptorChange.Kind.CLOSED));
        assertEquals(IndexDescriptor.State.CLOSE, backend.get("serverless_tenant-closing").state());
    }

    /**
     * An update that changes nothing writes nothing and records nothing.
     *
     * <p>Closing an index that is already closed is a request that has already been satisfied, and a write
     * for it would invalidate a live cache entry on every node for no change at all.
     */
    public void testAnUpdateThatChangesNothingRecordsNothing() throws Exception {
        assertTrue(IndexDescriptorPublisher.createGated(metadata("serverless_tenant-idle")).get(10, TimeUnit.SECONDS));
        assertEquals(EnumSet.of(DescriptorChange.Kind.CREATED), kindsFor("serverless_tenant-idle"));
        int before = changeLog.since(null).size();

        assertTrue(IndexDescriptorPublisher.updateGated("serverless_tenant-idle", current -> current).get(10, TimeUnit.SECONDS));

        assertEquals("an already-satisfied request must not produce an entry", before, changeLog.since(null).size());
    }

    /** An update against a name that is not there fails rather than resurrecting it. */
    public void testUpdatingADeletedIndexFailsRatherThanRecreatingIt() throws Exception {
        CompletableFuture<Boolean> update = IndexDescriptorPublisher.updateGated(
            "serverless_tenant-never-existed",
            current -> current.withState(IndexDescriptor.State.CLOSE)
        );

        Exception e = expectThrows(Exception.class, () -> update.get(10, TimeUnit.SECONDS));
        assertTrue(
            e.getCause() == null ? e.toString() : e.getCause().toString(),
            e.getCause() instanceof org.opensearch.index.IndexNotFoundException
        );
        assertNull(backend.get("serverless_tenant-never-existed"));
    }

    /**
     * A dynamic field added through the mapping store is recorded too, which is the fifth path.
     *
     * <p>Without it, every other node's cached descriptor goes on claiming the previous mapping generation
     * for the whole window, so a shard asking whether it is behind is told it is not.
     */
    public void testAMappingSwapIsRecorded() throws Exception {
        assertTrue(IndexDescriptorPublisher.createGated(metadata("serverless_tenant-mapped")).get(10, TimeUnit.SECONDS));
        DescriptorBackedMappingStore mappingStore = new DescriptorBackedMappingStore(() -> backend, null);
        DescriptorBackedMappingStore.registerDescriptor(backend.get("serverless_tenant-mapped"));

        assertTrue(
            mappingStore.compareAndSwap(
                "serverless_tenant-mapped-uuid",
                0L,
                new MappingGenerationStore.MappingGeneration(1L, Map.of("age", Map.of("type", "long")))
            )
        );

        assertTrue(kindsFor("serverless_tenant-mapped").contains(DescriptorChange.Kind.UPDATED));
    }
}
