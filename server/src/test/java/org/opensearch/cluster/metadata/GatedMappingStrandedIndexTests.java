/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.containsString;

/**
 * T50: a mapping written for a uuid whose tombstone is already durable must not land, and must not be
 * strandable the way T47's prune left it.
 *
 * <h2>Why this simulates the stale-cache node rather than deleting through the client</h2>
 *
 * The window T50 closes belongs to a node whose descriptor cache has not yet observed a deletion that
 * another node has already made durable: {@code isGated} decides purely from cluster state absence, which
 * is identical for a live gated index and a deleted one, and the uuid a put-mapping carries was resolved
 * from that stale cache before the request reached here. Driving that end to end through a client would
 * need two nodes disagreeing about the same name, which is what {@code GatedMappingIndexLossIT} needs an
 * internal cluster for. This test reaches the same seam directly: a descriptor supplier standing in for
 * "what the descriptor plane says right now" is set to answer with a tombstone, while the request carries
 * the uuid a stale-cache node would still be using. That is exactly the disagreement {@code
 * refuseIfDescriptorShowsTheIndexIsGone} exists to catch, without needing a second node to produce it.
 */
public class GatedMappingStrandedIndexTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    @After
    public void clearRegistrations() {
        MappingGenerationStore.register(null);
        AbsentIndexDescriptorSuppliers.register(null);
    }

    /** An in-memory {@link MappingGenerationStore.Store}, so this test can see exactly what did or did not land. */
    private static final class RecordingStore implements MappingGenerationStore.Store {
        private final Map<String, MappingGenerationStore.MappingGeneration> byUuid = new ConcurrentHashMap<>();

        @Override
        public MappingGenerationStore.MappingGeneration read(String indexUuid) {
            return byUuid.get(indexUuid);
        }

        @Override
        public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
            MappingGenerationStore.MappingGeneration current = byUuid.get(indexUuid);
            long currentGeneration = current == null ? 0L : current.generation();
            if (currentGeneration != expectedGeneration) {
                return false;
            }
            byUuid.put(indexUuid, updated);
            return true;
        }

        @Override
        public void delete(String indexUuid) {
            byUuid.remove(indexUuid);
        }
    }

    private static IndexDescriptor liveDescriptor(String name, String uuid) {
        return new IndexDescriptor(
            name,
            uuid,
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            java.util.List.of(),
            org.opensearch.Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            0L
        );
    }

    private PutMappingClusterStateUpdateRequest putMappingRequest(String name, String uuid, String field) {
        PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            "{ \"properties\": { \"" + field + "\": { \"type\": \"keyword\" }}}"
        );
        request.indices(new Index[] { new Index(name, uuid) });
        return request;
    }

    /**
     * The control. Without it, tightening the guard until every gated put-mapping is refused would satisfy
     * the failing case below while switching the write path off entirely.
     */
    public void testAMappingWriteForAUuidTheDescriptorStillResolvesAsLiveSucceeds() throws Exception {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);

        String name = "gated-live";
        String uuid = "live-uuid";
        AbsentIndexDescriptorSuppliers.register(candidate -> name.equals(candidate) ? liveDescriptor(name, uuid) : null);

        MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        mappingService.putMappingExecutor.recordGatedMapping(putMappingRequest(name, uuid, "tenant"));

        MappingGenerationStore.MappingGeneration recorded = store.read(uuid);
        assertNotNull("a live index's mapping must reach the store", recorded);
        assertEquals("keyword", MappingGenerationStore.typeOf(recorded.fields().get("tenant")));
    }

    /**
     * The failing case T50 exists for: the uuid this request carries was resolved before the tombstone
     * became durable, and the descriptor plane now says the name is gone. The write must be refused rather
     * than recreating the document T47's deletion-time prune already removed.
     */
    public void testAMappingWriteForATombstonedUuidIsRefused() throws Exception {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);

        String name = "gated-deleted";
        String staleUuid = "stale-uuid-from-before-the-delete";
        // What the descriptor plane says right now: tombstoned, and under a different uuid than the stale
        // request carries -- exactly what a name recreated after deletion would show, and the sharper of
        // the two ways this can disagree, since it also exercises the uuid comparison rather than only
        // exists().
        AbsentIndexDescriptorSuppliers.register(
            candidate -> name.equals(candidate) ? liveDescriptor(name, "new-uuid-after-recreation").tombstoned() : null
        );

        MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        IndexNotFoundException failure = expectThrows(
            IndexNotFoundException.class,
            () -> mappingService.putMappingExecutor.recordGatedMapping(putMappingRequest(name, staleUuid, "tenant"))
        );
        assertThat(failure.getMessage(), containsString(name));

        // The write-side half of T50's read-side criterion: nothing was recorded under the stale uuid, so a
        // read for it -- which is exactly what a query against the still-resolving name would issue -- comes
        // back absent rather than a resurrected document. It does not by itself stop a *different* node's
        // query from answering "no fields" for a name whose descriptor generation says otherwise; that
        // distinction needs the descriptor's mappingGeneration consulted on the read path, which is T59.
        // What this establishes is the half T50 owns: the document that read would find is never written.
        assertNull(
            "a refused write must leave nothing behind for the stale uuid to be read back",
            MappingGenerationStore.currentMapping(staleUuid)
        );
    }

    /**
     * Backward-compatible fallback: when descriptor resolution is not registered at all, the write path
     * behaves as it did before T50. Several tests exercise {@code MappingGenerationStore} on its own,
     * without installing {@code DescriptorGate}, and this is what keeps them unaffected -- {@code
     * DescriptorGate.install} always registers the descriptor supplier and the mapping store together, so a
     * production node that reaches this guard by way of the store also has resolution to consult.
     */
    public void testWithNoDescriptorResolutionRegisteredTheWriteIsUnaffected() throws Exception {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);
        assertFalse(AbsentIndexDescriptorSuppliers.isRegistered());

        MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        mappingService.putMappingExecutor.recordGatedMapping(putMappingRequest("gated-no-descriptor-plane", "some-uuid", "tenant"));

        assertNotNull(store.read("some-uuid"));
    }

    /**
     * A resolver bug must degrade, not fail the write. {@code AbsentIndexDescriptorSuppliers#supply} treats
     * null as "no answer" and swallows any failure that is not a {@link DescriptorUnavailableException} into
     * it -- every other caller of the seam relies on that to keep a plugin bug from becoming a request
     * failure. Refusing on null here as well as on a confirmed tombstone would reintroduce exactly that
     * failure mode for {@code recordGatedMapping}, and it would do so silently: this uuid was never
     * tombstoned, the resolver simply could not answer.
     */
    public void testANullResolverAnswerDoesNotRefuseTheWrite() throws Exception {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);

        // Registered and answering null for every name, as AbsentIndexDescriptorSuppliers.supply degrades an
        // ordinary resolver bug to, rather than the tombstone the failing test above registers.
        AbsentIndexDescriptorSuppliers.register(candidate -> null);

        String uuid = "uuid-behind-a-resolver-that-cannot-answer";
        MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        mappingService.putMappingExecutor.recordGatedMapping(putMappingRequest("gated-unresolvable", uuid, "tenant"));

        assertNotNull("a live index must not be refused because the resolver had no answer", store.read(uuid));
    }
}
