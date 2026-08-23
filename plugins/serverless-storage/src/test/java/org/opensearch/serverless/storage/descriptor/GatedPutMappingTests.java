/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The put-mapping protocol, asserted where it now lives.
 *
 * <p>Two things merged into one class, because after the protocol moved they are the same subject. T50 --
 * a mapping written for a uuid whose tombstone is already durable must not land, and must not be strandable
 * the way T47's prune left it -- was {@code GatedMappingStrandedIndexTests} in {@code server/}, driving
 * {@code MetadataMappingService.putMappingExecutor.recordGatedMapping} directly. T13/T15's
 * representability cases were in {@code GatedIndexMappingUpdateTests}, driving the same method through
 * {@code putMapping}. Neither method exists any more: core hands the whole request to {@link
 * org.opensearch.cluster.metadata.ClaimedIndexLifecycle#putMapping} and this class is the implementation
 * they were both really about.
 *
 * <h2>Why this simulates the stale-cache node rather than deleting through the client</h2>
 *
 * The window T50 closes belongs to a node whose descriptor cache has not yet observed a deletion that
 * another node has already made durable: core decides "cluster state cannot serve this" purely from cluster
 * state absence, which is identical for a live gated index and a deleted one, and the uuid a put-mapping
 * carries was resolved from that stale cache before the request reached here. Driving that end to end
 * through a client would need two nodes disagreeing about the same name, which is what {@code
 * GatedMappingIndexLossIT} needs an internal cluster for. This test reaches the same code directly: a
 * descriptor supplier standing in for "what the descriptor plane says right now" is set to answer with a
 * tombstone, while the request carries the uuid a stale-cache node would still be using.
 *
 * <p>These no longer need a node at all. Driving them through {@code OpenSearchSingleNodeTestCase} and
 * {@code getInstanceFromNode(MetadataMappingService.class)} was the only way to reach the protocol while it
 * was a package-private method on a Guice-constructed core service; it is an ordinary object now.
 */
public class GatedPutMappingTests extends OpenSearchTestCase {

    private final DescriptorBackedIndexLifecycle lifecycle = new DescriptorBackedIndexLifecycle();

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
            List.of(),
            Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            0L
        );
    }

    private static List<Index> one(String name, String uuid) {
        return List.of(new Index(name, uuid));
    }

    /** Applies a mapping and waits for the stage, so a failure surfaces as one rather than as a silent stage. */
    private void apply(List<Index> indices, String source) throws Exception {
        lifecycle.putMapping(indices, source).toCompletableFuture().get();
    }

    private Throwable failureOf(List<Index> indices, String source) {
        ExecutionException wrapped = expectThrows(
            ExecutionException.class,
            () -> lifecycle.putMapping(indices, source).toCompletableFuture().get()
        );
        return wrapped.getCause();
    }

    private static String mapping(String field) {
        return "{ \"properties\": { \"" + field + "\": { \"type\": \"keyword\" }}}";
    }

    /**
     * The control. Without it, tightening the guard until every put-mapping is refused would satisfy the
     * failing case below while switching the write path off entirely.
     */
    public void testAMappingWriteForAUuidTheDescriptorStillResolvesAsLiveSucceeds() throws Exception {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);

        String name = "gated-live";
        String uuid = "live-uuid";
        AbsentIndexDescriptorSuppliers.register(candidate -> name.equals(candidate) ? liveDescriptor(name, uuid) : null);

        apply(one(name, uuid), mapping("tenant"));

        MappingGenerationStore.MappingGeneration recorded = store.read(uuid);
        assertNotNull("a live index's mapping must reach the store", recorded);
        assertEquals("keyword", MappingGenerationStore.typeOf(recorded.fields().get("tenant")));
    }

    /**
     * The failing case T50 exists for: the uuid this request carries was resolved before the tombstone
     * became durable, and the descriptor plane now says the name is gone. The write must be refused rather
     * than recreating the document T47's deletion-time prune already removed.
     */
    public void testAMappingWriteForATombstonedUuidIsRefused() {
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

        Throwable failure = failureOf(one(name, staleUuid), mapping("tenant"));
        assertThat(failure, instanceOf(IndexNotFoundException.class));
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
     * behaves as it did before T50. Several tests exercise {@link MappingGenerationStore} on its own,
     * without installing {@link DescriptorGate}, and this is what keeps them unaffected -- {@code
     * DescriptorGate.install} always registers the descriptor supplier and the mapping store together, so a
     * production node that reaches this guard by way of the store also has resolution to consult.
     */
    public void testWithNoDescriptorResolutionRegisteredTheWriteIsUnaffected() throws Exception {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);
        assertFalse(AbsentIndexDescriptorSuppliers.isRegistered());

        apply(one("gated-no-descriptor-plane", "some-uuid"), mapping("tenant"));

        assertNotNull(store.read("some-uuid"));
    }

    /**
     * A resolver bug must degrade, not fail the write. {@link AbsentIndexDescriptorSuppliers#supply} treats
     * null as "no answer" and swallows any failure that is not a {@code DescriptorUnavailableException} into
     * it -- every other caller of the seam relies on that to keep a plugin bug from becoming a request
     * failure. Refusing on null here as well as on a confirmed tombstone would reintroduce exactly that
     * failure mode, and it would do so silently: this uuid was never tombstoned, the resolver simply could
     * not answer.
     */
    public void testANullResolverAnswerDoesNotRefuseTheWrite() throws Exception {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);

        // Registered and answering null for every name, as AbsentIndexDescriptorSuppliers.supply degrades an
        // ordinary resolver bug to, rather than the tombstone the failing test above registers.
        AbsentIndexDescriptorSuppliers.register(candidate -> null);

        String uuid = "uuid-behind-a-resolver-that-cannot-answer";
        apply(one("gated-unresolvable", uuid), mapping("tenant"));

        assertNotNull("a live index must not be refused because the resolver had no answer", store.read(uuid));
    }

    /** A second field from another shard must join the first rather than replace it. */
    public void testASecondFieldJoinsTheFirst() throws Exception {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);

        apply(one("gated-idx", "gated-uuid"), "{\"properties\":{\"age\":{\"type\":\"long\"}}}");
        apply(one("gated-idx", "gated-uuid"), "{\"properties\":{\"city\":{\"type\":\"keyword\"}}}");

        assertEquals("both fields must survive", 2, store.read("gated-uuid").fields().size());
    }

    /**
     * A put-mapping the store cannot carry must fail the request rather than record part of it.
     *
     * <p>The extraction here used to skip any property it could not read, record the rest and report
     * success. So a declaration added to a gated index was acknowledged and silently absent, while the same
     * declaration at creation was refused and kept the index in cluster state -- the "gated at creation and
     * refused on update, or the reverse" that the shared extractor exists to make impossible.
     *
     * <p>The case is a shorthand definition rather than the object field it was when T13 wrote it, because
     * T15 widened the store to hold whole definitions and object fields now carry. What remains
     * unrepresentable is a definition that is not an object at all.
     */
    public void testAPutMappingTheStoreCannotCarryIsRefused() {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);

        Throwable failure = failureOf(one("gated-idx", "gated-uuid"), "{\"properties\":{\"profile\":\"keyword\"}}");
        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertNull("nothing may be recorded, because a partial record is the loss this refuses", store.read("gated-uuid"));
    }

    /**
     * A field parameter is carried rather than refused, which is what T15 changed.
     *
     * <p>This asserted a refusal when T13 wrote it, and the refusal was correct at the time: the store held
     * a flat name-to-type map, so {@code ignore_above} sitting beside a perfectly good {@code keyword} had
     * nowhere to go. The guard before that asked whether a type was declared -- true here -- rather than
     * whether the definition was nothing but the type, which is why the parameter was dropped invisibly.
     *
     * <p>Now the stored value is the definition, so the assertion is that the parameter arrived, not that
     * the request was rejected. Asserting only that the field is present would pass against the old
     * behaviour, which stored the type and dropped the rest.
     */
    public void testAFieldParameterIsCarriedIntoTheStore() throws Exception {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);

        apply(one("gated-idx", "gated-uuid"), "{\"properties\":{\"code\":{\"type\":\"keyword\",\"ignore_above\":256}}}");

        Map<String, Object> definition = MappingGenerationStore.definition(store.read("gated-uuid").fields().get("code"));
        assertNotNull("the field must have reached the store", definition);
        assertEquals("keyword", definition.get("type"));
        assertEquals("and its parameter with it, or the store reduced it to a type again", 256, definition.get("ignore_above"));
    }

    /**
     * An object field is carried too, with its sub-properties.
     *
     * <p>The limitation that forced every refusal in this area was the store's flat name-to-type map, and a
     * property with its own properties was the case that could not be squeezed into it at all. Kept as a
     * separate case from the parameter above because they were unrepresentable for different reasons, and a
     * widening that handled one and not the other would leave the feature half-usable.
     */
    public void testAnObjectFieldIsCarriedIntoTheStore() throws Exception {
        RecordingStore store = new RecordingStore();
        MappingGenerationStore.register(store);

        apply(one("gated-idx", "gated-uuid"), "{\"properties\":{\"profile\":{\"properties\":{\"city\":{\"type\":\"keyword\"}}}}}");

        Map<String, Object> definition = MappingGenerationStore.definition(store.read("gated-uuid").fields().get("profile"));
        assertNotNull("the object field must have reached the store", definition);
        assertTrue("with its sub-properties, or carrying it bought nothing: " + definition, definition.containsKey("properties"));
    }

    /**
     * With no store installed this fails rather than acknowledging, which is the same answer core's own
     * {@code ClaimedIndexLifecycle.NOOP} gives on a node with no plugin: nothing here holds the index, so
     * there is nowhere for the change to be recorded and the ordinary path's {@link IndexNotFoundException}
     * is the honest outcome.
     */
    public void testWithNoStoreInstalledThePutMappingIsNotFound() {
        assertFalse(MappingGenerationStore.isRegistered());

        Throwable failure = failureOf(one("gated-idx", "gated-uuid"), mapping("tenant"));
        assertThat(failure, instanceOf(IndexNotFoundException.class));
        assertThat(failure.getMessage(), containsString("gated-idx"));
    }
}
