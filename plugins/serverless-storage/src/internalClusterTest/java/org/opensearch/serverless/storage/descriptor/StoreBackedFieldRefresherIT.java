/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;
import org.junit.After;

import java.util.Map;

/**
 * W15. Making a field another shard inferred usable by this one.
 *
 * <p>W14 put the trigger at the moment a shard meets a field its mapping does not have. This is what has to
 * happen then, and the part that is easy to get wrong is that fetching is not enough: a field present in the
 * store but absent from the in-memory mapping cannot parse a document, so the refresher has to merge into
 * {@link MapperService} rather than merely read.
 *
 * <p>The properties asserted are H6c's claim restated for the new trigger, because that claim is the reason
 * the design pulls instead of broadcasting. A shard that already knows the field must do no read, and one
 * meeting a genuinely new field must not pay a merge per field. Asserted by counting rather than by
 * inspection, since counting is the only way to tell "did not need to" from "did and got the same answer".
 */
public class StoreBackedFieldRefresherIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final String INDEX = "refresh-target";

    @After
    public void clearRegistration() {
        MappingGenerationStore.register(null);
    }

    private MapperService mapperServiceFor(String index) {
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = indicesService.indexService(resolveIndex(index));
            if (indexService != null) {
                return indexService.mapperService();
            }
        }
        throw new AssertionError("no MapperService for [" + index + "]");
    }

    /** The property: a field only the store knows becomes usable on this shard. */
    public void testAFieldOnlyTheStoreKnowsBecomesUsable() {
        createIndex(INDEX);
        ensureGreen(INDEX);
        MapperService mapperService = mapperServiceFor(INDEX);
        String uuid = resolveIndex(INDEX).getUUID();

        MappingGenerationStore.register(new IndexBackedMappingStore(client()));
        MappingGenerationStore.updateMapping(uuid, Map.of("inferred_elsewhere", "keyword"));

        assertNull(
            "the premise: this shard does not know the field yet",
            mapperService.documentMapper() == null ? null : mapperService.documentMapper().mappers().getMapper("inferred_elsewhere")
        );

        boolean known = new StoreBackedFieldRefresher().refresh(mapperService, uuid, "inferred_elsewhere");

        assertTrue("a field another shard inferred must become usable here", known);
        assertNotNull(
            "and it must be in the in-memory mapping, since a field only in the store cannot parse a document",
            mapperService.documentMapper().mappers().getMapper("inferred_elsewhere")
        );
    }

    /** A field nobody has inferred is genuinely new, and the caller must be told so. */
    public void testAGenuinelyNewFieldIsReportedAbsent() {
        createIndex(INDEX);
        ensureGreen(INDEX);
        MappingGenerationStore.register(new IndexBackedMappingStore(client()));
        MappingGenerationStore.updateMapping(resolveIndex(INDEX).getUUID(), Map.of("something_else", "keyword"));

        boolean known = new StoreBackedFieldRefresher().refresh(mapperServiceFor(INDEX), resolveIndex(INDEX).getUUID(), "brand_new");

        assertFalse(
            "a field the store does not have either must be reported absent, so the caller infers or "
                + "rejects exactly as it would have without any of this",
            known
        );
    }

    /**
     * The cost property, at integration level. The read count itself is asserted by
     * {@code FieldRefresherReadCountTests}, which counts store reads directly; P3 found that this test's
     * earlier name claimed a no-refetch property it never observed, because it checked a boolean and a map
     * size rather than counting anything.
     */
    public void testFurtherUnknownFieldsAreReportedAbsentWithoutRemerging() {
        createIndex(INDEX);
        ensureGreen(INDEX);
        String uuid = resolveIndex(INDEX).getUUID();
        MappingGenerationStore.register(new IndexBackedMappingStore(client()));
        MappingGenerationStore.updateMapping(uuid, Map.of("known_field", "keyword"));

        StoreBackedFieldRefresher refresher = new StoreBackedFieldRefresher();
        MapperService mapperService = mapperServiceFor(INDEX);

        assertTrue("the first miss must merge", refresher.refresh(mapperService, uuid, "known_field"));
        assertEquals("and record the generation it merged", 1, refresher.trackedIndexCount());

        assertFalse(
            "a second unknown field at the same generation must be reported absent",
            refresher.refresh(mapperService, uuid, "another")
        );
        assertFalse("and a third", refresher.refresh(mapperService, uuid, "yet_another"));
        assertEquals("without tracking more indices", 1, refresher.trackedIndexCount());
    }

    /** With no store registered the refresher is inert, so an ordinary index is untouched. */
    public void testWithoutAStoreTheRefresherFindsNothing() {
        createIndex(INDEX);
        ensureGreen(INDEX);

        assertFalse(
            "no store means no refresh, and the caller behaves exactly as before",
            new StoreBackedFieldRefresher().refresh(mapperServiceFor(INDEX), resolveIndex(INDEX).getUUID(), "anything")
        );
    }

    /**
     * The cache is bounded, so a node serving many indices does not accumulate one entry per index.
     *
     * <p>One real index serves all hundred uuids deliberately. The cache is keyed by uuid string and the
     * bound has nothing to do with how many indices exist, so creating a hundred of them would test the
     * same property while making the test slow and, as the first version proved, flaky: it destabilised the
     * cluster enough that the mapping index's primary was not active within a minute.
     */
    public void testTheGenerationCacheIsBounded() {
        createIndex(INDEX);
        ensureGreen(INDEX);
        MapperService mapperService = mapperServiceFor(INDEX);
        MappingGenerationStore.register(new IndexBackedMappingStore(client()));

        StoreBackedFieldRefresher refresher = new StoreBackedFieldRefresher(10);
        for (int i = 0; i < 100; i++) {
            MappingGenerationStore.updateMapping("uuid-" + i, Map.of("f", "keyword"));
            refresher.refresh(mapperService, "uuid-" + i, "f");
        }

        assertTrue(
            "the cache must stay bounded rather than growing with indices served, or a node accumulates an "
                + "entry per index and rebuilds the residency ceiling H11 and H12 removed",
            refresher.trackedIndexCount() <= 10
        );
    }

    /**
     * T9. The bound must hold when every entry is written within one clock tick.
     *
     * <p>T9 replaced this cache's {@code synchronizedMap} around an access-ordered {@code LinkedHashMap},
     * which P1 measured running backwards under contention, with the write-stamped concurrent map P2
     * established. Eviction therefore takes a threshold from sorted stamps and drops everything strictly
     * below it, and T4 found in {@code BlobDescriptorBackend} that a wall clock cannot supply those stamps:
     * entries written within one tick share a value, the threshold equals every candidate, and nothing is
     * evicted.
     *
     * <p>This cache uses a monotonic sequence for that reason, and this test is what stops it being
     * simplified back to {@code checkedAtNanos} on the grounds that there is already a timestamp there.
     */
    public void testTheBoundHoldsWhenEveryEntrySharesATimestamp() {
        createIndex(INDEX);
        ensureGreen(INDEX);
        MapperService mapperService = mapperServiceFor(INDEX);
        MappingGenerationStore.register(new IndexBackedMappingStore(client()));

        StoreBackedFieldRefresher refresher = new StoreBackedFieldRefresher(10, () -> 1_000_000_000L);
        for (int i = 0; i < 100; i++) {
            MappingGenerationStore.updateMapping("frozen-uuid-" + i, Map.of("f", "keyword"));
            refresher.refresh(mapperService, "frozen-uuid-" + i, "f");
        }

        assertTrue(
            "a hundred entries written at one instant must still evict down to the bound, but held " + refresher.trackedIndexCount(),
            refresher.trackedIndexCount() <= 10
        );
    }
}
