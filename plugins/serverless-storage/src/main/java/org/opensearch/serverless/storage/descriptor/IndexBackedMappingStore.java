/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.transport.client.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mappings for gated indices, stored outside cluster state.
 *
 * <p>H4c established that mappings have to leave cluster state or H3 and H5 are unusable, and H6a proved
 * the compare-and-swap converges under concurrent field inference. Neither had a backing store, so
 * {@code MappingGenerationStore} has never had a {@code Store} registered in production and the whole
 * no-broadcast mapping design was proven and inert.
 *
 * <p><b>The swap is external versioning rather than sequence numbers.</b> The generation is exactly what
 * the swap is conditioned on, and OpenSearch's external versioning rejects a write whose version is not
 * greater than the current one. So writing generation N+1 succeeds only when the stored generation is still
 * N, which is the compare-and-swap the interface asks for, expressed in one field rather than in a pair of
 * sequence numbers the caller would have to carry.
 *
 * <p>A {@code VersionConflictEngineException} is therefore a lost race and a correct answer. The caller
 * re-reads and merges, which is what {@code MappingGenerationStore}'s retry loop already does.
 *
 * <p><b>Reads and writes here are blocking, unlike the descriptor publish path.</b> That is a claim about
 * the callers rather than about this class, and it was wrong for one of them. It said a mapping update runs
 * on a transport thread handling a put-mapping or a dynamic-field inference, "not on the cluster state
 * thread, which is where W4 found that blocking deadlocks" -- true of dynamic inference, and false of
 * put-mapping, whose gated branch ran inside {@code MetadataMappingService.PutMappingExecutor}. That
 * executor <em>is</em> the cluster state thread, so every put-mapping on a gated index made two blocking
 * round trips on the one thread whose serialization is the ceiling this design exists to remove. It tripped
 * an assertion rather than merely being slow, and went unnoticed because the only test of the path drove
 * the executor with an in-memory double.
 *
 * <p>T19 moved that work to {@code MetadataMappingService#putMapping}, which dispatches to {@code GENERIC}
 * before submitting anything. So the safety argument now rests on both callers running off the cluster
 * state thread by construction, rather than on one of them happening to.
 */
public final class IndexBackedMappingStore implements MappingGenerationStore.Store {

    private static final Logger logger = LogManager.getLogger(IndexBackedMappingStore.class);

    /** Where gated mappings live. Separate from the descriptor index because they change independently. */
    public static final String MAPPING_INDEX = ".opensearch-index-mappings";

    private final Client client;
    private final AtomicBoolean indexKnownToExist = new AtomicBoolean();

    public IndexBackedMappingStore(Client client) {
        this.client = client;
    }

    /**
     * The stored mapping, or null when there genuinely is not one.
     *
     * <p><b>T26: absent and unreadable are different answers, and this used to give the same one.</b> Every
     * exception was caught and reported as null, and null is what an index with no fields looks like, so a
     * cluster block, an unavailable shard and a timeout all arrived at callers as an empty mapping.
     *
     * <p><b>What that actually cost, which is not what it first looks like.</b> The obvious reading is
     * silent field loss: {@code updateMapping} merges onto what it read, so reading empty for an index with
     * fields would swap them away. That cannot happen through this store, and the reason is worth keeping.
     * A merge from empty proposes generation 1, {@link #compareAndSwap} writes the generation as an external
     * version, and external versioning refuses anything not greater than what is stored. So the write was
     * rejected, the loop went round, and sixteen attempts later the caller was told its mapping update "did
     * not converge, which means sustained contention" -- a wrong diagnosis of an unreachable store, arrived
     * at through thirty-two wasted round trips. The failure was honest by accident and misleading on
     * purpose.
     *
     * <p>Now a read that cannot be performed says so. {@code updateMapping} retries it the same number of
     * times, so nothing loses the resilience the accident provided, and raises the read's own failure
     * instead of a contention message.
     *
     * <p><b>A missing mapping index is still null, and that is a judgement, not a certainty.</b> Nothing has
     * ever been written to it in the ordinary case, so no index can have a stored mapping. If the index is
     * deleted out from under a live cluster the same absence means the opposite, and the merge that follows
     * really would write a mapping holding one field where there had been many. Deleting it is not
     * something anything here does, and treating it as unreadable would fail every cluster before its first
     * mapped gated creation, so this is the trade rather than an oversight. T31 covers closing it.
     */
    @Override
    public MappingGenerationStore.MappingGeneration read(String indexUuid) {
        GetResponse response;
        try {
            response = client.prepareGet(MAPPING_INDEX, indexUuid).get();
        } catch (IndexNotFoundException e) {
            // Nothing has ever been written, so no index has a mapping. The only absence this can infer.
            logger.debug("no mapping index yet, so [{}] has no stored mapping", indexUuid);
            return null;
        }
        if (response.isExists() == false) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) response.getSourceAsMap().getOrDefault("fields", Map.of());
        return new MappingGenerationStore.MappingGeneration(response.getVersion(), fields);
    }

    @Override
    public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
        ensureIndexExists();
        try {
            Map<String, Object> source = new HashMap<>();
            source.put("uuid", indexUuid);
            source.put("fields", updated.fields());
            source.put("generation", updated.generation());
            // A bounded, aggregatable projection of the mapping, written alongside it.
            //
            // The fields object above is stored with indexing disabled, because user field names are
            // unbounded and indexing them would grow this index's own mapping with the union of every
            // gated index's fields. That is the residency problem this area exists to remove, one level
            // down. The consequence is that fields cannot be aggregated, and cluster stats needs exactly
            // that (W7).
            //
            // Field *types* are a bounded vocabulary of a couple of dozen names, so a nested array of
            // {type, count} is safe to index and gives an exact answer rather than an estimate: a terms
            // aggregation on type with a sum on count yields both the field count and the index count per
            // type, in one query whose cost does not grow with the number of gated indices.
            source.put("fieldTypeCounts", typeCountsOf(updated.fields()));
            client.index(
                new IndexRequest(MAPPING_INDEX).id(indexUuid).source(source).versionType(VersionType.EXTERNAL).version(updated.generation())
            ).actionGet();
            return true;
        } catch (VersionConflictEngineException e) {
            // Someone advanced the generation first. The caller re-reads and merges, which is the whole
            // point of returning false rather than throwing.
            return false;
        }
    }

    /** Collapses a mapping into one {type, count} entry per distinct field type. */
    private static java.util.List<Map<String, Object>> typeCountsOf(Map<String, Object> fields) {
        Map<String, Long> perType = new HashMap<>();
        for (Object value : fields.values()) {
            // Reads the type out of the definition rather than treating the value as one. Since T15 the
            // stored value is a field's whole definition; a value written before that is a bare type name,
            // and MappingGenerationStore#typeOf resolves both.
            String type = MappingGenerationStore.typeOf(value);
            if (type != null) {
                perType.merge(type, 1L, Long::sum);
            }
        }
        java.util.List<Map<String, Object>> counts = new java.util.ArrayList<>(perType.size());
        for (Map.Entry<String, Long> each : perType.entrySet()) {
            counts.add(Map.of("type", each.getKey(), "count", each.getValue()));
        }
        return counts;
    }

    /**
     * Creates the mapping index if it is not already there.
     *
     * <p>{@code fields} is deliberately not indexed as an object with dynamic mapping: it holds arbitrary
     * user field names, so mapping it would make the mapping index's own mapping grow with the union of
     * every gated index's fields. That is the residency problem this area exists to remove, reproduced one
     * level down, and it is the reason the field is stored as a disabled object.
     */
    private void ensureIndexExists() {
        if (indexKnownToExist.get()) {
            return;
        }
        try {
            client.admin()
                .indices()
                .create(
                    new CreateIndexRequest(MAPPING_INDEX).settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            .build()
                    )
                        .mapping(
                            Map.of(
                                "properties",
                                Map.of(
                                    "uuid",
                                    Map.of("type", "keyword"),
                                    "generation",
                                    Map.of("type", "long"),
                                    // Stored, not indexed. Indexing it would grow this index's own mapping
                                    // with the union of every gated index's field names.
                                    "fields",
                                    Map.of("type", "object", "enabled", false),
                                    // Nested so a terms-and-sum aggregation can answer per type exactly.
                                    // Bounded because field types are a fixed vocabulary, unlike field
                                    // names.
                                    "fieldTypeCounts",
                                    Map.of(
                                        "type",
                                        "nested",
                                        "properties",
                                        Map.of("type", Map.of("type", "keyword"), "count", Map.of("type", "long"))
                                    )
                                )
                            )
                        )
                )
                .actionGet();
            indexKnownToExist.set(true);
        } catch (Exception e) {
            if (e instanceof org.opensearch.ResourceAlreadyExistsException
                || e.getCause() instanceof org.opensearch.ResourceAlreadyExistsException) {
                indexKnownToExist.set(true);
                return;
            }
            throw e instanceof RuntimeException runtime ? runtime : new RuntimeException(e);
        }
    }
}
