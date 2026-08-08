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
 * before submitting anything. So the safety argument rests on the callers running off the cluster state
 * thread by construction, rather than on one of them happening to. There are three of them now: creation
 * through {@code MetadataCreateIndexService}, put-mapping and dynamic inference through
 * {@code MetadataMappingService}, and since T47 the prune in {@code MetadataDeleteIndexService}, which runs
 * from the tombstone writer's completion after the client has already been answered.
 *
 * <p>T44 asserts the first two rather than arguing them. The third is not covered there yet.
 */
public final class IndexBackedMappingStore implements MappingGenerationStore.Store {

    private static final Logger logger = LogManager.getLogger(IndexBackedMappingStore.class);

    /** Where gated mappings live. Separate from the descriptor index because they change independently. */
    public static final String MAPPING_INDEX = ".opensearch-index-mappings";

    /**
     * Shards for the mapping index when nobody says otherwise.
     *
     * <p>Five, and until T45 that was a literal in the middle of {@code ensureIndexExists} with no reasoning
     * attached and no way to change it. It is a guess: the index holds one small document per gated index,
     * so its bytes are trivial, and what it actually has to survive is write concurrency on the creation
     * path -- every gated creation with a declared mapping writes here. Five spreads that across five
     * primaries. Whether that is the right number is what T46 measures; until then the point of naming it
     * is that a wrong guess can now be corrected without a code change.
     */
    public static final int DEFAULT_SHARDS = 5;

    private final Client client;
    private final int shards;
    private final java.util.function.BooleanSupplier mappingIndexWasDeleted;
    private final AtomicBoolean indexKnownToExist = new AtomicBoolean();

    public IndexBackedMappingStore(Client client) {
        this(client, DEFAULT_SHARDS);
    }

    public IndexBackedMappingStore(Client client, int shards) {
        this(client, shards, () -> false);
    }

    /**
     * @param mappingIndexWasDeleted whether the mapping index has gone away after having been present, which
     *                               is the one thing that distinguishes "nothing was ever written" from
     *                               "everything that was written is gone". See {@link MappingIndexWatcher}.
     */
    public IndexBackedMappingStore(Client client, int shards, java.util.function.BooleanSupplier mappingIndexWasDeleted) {
        this.client = client;
        this.shards = shards;
        this.mappingIndexWasDeleted = mappingIndexWasDeleted;
    }

    /**
     * The stored mapping, or null when there genuinely is not one.
     *
     * <p><b>T43: absent and unreadable are different answers, and this used to give the same one.</b> Every
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
     * <p><b>A missing mapping index is null only while it has never been lost.</b> Nothing has ever been
     * written to it in the ordinary case, so no index can have a stored mapping, and treating that as
     * unreadable would fail every cluster before its first mapped gated creation. If the index is instead
     * deleted or replaced under a live cluster, the same absence means the opposite. T48 made that a
     * different answer rather than the same one: {@link MappingIndexWatcher} reports the loss and
     * {@code failIfTheStoreWasLost} refuses both absence paths.
     */
    @Override
    public MappingGenerationStore.MappingGeneration read(String indexUuid) {
        GetResponse response;
        try {
            response = client.prepareGet(MAPPING_INDEX, indexUuid).get();
        } catch (IndexNotFoundException e) {
            indexKnownToExist.set(false);
            failIfTheStoreWasLost(indexUuid, e);
            // Nothing has ever been written, so no index has a mapping.
            logger.debug("no mapping index yet, so [{}] has no stored mapping", indexUuid);
            return null;
        }
        if (response.isExists() == false) {
            // Guarded too, and this is the path that matters more. An index recreated after a deletion
            // answers here rather than throwing: the get succeeds against an empty index and reports the
            // document missing, which is indistinguishable from an index that never declared fields.
            failIfTheStoreWasLost(indexUuid, null);
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) response.getSourceAsMap().getOrDefault("fields", Map.of());
        return new MappingGenerationStore.MappingGeneration(response.getVersion(), fields);
    }

    /**
     * T48. Refuses to answer "no mapping" once the store's contents are known to be gone.
     *
     * <p>Absence means two opposite things and the difference is not in the response: nothing was ever
     * written, or everything that was written is lost. Answering null for the second lets the caller merge
     * onto empty and write one field where there had been many, and the write succeeds, because the document
     * that external versioning would have refused it against is gone too.
     *
     * <p>Only {@code updateMapping} and the refresher reach this. {@code createMapping} attempts its swap
     * without reading, so a creation into a lost store never consults this -- correctly, since a fresh UUID
     * has nothing to lose.
     */
    private void failIfTheStoreWasLost(String indexUuid, Exception cause) {
        if (mappingIndexWasDeleted.getAsBoolean() == false) {
            return;
        }
        throw new IllegalStateException(
            "["
                + MAPPING_INDEX
                + "] was lost while this node was running, so the mapping for ["
                + indexUuid
                + "] is missing rather than absent. Treating it as absent would replace whatever was "
                + "declared with whatever this caller happens to know.",
            cause
        );
    }

    @Override
    public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
        java.util.Objects.requireNonNull(indexUuid, "indexUuid must not be null");
        if (expectedGeneration < 0) {
            throw new IllegalArgumentException("expectedGeneration must be non-negative: " + expectedGeneration);
        }
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

    /**
     * Removes an index's mapping document.
     *
     * <p>A missing mapping index means there is nothing to remove, the same absence {@link #read} infers
     * and for the same reason. Anything else propagates: the caller decides whether a mapping that could
     * not be removed should fail the deletion, and it decides no.
     */
    @Override
    public void delete(String indexUuid) {
        try {
            client.prepareDelete(MAPPING_INDEX, indexUuid).get();
        } catch (IndexNotFoundException e) {
            indexKnownToExist.set(false);
            logger.debug("no mapping index, so [{}] has no mapping to remove", indexUuid);
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
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
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
                // Logged because this is the one moment a configured shard count is silently discarded. An
                // index's geometry is fixed at creation, so a node that starts after this index exists reads
                // a setting that can never apply, and without this line the only evidence is that the number
                // in the config and the number on the index disagree.
                logger.debug("[{}] already exists, so this node's configured shard count of {} does not apply", MAPPING_INDEX, shards);
                return;
            }
            throw e instanceof RuntimeException runtime ? runtime : new RuntimeException(e);
        }
    }
}
