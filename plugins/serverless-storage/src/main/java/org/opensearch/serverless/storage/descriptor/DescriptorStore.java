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
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The one place descriptors are read and written.
 *
 * <p>Every seam in Area H needs the same three operations and none of them existed: a get by id for
 * exact-name resolution, a put with {@code op_type=create} for uniqueness without a cluster state update,
 * and a prefix search with {@code search_after} paging for wildcards and listings. They live here rather
 * than being open-coded at each seam, which is precisely the mistake C3 made with routing lookups: the
 * resolution path and the write path were hooked separately, drifted apart, and the divergence was possible
 * only because the pairing was written out at every site.
 *
 * <p><b>The three operations have different consistency, and that is measured rather than chosen.</b> H18
 * established that a get by id reads through the translog and so sees a descriptor the instant its write is
 * acknowledged, while a search sees only refreshed segments. So {@link #get} is realtime and {@link
 * #findByPrefix} is refresh-bound, and callers needing immediate visibility must name the index exactly.
 *
 * <p><b>The index is created lazily.</b> A cluster that never creates a gated index never pays for it, and
 * creating at node start would put an index creation on every node's startup path for a feature most
 * clusters will not use. The flag is an optimisation only: creation races are resolved by
 * {@code ResourceAlreadyExistsException} being treated as success, because two nodes creating the
 * descriptor index concurrently is the normal case rather than an error.
 */
public final class DescriptorStore {

    private static final Logger logger = LogManager.getLogger(DescriptorStore.class);

    /** The descriptor index's name. Fixed rather than configurable: it is part of the on-disk contract. */
    public static final String DESCRIPTOR_INDEX = ".opensearch-index-descriptors";

    /**
     * How many descriptors one page of a prefix search returns.
     *
     * <p>S24 measured about 33 ms per thousand names, so a page is a bounded cost rather than a preference.
     * Callers that need everything page through; callers that need a page get one.
     */
    public static final int PAGE_SIZE = 1_000;

    private final Client client;
    private final int shardCount;
    private final AtomicBoolean indexKnownToExist = new AtomicBoolean();

    public DescriptorStore(Client client, int shardCount) {
        this.client = client;
        this.shardCount = shardCount;
    }

    /**
     * The descriptor for this name, or null if there is none.
     *
     * <p>Realtime, per H18: this is a get by id, so it reads through the translog and sees a descriptor as
     * soon as its write is acknowledged. That is what lets a client create an index and immediately write
     * to it.
     */
    public IndexDescriptor get(String name) {
        try {
            var response = client.prepareGet(DESCRIPTOR_INDEX, name).get();
            if (response.isExists() == false) {
                return null;
            }
            return DescriptorCodec.fromSource(response.getSourceAsMap());
        } catch (Exception e) {
            // A missing index is a missing descriptor, not a failure: before the first gated creation the
            // descriptor index does not exist, and every resolution would otherwise throw.
            logger.debug("descriptor lookup for [{}] failed", name, e);
            return null;
        }
    }

    /**
     * Records a descriptor for an index that does not yet exist, returning whether this call created it.
     *
     * <p>{@code op_type=create} is what makes the name unique without a cluster state update, which is the
     * whole of H3: the store, rather than the cluster manager, is the thing that says a name is taken. A
     * version conflict is therefore a lost race and a correct answer, not an error.
     */
    public boolean create(IndexDescriptor descriptor) {
        ensureIndexExists();
        try {
            client.index(new IndexRequest(DESCRIPTOR_INDEX).id(descriptor.name()).source(DescriptorCodec.toSource(descriptor)).create(true))
                .actionGet();
            return true;
        } catch (VersionConflictEngineException e) {
            // Someone else created this name first. That is the mechanism working.
            return false;
        }
    }

    /** Overwrites a descriptor, for a mapping generation bump or a tombstone. Blocks until written. */
    public void put(IndexDescriptor descriptor) {
        ensureIndexExists();
        client.index(new IndexRequest(DESCRIPTOR_INDEX).id(descriptor.name()).source(DescriptorCodec.toSource(descriptor))).actionGet();
    }

    /**
     * Records a descriptor without waiting for it to be written.
     *
     * <p><b>This exists because the publish hook runs on the cluster state thread.</b>
     * {@code Metadata} calls {@code IndexDescriptorPublisher.publish} while a cluster state is being built,
     * so a blocking write here deadlocks: the cluster state update waits on an index operation that itself
     * needs a cluster state to route. Registering the blocking {@link #put} as the publisher hung the node
     * rather than failing, which is how it was found.
     *
     * <p>The cost of not waiting is that a descriptor write can be lost to a crash between the cluster
     * state commit and the index operation landing. For creation that is tolerable, since the index is in
     * cluster state and the descriptor is redundant there. <b>For a tombstone it is not</b>, because H4b
     * established that a node adopting dangling shard data must be able to read the tombstone, and a lost
     * tombstone is exactly the case that protects against. That gap is real and is tracked rather than
     * papered over here.
     *
     * <p>Failures are logged rather than thrown for the same reason they are swallowed on the read path:
     * this runs inside cluster state construction, and an exception raised there fails an unrelated
     * cluster state update.
     */
    public void putAsync(IndexDescriptor descriptor) {
        try {
            client.index(
                new IndexRequest(DESCRIPTOR_INDEX).id(descriptor.name()).source(DescriptorCodec.toSource(descriptor)),
                new org.opensearch.core.action.ActionListener<>() {
                    @Override
                    public void onResponse(org.opensearch.action.index.IndexResponse response) {}

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("failed to record descriptor for [{}]", descriptor.name(), e);
                    }
                }
            );
        } catch (Exception e) {
            logger.warn("failed to submit descriptor write for [{}]", descriptor.name(), e);
        }
    }

    /**
     * One page of descriptors whose name starts with {@code prefix}, ordered by name.
     *
     * <p>Refresh-bound, per H18, because it is a search. A descriptor written microseconds ago may not
     * appear until the next refresh, which is the documented wildcard contract rather than a defect.
     *
     * @param afterName the last name of the previous page, or null for the first page
     */
    public List<IndexDescriptor> findByPrefix(String prefix, String afterName, int size) {
        try {
            var request = client.prepareSearch(DESCRIPTOR_INDEX)
                .setQuery(QueryBuilders.prefixQuery("name", prefix))
                .addSort("name", SortOrder.ASC)
                .setSize(size);
            if (Strings.isNullOrEmpty(afterName) == false) {
                request.searchAfter(new Object[] { afterName });
            }
            SearchResponse response = request.get();
            List<IndexDescriptor> descriptors = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                descriptors.add(DescriptorCodec.fromSource(hit.getSourceAsMap()));
            }
            return descriptors;
        } catch (Exception e) {
            // Same reasoning as get: no descriptor index means no descriptors, not a failed request.
            logger.debug("descriptor prefix search for [{}] failed", prefix, e);
            return List.of();
        }
    }

    /**
     * Creates the descriptor index if it is not already there.
     *
     * <p>The settings are the contract rather than tuning, and both were established by measurement:
     *
     * <ul>
     *   <li><b>refresh_interval</b> bounds how stale a wildcard may be (H18). It is set explicitly because
     *       leaving it at the default makes an API-visible guarantee depend on an unstated default.</li>
     *   <li><b>merge policy floor and segments per tier</b> bound lookup latency (S29, S30). Segments left
     *       unbounded gave 1.88x per decade; held low, the same decade cost 1.05x. This is the difference
     *       between the descriptor index being a ceiling and not being one.</li>
     * </ul>
     */
    private void ensureIndexExists() {
        if (indexKnownToExist.get()) {
            return;
        }
        try {
            client.admin()
                .indices()
                .create(
                    new CreateIndexRequest(DESCRIPTOR_INDEX).settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            // H18: this is the wildcard staleness bound, stated rather than defaulted.
                            .put("index.refresh_interval", "1s")
                            // S29 and S30: this is the lookup latency bound.
                            .put("index.merge.policy.segments_per_tier", 4)
                            .put("index.merge.policy.max_merge_at_once", 4)
                            .build()
                    ).mapping(DescriptorCodec.MAPPING)
                )
                .actionGet();
            indexKnownToExist.set(true);
        } catch (Exception e) {
            if (e instanceof org.opensearch.ResourceAlreadyExistsException
                || e.getCause() instanceof org.opensearch.ResourceAlreadyExistsException) {
                // The normal case on every node but the first. Concurrent creation is expected, not an error.
                indexKnownToExist.set(true);
                return;
            }
            throw e instanceof RuntimeException runtime ? runtime : new RuntimeException(e);
        }
    }

    /** Whether the descriptor index has been created, which tests assert rather than infer. */
    public boolean indexExists() {
        return client.admin().indices().prepareExists(DESCRIPTOR_INDEX).get().isExists();
    }

    /** The mapping and the document shape, kept beside each other so they cannot drift. */
    static final class DescriptorCodec {

        static final Map<String, Object> MAPPING = Map.of(
            "properties",
            Map.ofEntries(
                Map.entry("name", Map.of("type", "keyword")),
                Map.entry("uuid", Map.of("type", "keyword")),
                Map.entry("shardCount", Map.of("type", "integer")),
                Map.entry("searchOnlyReplicaCount", Map.of("type", "integer")),
                Map.entry("serverless", Map.of("type", "boolean")),
                Map.entry("state", Map.of("type", "keyword")),
                Map.entry("aliases", Map.of("type", "keyword")),
                Map.entry("createdVersion", Map.of("type", "long")),
                Map.entry("system", Map.of("type", "boolean")),
                Map.entry("hidden", Map.of("type", "boolean")),
                Map.entry("remoteSnapshot", Map.of("type", "boolean")),
                Map.entry("warm", Map.of("type", "boolean")),
                Map.entry("mappingGeneration", Map.of("type", "long")),
                // H17 pagination orders by (creationDate, name), so it has to be sortable here.
                Map.entry("creationDate", Map.of("type", "long"))
            )
        );

        static Map<String, Object> toSource(IndexDescriptor d) {
            return Map.ofEntries(
                Map.entry("name", d.name()),
                Map.entry("uuid", d.uuid()),
                Map.entry("shardCount", d.shardCount()),
                Map.entry("searchOnlyReplicaCount", d.searchOnlyReplicaCount()),
                Map.entry("serverless", d.serverless()),
                Map.entry("state", d.state().name()),
                Map.entry("aliases", d.aliases()),
                Map.entry("createdVersion", d.createdVersion()),
                Map.entry("system", d.system()),
                Map.entry("hidden", d.hidden()),
                Map.entry("remoteSnapshot", d.remoteSnapshot()),
                Map.entry("warm", d.warm()),
                Map.entry("mappingGeneration", d.mappingGeneration()),
                Map.entry("creationDate", d.creationDate())
            );
        }

        @SuppressWarnings("unchecked")
        static IndexDescriptor fromSource(Map<String, Object> source) {
            return new IndexDescriptor(
                (String) source.get("name"),
                (String) source.get("uuid"),
                ((Number) source.get("shardCount")).intValue(),
                ((Number) source.get("searchOnlyReplicaCount")).intValue(),
                (Boolean) source.get("serverless"),
                IndexDescriptor.State.valueOf((String) source.get("state")),
                (List<String>) source.getOrDefault("aliases", List.of()),
                ((Number) source.get("createdVersion")).longValue(),
                (Boolean) source.get("system"),
                (Boolean) source.get("hidden"),
                (Boolean) source.get("remoteSnapshot"),
                (Boolean) source.get("warm"),
                ((Number) source.get("mappingGeneration")).longValue(),
                ((Number) source.get("creationDate")).longValue()
            );
        }

        private DescriptorCodec() {}
    }
}
