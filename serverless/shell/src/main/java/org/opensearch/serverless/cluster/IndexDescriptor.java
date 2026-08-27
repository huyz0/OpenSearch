/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * The truth record for one index.
 *
 * <p>In phase 3 this is a blob in the object store, addressed by name and created with a
 * compare-and-swap on a register ({@code plan-area-h-metadata-off-cluster-state.md}). Phase 2 takes it
 * as an in-memory value so the projection and reconciliation paths can be built and tested before the
 * storage layer exists — the shape is the contract, not where it currently lives.
 *
 * <p>Note what is <em>not</em> here: no routing table, no allocation, no in-sync set. Those are not
 * properties of an index, they are properties of who happens to be serving it, and in this design they
 * come from shard-heads rather than from a descriptor.
 */
public final class IndexDescriptor {

    private final String name;
    private final String uuid;
    private final int numberOfShards;
    private final String mapping;
    private final Settings extraSettings;

    /**
     * Creates a descriptor.
     *
     * @param name the index name
     * @param uuid a stable uuid; the data plane rejects {@code _na_}
     * @param numberOfShards shard count
     * @param mapping the mapping source, or null for none
     * @param extraSettings settings layered over the defaults, or null
     */
    public IndexDescriptor(String name, String uuid, int numberOfShards, String mapping, Settings extraSettings) {
        this.name = Objects.requireNonNull(name);
        this.uuid = Objects.requireNonNull(uuid);
        if (numberOfShards < 1) {
            throw new IllegalArgumentException("numberOfShards must be positive, got " + numberOfShards);
        }
        this.numberOfShards = numberOfShards;
        this.mapping = mapping;
        this.extraSettings = extraSettings == null ? Settings.EMPTY : extraSettings;
    }

    /**
     * Returns the index name.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the index uuid.
     *
     * @return the uuid
     */
    public String uuid() {
        return uuid;
    }

    /**
     * Returns the shard count.
     *
     * @return number of shards
     */
    public int numberOfShards() {
        return numberOfShards;
    }

    /**
     * Renders this descriptor as the {@link IndexMetadata} the reused data plane expects.
     *
     * @param primaryTerms term per shard id, from the shard-heads; a shard with no entry gets term 1
     * @return the metadata
     * @throws IOException if the mapping cannot be parsed
     */
    public IndexMetadata toIndexMetadata(Map<Integer, Long> primaryTerms) throws IOException {
        final IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    // No replicas: durability comes from the object store, not from copies on peers.
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
                    .put(extraSettings)
                    .build()
            );
        if (mapping != null) {
            builder.putMapping(mapping);
        }
        for (int shard = 0; shard < numberOfShards; shard++) {
            // S0/F4: the data plane refuses to activate a primary at term 0, so a term must always be
            // supplied. In this design its only legitimate source is the shard-head's CAS generation.
            builder.primaryTerm(shard, primaryTerms.getOrDefault(shard, 1L));
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return "IndexDescriptor[" + name + "/" + uuid + " shards=" + numberOfShards + "]";
    }
}
