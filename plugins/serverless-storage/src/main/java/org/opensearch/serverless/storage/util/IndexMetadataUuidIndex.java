/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.util;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;

import java.util.HashMap;
import java.util.Map;

/**
 * {@code Metadata} has no uuid lookup, only {@link Metadata#index(String)} by name, so every caller
 * that only knows an index's uuid (a shard-scoped request, a per-shard tick) has had to scan {@code
 * metadata.indices().values()} to find it. Lifted out of {@code
 * org.opensearch.serverless.storage.scaletozero.ShardSuspensionCoordinator}, which built this exact
 * cache first (S25 measured the win: 20.4 ms for the first resolution against 50,000 indices, ~0.07 ms
 * for every one after, against the same {@code Metadata} instance) after H1d's own audit found four
 * further copies of the same bare scan still uncached: {@code TransportMigrateShardAction}, {@code
 * TransportShardSplitAction}, {@code WriterPublicationNotifier}, and {@code
 * ReaderCacheAffinityRecorder}.
 *
 * <p>Rebuilds only when the {@link Metadata} instance changes, not on every call -- safe because
 * {@code Metadata} is immutable, so identity is a sound cache key, and correct even when this method
 * is called concurrently across two different {@code Metadata} instances: a race just means one caller
 * pays for a rebuild the other's result also benefits from, not a wrong answer, since whichever build
 * finishes last wins and every build from the same instance produces an identical map.
 *
 * <p>One instance per caller, not shared across callers, deliberately: an instance only ever caches
 * against the single most recent {@link Metadata} it was called with (not a map of several), which is
 * sound for a caller whose calls cluster around one state version at a time -- a tick walking many
 * shards of the same cluster state, or a burst of requests arriving before the next state change -- but
 * would thrash into constant rebuilding for two callers that see two different state versions
 * interleaved on the same instance.
 */
public final class IndexMetadataUuidIndex {

    private volatile Metadata builtFrom;
    private volatile Map<String, IndexMetadata> index;

    /**
     * Resolves an index by uuid, scanning {@code metadata} once per distinct instance seen rather than
     * once per call.
     *
     * @param metadata the cluster metadata to resolve against.
     * @param indexUuid the uuid to look up.
     * @return the matching {@link IndexMetadata}, or {@code null} if no index in {@code metadata} has
     * that uuid.
     */
    public IndexMetadata findByUuid(Metadata metadata, String indexUuid) {
        Map<String, IndexMetadata> current = index;
        if (metadata != builtFrom || current == null) {
            Map<String, IndexMetadata> rebuilt = new HashMap<>(metadata.indices().size());
            for (IndexMetadata indexMetadata : metadata.indices().values()) {
                rebuilt.put(indexMetadata.getIndexUUID(), indexMetadata);
            }
            current = rebuilt;
            index = rebuilt;
            builtFrom = metadata;
        }
        return current.get(indexUuid);
    }
}
