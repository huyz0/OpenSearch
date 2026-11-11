/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

/**
 * A node's copy of one store's contents, valid while the marker has not moved.
 *
 * <p>Shared across the {@code TemplateStore} instances the plane hands out for one container, which
 * is what makes reading every template on every index creation cost one small register read rather
 * than a listing and a read per template. One reference, replaced whole, so a reader never sees a
 * version labelled with another version's contents.
 */
public final class TemplateCache {
    volatile TemplateStore.Snapshot snapshot;
    final org.opensearch.common.cache.Cache<String, String> content = org.opensearch.common.cache.CacheBuilder.<String, String>builder()
        .setMaximumWeight(TemplateStore.MAX_CACHED_BYTES)
        .weigher((key, value) -> TemplateStore.retainedBytes(key, value))
        .build();

    /** Creates an empty cache, which the first read fills. */
    public TemplateCache() {}
}
