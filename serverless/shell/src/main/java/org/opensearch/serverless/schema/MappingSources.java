/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.schema;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.mapper.MapperService;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * One copy in heap of each distinct mapping source, and of the index metadata form of it.
 *
 * <p>Every descriptor a node has read carries its index's mapping as text, and every open index renders it again as
 * {@link MappingMetadata}. Across thousands of indices made from a few templates these are thousands of equal copies.
 * Both maps here are weak: a source stays only while some descriptor still holds it.
 */
public final class MappingSources {

    private static final Map<String, WeakReference<String>> SOURCES = new WeakHashMap<>();
    private static final Map<String, MappingMetadata> METADATA = new WeakHashMap<>();

    private MappingSources() {}

    /**
     * Returns the one held copy equal to this source, holding this one if there is none.
     *
     * @param source a mapping source, or null
     * @return an equal source, or null
     */
    public static String intern(String source) {
        if (source == null) {
            return null;
        }
        synchronized (SOURCES) {
            final WeakReference<String> held = SOURCES.get(source);
            final String existing = held == null ? null : held.get();
            if (existing != null) {
                return existing;
            }
            SOURCES.put(source, new WeakReference<>(source));
            return source;
        }
    }

    /**
     * Returns the index metadata form of this source, parsed once per distinct source.
     *
     * @param source a mapping source
     * @return its mapping metadata, shared by every caller passing an equal source
     */
    public static MappingMetadata metadata(String source) {
        final String key = intern(source);
        synchronized (METADATA) {
            MappingMetadata metadata = METADATA.get(key);
            if (metadata == null) {
                metadata = new MappingMetadata(
                    MapperService.SINGLE_MAPPING_NAME,
                    XContentHelper.convertToMap(MediaTypeRegistry.xContent(key).xContent(), key, true)
                );
                METADATA.put(key, metadata);
            }
            return metadata;
        }
    }
}
