/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.compress.CompressedXContent;

/**
 * Parsed mappings that indices sharing one set of analyzers can reuse instead of each parsing its own.
 *
 * <p>A {@link Mapping} is immutable once built: a mapping update builds a new one copy-on-write and leaves the old
 * one untouched. What ties a parsed mapping to an index is the analyzers, similarities and settings its field
 * mappers were built with. So two indices whose analyzers are the same instances, and whose settings that feed
 * mapping parsing are equal, can share one parsed mapping for the same source. An implementation is attached to
 * the {@link org.opensearch.index.analysis.IndexAnalyzers} those indices share, which is what keeps that rule.
 *
 * <p>{@link MapperService} consults it only when it loads an index's mapping for the first time from index
 * metadata. Later updates parse and merge as usual and leave the index with a mapping of its own.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SharedMappings {

    /**
     * Returns the parsed mapping for this exact source, or null if none is held.
     *
     * @param source the mapping source, as index metadata holds it
     * @return the parsed mapping, or null
     */
    Mapping get(CompressedXContent source);

    /**
     * Offers a mapping parsed and validated from this source, for the next index with the same source.
     *
     * @param source the mapping source it was parsed from
     * @param mapping the parsed mapping
     */
    void put(CompressedXContent source, Mapping mapping);
}
