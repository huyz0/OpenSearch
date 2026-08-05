/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MapperService;

/**
 * A validator that is called during index creation after mappings have been merged,
 * allowing plugins to validate the combination of index settings and mappings.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexCreationValidator {
    /**
     * Validates the index settings against the merged mappings.
     * Throw {@link IllegalArgumentException} to reject index creation.
     *
     * @param mapperService the mapper service with merged mappings, or null when {@link #requiresMappings()}
     *                      is false and the creation path had no mappings to merge
     * @param indexSettings the index settings
     */
    void validate(MapperService mapperService, IndexSettings indexSettings);

    /**
     * Whether this validator reads the {@link MapperService} it is given.
     *
     * <p>Supplying one costs far more than the check it enables. It means building a whole throwaway
     * {@link IndexService} on the cluster manager, which happens under a lock on {@code IndicesService}.
     * Profiling a creation-heavy workload put 41.5% of on-CPU samples inside that call tree, and recorded
     * 122,222 blocking events on that one monitor totalling 1,453 seconds of blocked thread time over a two
     * minute run. For a validator that only reads settings, every bit of that is spent producing an argument
     * it never looks at.
     *
     * <p>Answering false lets a creation path with no mappings to merge skip building one and pass null
     * instead. Answering true -- the default, and therefore what every existing implementation says without
     * being changed -- keeps the current behaviour exactly.
     *
     * @return true if {@link #validate} may dereference its {@code mapperService} argument
     */
    default boolean requiresMappings() {
        return true;
    }
}
