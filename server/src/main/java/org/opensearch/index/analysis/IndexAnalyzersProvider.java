/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.analysis;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;

import java.io.IOException;

/**
 * Builds the analyzers for an index that is being opened, in place of {@link AnalysisRegistry#build(IndexSettings)}.
 *
 * <p>Lets a node hand indices with the same analysis configuration one set of analyzers, through
 * {@link IndexAnalyzers#sharedView}, rather than building a set per index. The index closes what it is given
 * when it closes, as it would its own; a shared view releases its share rather than closing the analyzers.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
@FunctionalInterface
public interface IndexAnalyzersProvider {

    /**
     * Returns the analyzers for this index.
     *
     * @param indexSettings the settings of the index being opened
     * @param registry the registry that would otherwise build them
     * @return the analyzers, which the index closes when it closes
     * @throws IOException if they cannot be built
     */
    IndexAnalyzers build(IndexSettings indexSettings, AnalysisRegistry registry) throws IOException;
}
