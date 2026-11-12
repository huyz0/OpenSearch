/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.analysis;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.mapper.SharedMappings;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static org.opensearch.index.analysis.AnalysisRegistry.DEFAULT_ANALYZER_NAME;
import static org.opensearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME;
import static org.opensearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME;

/**
 * IndexAnalyzers contains a name to analyzer mapping for a specific index.
 * This class only holds analyzers that are explicitly configured for an index and doesn't allow
 * access to individual tokenizers, char or token filter.
 *
 * @see AnalysisRegistry
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class IndexAnalyzers implements Closeable {
    private final Map<String, NamedAnalyzer> analyzers;
    private final Map<String, NamedAnalyzer> normalizers;
    private final Map<String, NamedAnalyzer> whitespaceNormalizers;
    @Nullable
    private final Closeable release;
    @Nullable
    private final SharedMappings sharedMappings;

    public IndexAnalyzers(
        Map<String, NamedAnalyzer> analyzers,
        Map<String, NamedAnalyzer> normalizers,
        Map<String, NamedAnalyzer> whitespaceNormalizers
    ) {
        Objects.requireNonNull(analyzers.get(DEFAULT_ANALYZER_NAME), "the default analyzer must be set");
        if (analyzers.get(DEFAULT_ANALYZER_NAME).name().equals(DEFAULT_ANALYZER_NAME) == false) {
            throw new IllegalStateException(
                "default analyzer must have the name [default] but was: [" + analyzers.get(DEFAULT_ANALYZER_NAME).name() + "]"
            );
        }
        this.analyzers = unmodifiableMap(analyzers);
        this.normalizers = unmodifiableMap(normalizers);
        this.whitespaceNormalizers = unmodifiableMap(whitespaceNormalizers);
        this.release = null;
        this.sharedMappings = null;
    }

    private IndexAnalyzers(IndexAnalyzers shared, Closeable release, SharedMappings sharedMappings) {
        this.analyzers = shared.analyzers;
        this.normalizers = shared.normalizers;
        this.whitespaceNormalizers = shared.whitespaceNormalizers;
        this.release = Objects.requireNonNull(release);
        this.sharedMappings = sharedMappings;
    }

    /**
     * Returns a view of these analyzers for one more index to use. Closing the view runs {@code release} instead of
     * closing the analyzers, so the owner closes them once the last index using them has released its view.
     *
     * @param release what closing the view does
     * @param sharedMappings parsed mappings the indices sharing these analyzers may reuse, or null for none
     * @return the view
     */
    @ExperimentalApi
    public IndexAnalyzers sharedView(Closeable release, @Nullable SharedMappings sharedMappings) {
        return new IndexAnalyzers(this, release, sharedMappings);
    }

    /**
     * Returns the parsed mappings shared by the indices that share these analyzers, or null if they are not shared.
     *
     * @return the shared mappings, or null
     */
    @ExperimentalApi
    @Nullable
    public SharedMappings sharedMappings() {
        return sharedMappings;
    }

    /**
     * Returns an analyzer mapped to the given name or <code>null</code> if not present
     */
    public NamedAnalyzer get(String name) {
        return analyzers.get(name);
    }

    /**
     * Returns an (unmodifiable) map of containing the index analyzers
     */
    public Map<String, NamedAnalyzer> getAnalyzers() {
        return analyzers;
    }

    /**
     * Returns a normalizer mapped to the given name or <code>null</code> if not present
     */
    public NamedAnalyzer getNormalizer(String name) {
        return normalizers.get(name);
    }

    /**
     * Returns a normalizer that splits on whitespace mapped to the given name or <code>null</code> if not present
     */
    public NamedAnalyzer getWhitespaceNormalizer(String name) {
        return whitespaceNormalizers.get(name);
    }

    /**
     * Returns the default index analyzer for this index
     */
    public NamedAnalyzer getDefaultIndexAnalyzer() {
        return analyzers.get(DEFAULT_ANALYZER_NAME);
    }

    /**
     * Returns the default search analyzer for this index. If not set, this will return the default analyzer
     */
    public NamedAnalyzer getDefaultSearchAnalyzer() {
        return analyzers.getOrDefault(DEFAULT_SEARCH_ANALYZER_NAME, getDefaultIndexAnalyzer());
    }

    /**
     * Returns the default search quote analyzer for this index
     */
    public NamedAnalyzer getDefaultSearchQuoteAnalyzer() {
        return analyzers.getOrDefault(DEFAULT_SEARCH_QUOTED_ANALYZER_NAME, getDefaultSearchAnalyzer());
    }

    @Override
    public void close() throws IOException {
        if (release != null) {
            release.close();
            return;
        }
        IOUtils.close(
            Stream.of(analyzers.values().stream(), normalizers.values().stream(), whitespaceNormalizers.values().stream())
                .flatMap(s -> s)
                .filter(a -> a.scope() == AnalyzerScope.INDEX)
                .collect(Collectors.toList())
        );
    }
}
