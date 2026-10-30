/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.pipeline;

import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;

/** Bounded reuse of native compiled pipelines by their exact ID and immutable definition. */
public final class CompiledPipelineCache<T> {
    private static final long MAXIMUM_WEIGHT = 32L * 1024 * 1024;
    // At most 128 small entries, with extra allowance for processor graphs as definitions grow.
    private static final long MINIMUM_ENTRY_WEIGHT = MAXIMUM_WEIGHT / 128L;

    private record Definition(String id, String source) {
        long weight() {
            return Math.max(MINIMUM_ENTRY_WEIGHT, 4096L + (id == null ? 0L : 2L * id.length()) + 16L * source.length());
        }

        @Override
        public String toString() {
            return "Definition[id=" + id + ", chars=" + source.length() + "]";
        }
    }

    private final Cache<Definition, T> cache = CacheBuilder.<Definition, T>builder()
        .setMaximumWeight(MAXIMUM_WEIGHT)
        .weigher((definition, value) -> definition.weight())
        .build();
    private final Object[] compiling = new Object[64];

    /** Creates a cache whose definitions and entry count are bounded. */
    public CompiledPipelineCache() {
        for (int i = 0; i < compiling.length; i++) {
            compiling[i] = new Object();
        }
    }

    /**
     * Reuses an unchanged native pipeline, sharing compilation with concurrent callers.
     *
     * @param id the pipeline ID, which native processors and metrics may retain
     * @param source the complete stored definition
     * @param compiler compiles a fresh native pipeline; failed compilations are never cached
     * @return the compiled pipeline
     * @throws Exception if compilation fails
     */
    public T get(String id, String source, CheckedSupplier<T, Exception> compiler) throws Exception {
        final var definition = new Definition(id, source);
        if (definition.weight() > MAXIMUM_WEIGHT) {
            return compiler.get();
        }
        final T known = cache.get(definition);
        if (known != null) {
            return known;
        }
        synchronized (compiling[(definition.hashCode() & Integer.MAX_VALUE) % compiling.length]) {
            final T completed = cache.get(definition);
            if (completed != null) {
                return completed;
            }
            // Do not leave a pending future behind when a processor factory throws an Error.
            final T compiled = compiler.get();
            cache.put(definition, compiled);
            return compiled;
        }
    }
}
