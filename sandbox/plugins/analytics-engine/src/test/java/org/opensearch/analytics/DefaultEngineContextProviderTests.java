/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pins the security-wiring contract for {@link AnalyticsPlugin.DefaultEngineContextProvider}:
 * {@code getContext()} must thread the cluster's {@link IndexNameExpressionResolver} (which
 * carries security-plugin extensions and system-index access rules) into
 * {@code OpenSearchSchemaBuilder.buildSchema}, not silently construct a fresh resolver. A
 * regression to the single-arg buildSchema(state) overload would bypass those checks.
 */
public class DefaultEngineContextProviderTests extends OpenSearchTestCase {

    public void testGetContextUsesInjectedResolver() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        IndexNameExpressionResolver injectedResolver = mock(IndexNameExpressionResolver.class);
        when(
            injectedResolver.concreteIndexNames(
                same(clusterState),
                any(IndicesOptions.class),
                org.mockito.ArgumentMatchers.anyBoolean(),
                any(String[].class)
            )
        ).thenReturn(new String[0]);

        AnalyticsPlugin.DefaultEngineContextProvider ctx = new AnalyticsPlugin.DefaultEngineContextProvider(
            clusterService,
            injectedResolver,
            null
        );

        SchemaPlus schema = ctx.getContext().schema();
        // Trigger a lazy resolve so the resolver is actually invoked. Cluster state has no
        // indices, so the lookup returns null — but the resolver gets called along the way
        // (in IndexResolution.resolve's fallback path), which is what we're pinning.
        schema.getTable("any_table");

        verify(injectedResolver, atLeastOnce()).concreteIndexNames(
            same(clusterState),
            any(IndicesOptions.class),
            org.mockito.ArgumentMatchers.anyBoolean(),
            any(String[].class)
        );
    }

    /** A backend whose only interesting behaviour is how it converts exceptions. */
    private static AnalyticsSearchBackendPlugin backend(String name, UnaryOperator<Exception> converter) {
        return new AnalyticsSearchBackendPlugin() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public Exception convertException(Exception original) {
                return converter.apply(original);
            }
        };
    }

    private static AnalyticsPlugin.DefaultEngineContextProvider providerWith(AnalyticsSearchBackendPlugin... backends) {
        Map<String, AnalyticsSearchBackendPlugin> byName = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin b : backends) {
            byName.put(b.name(), b);
        }
        return new AnalyticsPlugin.DefaultEngineContextProvider(
            mock(ClusterService.class),
            mock(IndexNameExpressionResolver.class),
            byName
        );
    }

    /** A single recognising backend still gets to apply its typed status (e.g. 429). */
    public void testConvertExceptionUsesTheOnlyBackendThatRecognisesIt() {
        Exception original = new RuntimeException("native pool exhausted");
        OpenSearchStatusException converted = new OpenSearchStatusException("breaker", RestStatus.TOO_MANY_REQUESTS);

        AnalyticsPlugin.DefaultEngineContextProvider ctx = providerWith(backend("lucene", e -> e), backend("datafusion", e -> converted));

        assertSame(converted, ctx.convertException(original));
    }

    /**
     * The old implementation returned the FIRST backend whose conversion changed the exception,
     * so a backend that never ran the query could relabel another backend's failure, and which
     * one won depended on plugin registration order. With no way to identify the responsible
     * backend at this call site, an ambiguous conversion must be declined.
     */
    public void testConvertExceptionDeclinesWhenTwoBackendsBothClaimIt() {
        Exception original = new RuntimeException("boom");
        AnalyticsPlugin.DefaultEngineContextProvider ctx = providerWith(
            backend("lucene", e -> new OpenSearchStatusException("lucene says 400", RestStatus.BAD_REQUEST)),
            backend("datafusion", e -> new OpenSearchStatusException("datafusion says 429", RestStatus.TOO_MANY_REQUESTS))
        );

        assertSame("an unattributable failure must be surfaced unchanged", original, ctx.convertException(original));
    }

    public void testConvertExceptionReturnsOriginalWhenNoBackendRecognisesIt() {
        Exception original = new RuntimeException("boom");
        AnalyticsPlugin.DefaultEngineContextProvider ctx = providerWith(backend("lucene", e -> e), backend("datafusion", e -> e));

        assertSame(original, ctx.convertException(original));
    }
}
