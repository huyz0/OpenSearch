/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.nameindex.NameIndexService;
import org.opensearch.serverless.storage.nameindex.action.ResolveIndexNamesAction;
import org.opensearch.serverless.storage.nameindex.action.ResolveIndexNamesRequest;
import org.opensearch.serverless.storage.nameindex.action.ResolveIndexNamesResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Area A's acceptance criterion, and the one thing every other test in this area could not check.
 *
 * <p>Everything else is unit-level: the structure, the overlay, the merge, the resolver's flag
 * handling. All of it can be right while the service is fed wrong, because the feed is the one part
 * that only exists inside a running cluster. This asserts the property that actually matters, which is
 * not "the resolver behaves sensibly" but <b>"the resolver agrees with core"</b>. A name index that
 * resolves an expression to a different set than {@link IndexNameExpressionResolver} does is worse than
 * no name index, because the disagreement produces wrong answers rather than errors.
 *
 * <p>The comparison is made against core's own resolver on the same cluster state, for the same
 * expressions and the same {@link IndicesOptions}, so it cannot drift as either side changes.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ServerlessStorageNameIndexIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ServerlessStoragePlugin.NAME_INDEX_ENABLED_SETTING.getKey(), true)
            .build();
    }

    public void testResolutionAgreesWithCoreAcrossTheIndexLifecycle() throws Exception {
        createIndexWithAlias("logs-2024", "all-logs");
        createIndexWithAlias("logs-2025", "all-logs");
        createIndexWithAlias("metrics-2024", null);

        assertAgreesWithCore("logs-*", "*", "all-logs", "metrics-2024", "logs-2024");

        // A delete has to reach the name index, not just cluster state.
        assertAcked(client().admin().indices().prepareDelete("logs-2024").get());
        assertAgreesWithCore("logs-*", "*", "all-logs");

        // And a create after a delete, so the overlay's tombstone does not outlive its usefulness.
        createIndexWithAlias("logs-2026", "all-logs");
        assertAgreesWithCore("logs-*", "*", "all-logs");
    }

    /**
     * Closed indices are the reason status is a byte on the entry rather than an absence, so the flags
     * that select them are worth checking against core rather than against the unit tests' own reading
     * of what those flags mean.
     */
    public void testClosedIndicesAgreeWithCore() throws Exception {
        createIndexWithAlias("open-idx", null);
        createIndexWithAlias("shut-idx", null);
        assertAcked(client().admin().indices().prepareClose("shut-idx").get());

        awaitNameIndex(() -> nameIndexService().getNameIndex().lookup("shut-idx") != null);

        for (IndicesOptions options : List.of(
            IndicesOptions.strictExpandOpen(),
            IndicesOptions.strictExpand(),
            IndicesOptions.lenientExpandOpen()
        )) {
            assertResolvesLikeCore(options, "*");
        }
    }

    /** The transport action is the tier's public surface, so it gets exercised rather than inferred. */
    public void testTransportActionResolvesThroughTheNameIndex() throws Exception {
        createIndexWithAlias("svc-a", "svc-alias");
        createIndexWithAlias("svc-b", "svc-alias");
        awaitNameIndex(() -> nameIndexService().getNameIndex().lookup("svc-b") != null);

        ResolveIndexNamesResponse response = client().execute(
            ResolveIndexNamesAction.INSTANCE,
            new ResolveIndexNamesRequest(IndicesOptions.strictExpandOpen(), "svc-alias")
        ).actionGet();

        assertEquals(List.of("svc-a", "svc-b"), response.getIndices());
    }

    private void createIndexWithAlias(String index, String alias) throws Exception {
        if (alias == null) {
            assertAcked(prepareCreate(index).setSettings(smallIndexSettings()));
        } else {
            assertAcked(
                prepareCreate(index).setSettings(smallIndexSettings()).addAlias(new org.opensearch.action.admin.indices.alias.Alias(alias))
            );
        }
        ensureGreen(index);
        awaitNameIndex(() -> nameIndexService().getNameIndex().lookup(index) != null);
    }

    private static Settings smallIndexSettings() {
        return Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build();
    }

    private void assertAgreesWithCore(String... expressions) throws Exception {
        for (String expression : expressions) {
            assertResolvesLikeCore(IndicesOptions.strictExpandOpen(), expression);
        }
    }

    /**
     * Both sides are sorted before comparison. Core returns concrete names in its own order and the name
     * index returns them in byte order; the sets are the contract, not the sequence.
     */
    private void assertResolvesLikeCore(IndicesOptions options, String expression) throws Exception {
        IndexNameExpressionResolver coreResolver = internalCluster().getInstance(IndexNameExpressionResolver.class);
        List<String> fromCore = Arrays.stream(coreResolver.concreteIndexNames(clusterService().state(), options, expression))
            .sorted()
            .collect(Collectors.toList());

        List<String> fromNameIndex = nameIndexService().resolve(options, expression).stream().sorted().collect(Collectors.toList());

        assertEquals("disagreement resolving \"" + expression + "\" with " + options, fromCore, fromNameIndex);
    }

    private NameIndexService nameIndexService() {
        return internalCluster().getInstance(NameIndexService.class);
    }

    /**
     * The service applies on cluster state application, which lands after the create call returns, so
     * reads have to wait for it rather than assume it. This is the visibility gap the consistency model
     * documents: a single change is immediately visible <em>once applied</em>, and application is
     * asynchronous with respect to the API call that caused it.
     */
    private void awaitNameIndex(java.util.function.BooleanSupplier condition) throws Exception {
        assertBusy(() -> assertTrue("the name index did not catch up with cluster state", condition.getAsBoolean()), 30, TimeUnit.SECONDS);
    }
}
