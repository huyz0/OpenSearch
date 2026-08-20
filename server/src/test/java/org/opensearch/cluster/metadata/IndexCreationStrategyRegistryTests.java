/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

/**
 * Direct unit coverage of {@link IndexCreationStrategyRegistry} itself -- until D2's final slice, nothing
 * exercised this class except indirectly through {@code ServerlessNamespaceTests}/{@code
 * CoreIsInertWithoutAPluginTests}'s exercise of {@code MetadataCreateIndexService}. Covers the
 * null-registration and exception-safety guarantees the class's own javadoc documents, for all four static
 * methods, not just the one D1/D2's first two slices already wired a caller to.
 */
public class IndexCreationStrategyRegistryTests extends OpenSearchTestCase {

    @After
    public void clearRegistry() {
        IndexCreationStrategyRegistry.register(null);
    }

    public void testEverythingAnswersUnclaimedWhenNothingIsRegistered() {
        assertFalse(IndexCreationStrategyRegistry.isRegistered());
        assertFalse(IndexCreationStrategyRegistry.claims("serverless_x"));
        assertFalse(IndexCreationStrategyRegistry.claims("serverless_x", request("serverless_x")));
        assertFalse(IndexCreationStrategyRegistry.skipsClusterState(anIndex("serverless_x")));
        assertFalse(
            "null indexMetadata must not NPE even with a strategy that would otherwise claim everything",
            IndexCreationStrategyRegistry.skipsClusterState(null)
        );
        assertNotNull("falls back to the interface's own generic default", IndexCreationStrategyRegistry.describeClaimedNamespace());
        assertEquals(
            "with nothing registered there is no cap on multi-index open/close, which is what an "
                + "unclaimed index has always had",
            Integer.MAX_VALUE,
            IndexCreationStrategyRegistry.maxMultiIndexStateChangeTargets()
        );
    }

    /**
     * Phase J3 of {@code core-pluggability-refactor-plan.md}: the multi-index open/close cap that {@code
     * MetadataIndexStateService} used to hardcode as 50.
     */
    public void testMaxMultiIndexStateChangeTargetsComesFromTheStrategy() {
        IndexCreationStrategyRegistry.register(new IndexCreationStrategy() {
            @Override
            public int maxMultiIndexStateChangeTargets() {
                return 7;
            }
        });
        assertEquals(7, IndexCreationStrategyRegistry.maxMultiIndexStateChangeTargets());

        IndexCreationStrategyRegistry.register(new IndexCreationStrategy() {
            @Override
            public int maxMultiIndexStateChangeTargets() {
                throw new RuntimeException("boom");
            }
        });
        assertEquals(
            "a broken strategy must not refuse every multi-index request; it falls back to no cap",
            Integer.MAX_VALUE,
            IndexCreationStrategyRegistry.maxMultiIndexStateChangeTargets()
        );
    }

    public void testDelegatesToTheRegisteredStrategy() {
        IndexCreationStrategyRegistry.register(new IndexCreationStrategy() {
            @Override
            public boolean claims(String indexName) {
                return indexName.startsWith("claimed_");
            }

            @Override
            public boolean skipsClusterState(IndexMetadata indexMetadata) {
                return indexMetadata.getIndex().getName().equals("claimed_skips-cluster-state");
            }

            @Override
            public String describeClaimedNamespace() {
                return "a test namespace";
            }
        });

        assertTrue(IndexCreationStrategyRegistry.isRegistered());
        assertTrue(IndexCreationStrategyRegistry.claims("claimed_a"));
        assertTrue(
            "the two-arg overload defaults to the one-arg answer when a strategy only overrides the name-only method",
            IndexCreationStrategyRegistry.claims("claimed_a", request("claimed_a"))
        );
        assertFalse(IndexCreationStrategyRegistry.claims("ordinary"));
        assertTrue(IndexCreationStrategyRegistry.skipsClusterState(anIndex("claimed_skips-cluster-state")));
        assertFalse(
            "claims() and skipsClusterState() are independently answered, so a claimed name need not also skip",
            IndexCreationStrategyRegistry.skipsClusterState(anIndex("claimed_a"))
        );
        assertEquals("a test namespace", IndexCreationStrategyRegistry.describeClaimedNamespace());
    }

    public void testAThrowingStrategyAnswersFalseAndFallsBackToTheDefaultDescription() {
        IndexCreationStrategyRegistry.register(new IndexCreationStrategy() {
            @Override
            public boolean claims(String indexName) {
                throw new RuntimeException("boom");
            }

            @Override
            public boolean skipsClusterState(IndexMetadata indexMetadata) {
                throw new RuntimeException("boom");
            }

            @Override
            public String describeClaimedNamespace() {
                throw new RuntimeException("boom");
            }
        });

        assertFalse(
            "a broken strategy must not strand an index unclaimed-and-unregistered-looking as claimed",
            IndexCreationStrategyRegistry.claims("serverless_x")
        );
        assertFalse(IndexCreationStrategyRegistry.claims("serverless_x", request("serverless_x")));
        assertFalse(
            "wrongly answering true here would strand an index with no record anywhere, same direction "
                + "DescriptorOnlyCreation#skipsClusterState already documents",
            IndexCreationStrategyRegistry.skipsClusterState(anIndex("serverless_x"))
        );
        assertNotNull(IndexCreationStrategyRegistry.describeClaimedNamespace());
    }

    private static CreateIndexClusterStateUpdateRequest request(String name) {
        return new CreateIndexClusterStateUpdateRequest("cause", name, name).settings(Settings.EMPTY);
    }

    private static IndexMetadata anIndex(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
