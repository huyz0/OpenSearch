/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import java.util.function.Predicate;

/**
 * Phase D3 of {@code core-pluggability-refactor-plan.md}: a mutable, test-local {@link IndexCreationStrategy}
 * that lets a test drive {@code MetadataCreateIndexService}/{@code MetadataMappingService}'s consultation of
 * the SPI directly, without registering through {@code DescriptorOnlyCreation} -- which, once D3 relocates
 * it into {@code plugins/serverless-storage}, {@code server/src/test} can no longer reference at all.
 *
 * <p><b>What this replaces, and why it is not itself a copy of {@code DescriptorOnlyCreation}.</b> Every
 * test that used {@code DescriptorOnlyCreation.register(Predicate<IndexMetadata>)} was never actually
 * testing that class's own logic (the real "serverless_" naming convention is plugin business, covered by
 * the plugin's own tests once D3 moves it there) -- it was testing that {@code MetadataCreateIndexService}
 * correctly consults <em>whatever</em> is registered through the SPI. This class exists to keep asserting
 * exactly that, generically: a fixed, test-only namespace prefix stands in for "the plugin's naming
 * convention," and a settable predicate stands in for "the plugin's finished-metadata gate" -- neither
 * claims to be {@code DescriptorOnlyCreation}'s own behavior, only a stand-in with the same shape.
 *
 * <p>Deliberately mutable rather than a fresh instance per test: several tests (e.g. {@code
 * ServerlessNamespaceTests#testCertainlyGatedIsTheNameAndNothingElse}) assert the "nothing registered"
 * answer first and then activate the gate mid-test, matching what {@code DescriptorOnlyCreation.register}
 * let them do directly.
 */
final class TestIndexCreationStrategy implements IndexCreationStrategy {

    /** A fixed test-only namespace prefix. Not a reference to any plugin's real naming convention. */
    static final String TEST_NAMESPACE_PREFIX = "serverless_";

    private volatile boolean active = false;
    private volatile Predicate<IndexMetadata> gate = indexMetadata -> false;

    /** Activates claims() for {@link #TEST_NAMESPACE_PREFIX}-prefixed names, with the given gate. */
    void activate(Predicate<IndexMetadata> gate) {
        this.gate = gate;
        this.active = true;
    }

    /** Restores the default, unregistered-equivalent state. Call this in {@code @After}. */
    void deactivate() {
        this.active = false;
        this.gate = indexMetadata -> false;
    }

    @Override
    public boolean claims(String indexName) {
        return active && indexName != null && indexName.startsWith(TEST_NAMESPACE_PREFIX);
    }

    @Override
    public boolean skipsClusterState(IndexMetadata indexMetadata) {
        try {
            return gate.test(indexMetadata);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String describeClaimedNamespace() {
        return "the test namespace [" + TEST_NAMESPACE_PREFIX + "]";
    }
}
