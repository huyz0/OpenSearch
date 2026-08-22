/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Phase D3 of {@code core-pluggability-refactor-plan.md}: the pure unit test of {@link
 * DescriptorOnlyCreation#namesAServerlessIndex}, extracted from core's own {@code ServerlessNamespaceTests}
 * when this class relocated into the plugin. This is the one piece of that suite that was ever genuinely
 * testing {@code DescriptorOnlyCreation}'s own specific naming convention rather than core's consultation of
 * whatever {@code IndexCreationStrategy} is registered -- so it is the one piece that moved with the class,
 * rather than being rewritten against a test-local strategy the way the rest of that suite was.
 */
public class DescriptorOnlyCreationTests extends OpenSearchTestCase {

    public void testTheNamespaceIsAPrefixOnTheNameAndNothingElse() {
        assertTrue(DescriptorOnlyCreation.namesAServerlessIndex("serverless_tenant-42"));
        assertTrue(
            "the prefix alone is in the namespace, however useless a name it is",
            DescriptorOnlyCreation.namesAServerlessIndex("serverless_")
        );
        assertFalse(DescriptorOnlyCreation.namesAServerlessIndex("tenant-42"));
        assertFalse("a prefix in the middle is not a prefix", DescriptorOnlyCreation.namesAServerlessIndex("my-serverless_index"));
        assertFalse("the hyphenated spelling is a different name", DescriptorOnlyCreation.namesAServerlessIndex("serverless-tenant"));
        assertFalse("total on the name means null is an answer, not a throw", DescriptorOnlyCreation.namesAServerlessIndex(null));
    }
}
