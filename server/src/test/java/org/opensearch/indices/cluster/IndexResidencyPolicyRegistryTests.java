/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.cluster;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

/**
 * Direct unit coverage of {@link IndexResidencyPolicyRegistry} itself, mirroring {@code
 * IndexCreationStrategyRegistryTests}'s own shape -- the null-registration, exception-safety, and
 * delegation guarantees the class's own javadoc documents.
 *
 * <p>Unlike that sibling registry, "nothing registered" here does not mean "answers false" -- it means
 * "answers with the same numeric defaults {@code IndicesClusterStateService} hardcoded before this phase."
 * See {@link IndexResidencyPolicy}'s own javadoc for why.
 */
public class IndexResidencyPolicyRegistryTests extends OpenSearchTestCase {

    @After
    public void clearRegistry() {
        IndexResidencyPolicyRegistry.register(null);
    }

    public void testUnregisteredFallsBackToTheHistoricalHardcodedDefaults() {
        assertFalse(IndexResidencyPolicyRegistry.isRegistered());
        assertEquals(TimeValue.timeValueSeconds(60), IndexResidencyPolicyRegistry.sweepInterval());
        assertEquals(TimeValue.timeValueMinutes(30), IndexResidencyPolicyRegistry.idleEvictionAfter());
        assertEquals(0, IndexResidencyPolicyRegistry.maxOpen());
    }

    public void testDelegatesToTheRegisteredPolicy() {
        IndexResidencyPolicyRegistry.register(new IndexResidencyPolicy() {
            @Override
            public TimeValue sweepInterval() {
                return TimeValue.timeValueSeconds(1);
            }

            @Override
            public TimeValue idleEvictionAfter() {
                return TimeValue.timeValueSeconds(2);
            }

            @Override
            public int maxOpen() {
                return 42;
            }
        });

        assertTrue(IndexResidencyPolicyRegistry.isRegistered());
        assertEquals(TimeValue.timeValueSeconds(1), IndexResidencyPolicyRegistry.sweepInterval());
        assertEquals(TimeValue.timeValueSeconds(2), IndexResidencyPolicyRegistry.idleEvictionAfter());
        assertEquals(42, IndexResidencyPolicyRegistry.maxOpen());
    }

    public void testAThrowingPolicyFallsBackToTheDefaultsRatherThanPropagating() {
        IndexResidencyPolicyRegistry.register(new IndexResidencyPolicy() {
            @Override
            public TimeValue sweepInterval() {
                throw new RuntimeException("boom");
            }

            @Override
            public TimeValue idleEvictionAfter() {
                throw new RuntimeException("boom");
            }

            @Override
            public int maxOpen() {
                throw new RuntimeException("boom");
            }
        });

        assertEquals(
            "a broken policy must not take the sweep timer down with it",
            TimeValue.timeValueSeconds(60),
            IndexResidencyPolicyRegistry.sweepInterval()
        );
        assertEquals(TimeValue.timeValueMinutes(30), IndexResidencyPolicyRegistry.idleEvictionAfter());
        assertEquals(0, IndexResidencyPolicyRegistry.maxOpen());
    }

    public void testDefaultMethodsAreTheHistoricalDefaults() {
        IndexResidencyPolicy defaults = new IndexResidencyPolicy() {
        };
        assertEquals(TimeValue.timeValueSeconds(60), defaults.sweepInterval());
        assertEquals(TimeValue.timeValueMinutes(30), defaults.idleEvictionAfter());
        assertEquals(0, defaults.maxOpen());
    }
}
