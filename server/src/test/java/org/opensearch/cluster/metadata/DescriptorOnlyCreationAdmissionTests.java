/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

/**
 * The admission check decides which road a creation takes, and unregistered it must choose the old one.
 *
 * <p>That default is the whole of R1 for this seam. A cluster with no serverless plugin must reach
 * {@code submitStateUpdateTasks} for every index exactly as it did before the branch existed, and the only
 * thing standing between it and the new path is this predicate answering false when nothing registered one.
 *
 * <p>The throwing case matters for the same reason it matters on {@link DescriptorOnlyCreation#skipsClusterState}
 * and resolves the same way. A check that blows up must not take the request with it: the ordinary path is the
 * one that has always worked, so a broken predicate sends requests there rather than failing them or, worse,
 * admitting them off-thread on the strength of an exception.
 */
public class DescriptorOnlyCreationAdmissionTests extends OpenSearchTestCase {

    private static final Settings GATED = Settings.builder().put("index.serverless_storage.enabled", true).build();

    @After
    public void clearRegistrations() {
        DescriptorOnlyCreation.registerAdmissionCheck(null);
        DescriptorOnlyCreation.register(null);
    }

    public void testUnregisteredRefusesEverything() {
        assertFalse(
            "with nothing registered every creation must take the path it always took",
            DescriptorOnlyCreation.mayBypassClusterState(GATED)
        );
    }

    public void testARegisteredCheckDecides() {
        DescriptorOnlyCreation.registerAdmissionCheck(settings -> settings.getAsBoolean("index.serverless_storage.enabled", false));

        assertTrue(DescriptorOnlyCreation.mayBypassClusterState(GATED));
        assertFalse(
            "an ordinary creation must not be admitted off the cluster state thread",
            DescriptorOnlyCreation.mayBypassClusterState(Settings.EMPTY)
        );
    }

    public void testNullSettingsAreRefusedRatherThanThrown() {
        DescriptorOnlyCreation.registerAdmissionCheck(settings -> true);

        assertFalse(
            "a request with no settings has said nothing about wanting the new path",
            DescriptorOnlyCreation.mayBypassClusterState(null)
        );
    }

    public void testAThrowingCheckSendsTheRequestDownTheOldRoad() {
        DescriptorOnlyCreation.registerAdmissionCheck(settings -> { throw new IllegalStateException("broken"); });

        assertFalse(
            "a broken admission check must not fail the creation or admit it; it must fall back to the path "
                + "that does not depend on it",
            DescriptorOnlyCreation.mayBypassClusterState(GATED)
        );
    }

    public void testUnregisteringRestoresTheDefault() {
        DescriptorOnlyCreation.registerAdmissionCheck(settings -> true);
        assertTrue(DescriptorOnlyCreation.mayBypassClusterState(GATED));

        DescriptorOnlyCreation.registerAdmissionCheck(null);

        assertFalse("a node shutting down must leave the registry as it found it", DescriptorOnlyCreation.mayBypassClusterState(GATED));
    }
}
