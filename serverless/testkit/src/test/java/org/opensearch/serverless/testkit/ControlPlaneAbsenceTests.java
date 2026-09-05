/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

/** Negative control: the absence check is worthless unless it can be made to fail. */
public class ControlPlaneAbsenceTests extends OpenSearchTestCase {

    private static final class Decoy {}

    public void testWalkerFindsAViolationPlantedThreeHopsDeep() {
        final Decoy decoy = new Decoy();
        final Object root = new Object() {
            @SuppressWarnings("unused")
            final Object level2 = new Object() {
                @SuppressWarnings("unused")
                final Object level3 = decoy;
            };
        };
        final ControlPlaneAbsence.Result r = ControlPlaneAbsence.scan(Set.of(Decoy.class.getName()), root);
        assertEquals("walker failed to reach a decoy three hops deep", Set.of(Decoy.class.getName()), r.found);
    }

    public void testWalkerReportsNothingOnACleanGraph() {
        final ControlPlaneAbsence.Result r = ControlPlaneAbsence.scan(ControlPlaneAbsence.CONTROL_PLANE, "a string", 42);
        assertTrue(r.found.isEmpty());
    }
}
