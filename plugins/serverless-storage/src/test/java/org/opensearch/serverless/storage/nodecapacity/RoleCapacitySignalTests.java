/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class RoleCapacitySignalTests extends OpenSearchTestCase {

    public void testTotalAssignedShardCountSumsAcrossNodes() {
        RoleCapacitySignal signal = new RoleCapacitySignal(
            List.of(
                new NodeCapacityEntry("node-1", "node-1", 3, false, 1, 0, false),
                new NodeCapacityEntry("node-2", "node-2", 5, false, 0, 2, false)
            ),
            0,
            List.of(),
            0,
            Map.of()
        );

        assertEquals(8, signal.totalAssignedShardCount());
    }

    public void testTotalAssignedShardCountIsZeroWithNoNodes() {
        RoleCapacitySignal signal = new RoleCapacitySignal(List.of(), 0, List.of(), 0, Map.of());

        assertEquals(0, signal.totalAssignedShardCount());
    }
}
