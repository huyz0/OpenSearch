/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class NodeCapacitySignalTests extends OpenSearchTestCase {

    public void testNodeCapacityEntryRoundTrip() throws Exception {
        NodeCapacityEntry entry = new NodeCapacityEntry("node-1", "reader-1", 3, false, 1, 2, true);
        BytesStreamOutput out = new BytesStreamOutput();
        entry.writeTo(out);
        NodeCapacityEntry roundTripped = new NodeCapacityEntry(out.bytes().streamInput());
        assertEquals(entry, roundTripped);
    }

    public void testRoleCapacitySignalRoundTrip() throws Exception {
        NodeCapacityEntry entry = new NodeCapacityEntry("node-1", "reader-1", 3, true, 3, 0, false);
        RoleCapacitySignal signal = new RoleCapacitySignal(List.of(entry), 5, List.of("node-1"), 4, Map.of("my-index", 5));
        BytesStreamOutput out = new BytesStreamOutput();
        signal.writeTo(out);
        RoleCapacitySignal roundTripped = new RoleCapacitySignal(out.bytes().streamInput());
        assertEquals(signal, roundTripped);
        assertEquals(1, roundTripped.nodeCount());
    }

    public void testNodeCapacitySignalRoundTrip() throws Exception {
        RoleCapacitySignal writer = new RoleCapacitySignal(List.of(), 2, List.of(), 1, Map.of("idx", 2));
        RoleCapacitySignal reader = new RoleCapacitySignal(
            List.of(new NodeCapacityEntry("n", "n", 1, true, 1, 1, false)),
            0,
            List.of("n"),
            0,
            Map.of()
        );
        NodeCapacitySignal signal = new NodeCapacitySignal(writer, reader);
        BytesStreamOutput out = new BytesStreamOutput();
        signal.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NodeCapacitySignal roundTripped = new NodeCapacitySignal(in);
        assertEquals(signal, roundTripped);
    }

    public void testEmptySignalHasNoNodesOrPressure() {
        NodeCapacitySignal empty = NodeCapacitySignal.empty();
        assertEquals(0, empty.writer().nodeCount());
        assertEquals(0, empty.reader().nodeCount());
        assertEquals(0, empty.writer().unassignedShardCount());
        assertEquals(0, empty.reader().sustainedPressureTicks());
    }
}
