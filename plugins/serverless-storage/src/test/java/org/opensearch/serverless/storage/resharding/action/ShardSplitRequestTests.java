/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class ShardSplitRequestTests extends OpenSearchTestCase {

    public void testValidateAcceptsAWellFormedRequest() {
        ShardSplitRequest request = new ShardSplitRequest("source-idx", 0, "target-idx", 1, 0, 2);
        assertNull(request.validate());
    }

    public void testValidateRejectsAMissingSourceIndexUuid() {
        ShardSplitRequest request = new ShardSplitRequest("", 0, "target-idx", 0, 0, 2);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("source indexUuid")));
    }

    public void testValidateRejectsAMissingTargetIndexUuid() {
        ShardSplitRequest request = new ShardSplitRequest("source-idx", 0, "", 0, 0, 2);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("target indexUuid")));
    }

    public void testValidateRejectsANegativeSourceShardId() {
        ShardSplitRequest request = new ShardSplitRequest("source-idx", -1, "target-idx", 0, 0, 2);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("source shardId")));
    }

    public void testValidateRejectsFewerThanTwoPartitions() {
        ShardSplitRequest request = new ShardSplitRequest("source-idx", 0, "target-idx", 0, 0, 1);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("numPartitions")));
    }

    public void testValidateRejectsAnOutOfRangePartitionIndex() {
        ShardSplitRequest request = new ShardSplitRequest("source-idx", 0, "target-idx", 0, 3, 3);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("partitionIndex")));
    }

    public void testValidateReportsEveryViolationTogether() {
        ShardSplitRequest request = new ShardSplitRequest(null, -1, null, -1, -1, 1);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertEquals(6, validation.validationErrors().size());
    }

    public void testSerializationRoundTrip() throws Exception {
        ShardSplitRequest original = new ShardSplitRequest("source-idx", 3, "target-idx", 7, 2, 5);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardSplitRequest deserialized = new ShardSplitRequest(out.bytes().streamInput());

        assertEquals(original.sourceIndexUuid(), deserialized.sourceIndexUuid());
        assertEquals(original.sourceShardId(), deserialized.sourceShardId());
        assertEquals(original.targetIndexUuid(), deserialized.targetIndexUuid());
        assertEquals(original.targetShardId(), deserialized.targetShardId());
        assertEquals(original.partitionIndex(), deserialized.partitionIndex());
        assertEquals(original.numPartitions(), deserialized.numPartitions());
    }
}
