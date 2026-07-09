/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class ShardCloneRequestTests extends OpenSearchTestCase {

    public void testValidateAcceptsAWellFormedRequest() {
        ShardCloneRequest request = new ShardCloneRequest("source-idx", 0, "target-idx", 1);
        assertNull(request.validate());
    }

    public void testValidateRejectsAMissingSourceIndexUuid() {
        ShardCloneRequest request = new ShardCloneRequest("", 0, "target-idx", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("source indexUuid")));
    }

    public void testValidateRejectsANullSourceIndexUuid() {
        ShardCloneRequest request = new ShardCloneRequest(null, 0, "target-idx", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("source indexUuid")));
    }

    public void testValidateRejectsAMissingTargetIndexUuid() {
        ShardCloneRequest request = new ShardCloneRequest("source-idx", 0, "", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("target indexUuid")));
    }

    public void testValidateRejectsANegativeSourceShardId() {
        ShardCloneRequest request = new ShardCloneRequest("source-idx", -1, "target-idx", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("source shardId")));
    }

    public void testValidateRejectsANegativeTargetShardId() {
        ShardCloneRequest request = new ShardCloneRequest("source-idx", 0, "target-idx", -2);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("target shardId")));
    }

    public void testValidateReportsEveryViolationTogether() {
        ShardCloneRequest request = new ShardCloneRequest(null, -1, null, -1);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertEquals(4, validation.validationErrors().size());
    }

    public void testSerializationRoundTrip() throws Exception {
        ShardCloneRequest original = new ShardCloneRequest("source-idx", 3, "target-idx", 7);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardCloneRequest deserialized = new ShardCloneRequest(out.bytes().streamInput());

        assertEquals(original.sourceIndexUuid(), deserialized.sourceIndexUuid());
        assertEquals(original.sourceShardId(), deserialized.sourceShardId());
        assertEquals(original.targetIndexUuid(), deserialized.targetIndexUuid());
        assertEquals(original.targetShardId(), deserialized.targetShardId());
    }
}
