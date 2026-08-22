/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class ShardRetentionStatsRequestTests extends OpenSearchTestCase {

    public void testValidateAcceptsAWellFormedRequest() {
        ShardRetentionStatsRequest request = new ShardRetentionStatsRequest("idx", 0);
        assertNull(request.validate());
    }

    public void testValidateRejectsAMissingIndexUuid() {
        ShardRetentionStatsRequest request = new ShardRetentionStatsRequest("", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("indexUuid")));
    }

    public void testValidateRejectsANullIndexUuid() {
        ShardRetentionStatsRequest request = new ShardRetentionStatsRequest(null, 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("indexUuid")));
    }

    public void testValidateRejectsANegativeShardId() {
        ShardRetentionStatsRequest request = new ShardRetentionStatsRequest("idx", -1);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("shardId")));
    }

    public void testValidateReportsEveryViolationTogether() {
        ShardRetentionStatsRequest request = new ShardRetentionStatsRequest(null, -1);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertEquals(2, validation.validationErrors().size());
    }

    public void testSerializationRoundTrip() throws Exception {
        ShardRetentionStatsRequest original = new ShardRetentionStatsRequest("idx", 3);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardRetentionStatsRequest deserialized = new ShardRetentionStatsRequest(out.bytes().streamInput());

        assertEquals(original.indexUuid(), deserialized.indexUuid());
        assertEquals(original.shardId(), deserialized.shardId());
    }
}
