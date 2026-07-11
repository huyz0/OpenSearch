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

public class ShardPartitionRewriteRequestTests extends OpenSearchTestCase {

    public void testValidateAcceptsAWellFormedRequest() {
        ShardPartitionRewriteRequest request = new ShardPartitionRewriteRequest("idx", 0);
        assertNull(request.validate());
    }

    public void testValidateRejectsAMissingIndexUuid() {
        ShardPartitionRewriteRequest request = new ShardPartitionRewriteRequest("", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("indexUuid")));
    }

    public void testValidateRejectsANegativeShardId() {
        ShardPartitionRewriteRequest request = new ShardPartitionRewriteRequest("idx", -1);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("shardId")));
    }

    public void testSerializationRoundTrip() throws Exception {
        ShardPartitionRewriteRequest original = new ShardPartitionRewriteRequest("idx", 3);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardPartitionRewriteRequest deserialized = new ShardPartitionRewriteRequest(out.bytes().streamInput());

        assertEquals(original.indexUuid(), deserialized.indexUuid());
        assertEquals(original.shardId(), deserialized.shardId());
    }
}
