/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class ShardIdleTimeRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        ShardIdleTimeRequest original = new ShardIdleTimeRequest("idx-uuid", 3);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardIdleTimeRequest deserialized = new ShardIdleTimeRequest(out.bytes().streamInput());

        assertEquals(original.indexUuid(), deserialized.indexUuid());
        assertEquals(original.shardId(), deserialized.shardId());
    }

    public void testValidateRejectsAMissingIndexUuid() {
        ShardIdleTimeRequest request = new ShardIdleTimeRequest("", 0);
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("indexUuid"));
    }

    public void testValidateRejectsANegativeShardId() {
        ShardIdleTimeRequest request = new ShardIdleTimeRequest("idx-uuid", -1);
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("shardId"));
    }

    public void testValidateAcceptsAWellFormedRequest() {
        ShardIdleTimeRequest request = new ShardIdleTimeRequest("idx-uuid", 0);
        assertNull(request.validate());
    }
}
