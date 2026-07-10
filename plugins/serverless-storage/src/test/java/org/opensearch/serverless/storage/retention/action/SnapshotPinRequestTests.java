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

public class SnapshotPinRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        SnapshotPinRequest original = new SnapshotPinRequest("idx-uuid", 2, "snap-1");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        SnapshotPinRequest deserialized = new SnapshotPinRequest(out.bytes().streamInput());

        assertEquals(original.indexUuid(), deserialized.indexUuid());
        assertEquals(original.shardId(), deserialized.shardId());
        assertEquals(original.snapshotId(), deserialized.snapshotId());
    }

    public void testValidateRejectsAMissingIndexUuid() {
        ActionRequestValidationException e = new SnapshotPinRequest("", 0, "snap-1").validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("indexUuid"));
    }

    public void testValidateRejectsANegativeShardId() {
        ActionRequestValidationException e = new SnapshotPinRequest("idx-uuid", -1, "snap-1").validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("shardId"));
    }

    public void testValidateRejectsAMissingSnapshotId() {
        ActionRequestValidationException e = new SnapshotPinRequest("idx-uuid", 0, "").validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("snapshotId"));
    }

    public void testValidateAcceptsAWellFormedRequest() {
        assertNull(new SnapshotPinRequest("idx-uuid", 0, "snap-1").validate());
    }
}
