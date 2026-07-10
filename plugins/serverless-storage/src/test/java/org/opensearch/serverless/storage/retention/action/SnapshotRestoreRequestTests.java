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

public class SnapshotRestoreRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        SnapshotRestoreRequest original = new SnapshotRestoreRequest("idx-uuid", 2, "snap-1");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        SnapshotRestoreRequest deserialized = new SnapshotRestoreRequest(out.bytes().streamInput());

        assertEquals(original.indexUuid(), deserialized.indexUuid());
        assertEquals(original.shardId(), deserialized.shardId());
        assertEquals(original.snapshotId(), deserialized.snapshotId());
    }

    public void testValidateRejectsAMissingSnapshotId() {
        ActionRequestValidationException e = new SnapshotRestoreRequest("idx-uuid", 0, "").validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("snapshotId"));
    }

    public void testValidateAcceptsAWellFormedRequest() {
        assertNull(new SnapshotRestoreRequest("idx-uuid", 0, "snap-1").validate());
    }
}
