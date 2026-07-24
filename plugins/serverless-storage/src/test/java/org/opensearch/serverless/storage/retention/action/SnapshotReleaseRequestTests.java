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
import org.opensearch.serverless.storage.retention.PitrRetentionPolicy;
import org.opensearch.test.OpenSearchTestCase;

public class SnapshotReleaseRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        SnapshotReleaseRequest original = new SnapshotReleaseRequest("idx-uuid", 2, "snap-1");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        SnapshotReleaseRequest deserialized = new SnapshotReleaseRequest(out.bytes().streamInput());

        assertEquals(original.indexUuid(), deserialized.indexUuid());
        assertEquals(original.shardId(), deserialized.shardId());
        assertEquals(original.snapshotId(), deserialized.snapshotId());
    }

    public void testValidateRejectsAMissingSnapshotId() {
        ActionRequestValidationException e = new SnapshotReleaseRequest("idx-uuid", 0, "").validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("snapshotId"));
    }

    public void testValidateAcceptsAWellFormedRequest() {
        assertNull(new SnapshotReleaseRequest("idx-uuid", 0, "snap-1").validate());
    }

    /**
     * "pitr" is PitrRetentionPolicy's own reserved pinId. removePin(String, int, String) removes
     * EVERY pin under a given pinId, so accepting this name here would let a caller wipe out
     * every internal PITR-window pin on this shard, exposing in-window manifests to GC deletion.
     */
    public void testValidateRejectsTheReservedPitrSnapshotId() {
        ActionRequestValidationException e = new SnapshotReleaseRequest("idx-uuid", 0, PitrRetentionPolicy.PITR_PIN_ID).validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("reserved"));
    }
}
