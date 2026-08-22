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

    /**
     * The indexUuid becomes a blob-path segment ({@code BlobPath.cleanPath().add(indexUuid)}) that
     * the underlying store resolves without normalising, so a {@code ..} or {@code /} in it used to
     * be a path the caller chose rather than a shard the caller owns. The authoritative control is
     * the transport action's cluster-metadata resolution; this boundary check is the cheap half
     * that refuses the obviously hostile shapes outright.
     */
    public void testValidateRejectsATraversalShapedIndexUuid() {
        ActionRequestValidationException e = new SnapshotRestoreRequest("../../../etc", 0, "snap-1").validate();
        assertNotNull(e);
        assertTrue(e.getMessage(), e.getMessage().contains("indexUuid must contain only"));
    }

    /** Every character a real generated uuid can carry must still be accepted. */
    public void testValidateAcceptsTheFullRealIndexUuidAlphabet() {
        assertNull(new SnapshotRestoreRequest("Ab0-_zZ9", 0, "snap-1").validate());
    }
}
