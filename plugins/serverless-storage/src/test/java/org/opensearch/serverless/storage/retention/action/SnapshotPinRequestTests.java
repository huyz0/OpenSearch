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

    /**
     * The indexUuid becomes a blob-path segment ({@code BlobPath.cleanPath().add(indexUuid)}) that
     * the underlying store resolves without normalising, so a {@code ..} or {@code /} in it used to
     * be a path the caller chose rather than a shard the caller owns. The authoritative control is
     * the transport action's cluster-metadata resolution (see {@link
     * TransportSnapshotPinActionTests}); this boundary check is the cheap half that refuses the
     * obviously hostile shapes outright.
     */
    public void testValidateRejectsATraversalShapedIndexUuid() {
        for (String hostile : new String[] { "../../../etc", "..", "a/b", "a\\b", "idx uuid", "idx.uuid", "idx\u0000uuid" }) {
            ActionRequestValidationException e = new SnapshotPinRequest(hostile, 0, "snap-1").validate();
            assertNotNull("[" + hostile + "] must not be accepted as an index uuid", e);
            assertTrue(e.getMessage(), e.getMessage().contains("indexUuid must contain only"));
        }
    }

    /** Every character a real generated uuid can carry must still be accepted. */
    public void testValidateAcceptsTheFullRealIndexUuidAlphabet() {
        assertNull(new SnapshotPinRequest("Ab0-_zZ9", 0, "snap-1").validate());
        assertNull("the _na_ placeholder must remain valid", new SnapshotPinRequest("_na_", 0, "snap-1").validate());
    }

    /**
     * "pitr" is PitrRetentionPolicy's own reserved pinId for its internal PITR-window pins.
     * Accepting it here would let a caller's own replacePin call wipe out every legitimate PITR
     * pin on this shard right now, or have their own "snapshot" silently deleted by the next
     * reconciler tick that mistakes it for a stale internal entry.
     */
    public void testValidateRejectsTheReservedPitrSnapshotId() {
        ActionRequestValidationException e = new SnapshotPinRequest("idx-uuid", 0, PitrRetentionPolicy.PITR_PIN_ID).validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("reserved"));
    }
}
