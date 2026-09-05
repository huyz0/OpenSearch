/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.test.OpenSearchTestCase;

/**
 * What a {@code snapshotId} is allowed to be, and why that is a security boundary rather than tidiness.
 *
 * <h2>Where the string ends up</h2>
 *
 * Two durable places, neither of which normalises it: a {@code PinRecord}'s pin id, and the blob name
 * {@code "pin-ledger-" + snapshotId} written into the index's shard-0 container. Validation used to check
 * only "non-empty" and "not the reserved PITR name", which left two real holes:
 *
 * <ul>
 * <li>{@code ../../x} escapes the shard prefix on a filesystem-backed repository -- the same class of hole
 *     {@code requireRealShard} exists to close for {@code indexUuid}, left open for this field.</li>
 * <li>{@code clone:<uuid>:0} lands inside the pin-id namespace a live clone's protection uses, and
 *     {@code _snapshot_release} removes every pin under a given id -- so a caller could strip a clone's
 *     only pin on the shard that physically holds its data.</li>
 * </ul>
 */
public class SnapshotIdValidationTests extends OpenSearchTestCase {

    private static final String REAL_UUID = "aReallyRealIndexUuid1";

    private static ActionRequestValidationException validatePin(String snapshotId) {
        return new SnapshotPinRequest(REAL_UUID, 0, snapshotId).validate();
    }

    private static ActionRequestValidationException validateRelease(String snapshotId) {
        return new SnapshotReleaseRequest(REAL_UUID, 0, snapshotId).validate();
    }

    public void testAnOrdinarySnapshotNameIsAccepted() {
        assertNull(validatePin("nightly-2026.09.05_v2"));
        assertNull(validateRelease("nightly-2026.09.05_v2"));
        assertNull(new IndexSnapshotPinRequest("idx", "nightly.2").validate());
        assertNull(new IndexSnapshotReleaseRequest("idx", "nightly.2").validate());
    }

    /** The traversal spelling: dots are legal in a name, but the one sequence that means "somewhere else" is not. */
    public void testATraversalShapedSnapshotIdIsRejectedEverywhere() {
        assertNotNull("a pin must not be able to choose a blob path", validatePin("../../x"));
        assertNotNull("nor a release", validateRelease("../../x"));
        assertNotNull(new IndexSnapshotPinRequest("idx", "../../x").validate());
        assertNotNull(new IndexSnapshotReleaseRequest("idx", "../../x").validate());
        assertNotNull("a leading dot is the other way a name stops being one", validatePin(".hidden"));
    }

    /**
     * The pin-id spoofing case. A release removes every pin sharing an id, so being able to name this
     * namespace is being able to unpin a live clone's source -- the shard that physically holds its bytes.
     */
    public void testACloneShapedSnapshotIdIsRejected() {
        String clonePinId = ShardCloner.clonePinId("some-other-index-uuid", 0);
        assertNotNull("minting a clone's pin id must be refused", validatePin(clonePinId));
        assertNotNull("and, far more dangerously, releasing one must be too", validateRelease(clonePinId));
    }

    /**
     * The deep-snapshot namespace is reserved on the operator-facing index-wide requests only. The per-shard
     * request type deliberately still accepts it, because that is the path the export action itself uses to
     * take and release its own 60-minute pin; the refusal for a hand-written request lives in the REST layer.
     */
    public void testTheDeepSnapshotPrefixIsReservedOnTheIndexWideRequests() {
        assertNotNull(new IndexSnapshotPinRequest("idx", "deep-abc123").validate());
        assertNotNull(new IndexSnapshotReleaseRequest("idx", "deep-abc123").validate());
        assertNull(
            "the shard-level request must keep accepting it -- TransportShardDeepSnapshotAction pins through it",
            validatePin("deep-abc123")
        );
    }

    public void testAnOverlongSnapshotIdIsRejected() {
        assertNotNull(validatePin("a".repeat(256)));
        assertNull(validatePin("a".repeat(255)));
    }
}
