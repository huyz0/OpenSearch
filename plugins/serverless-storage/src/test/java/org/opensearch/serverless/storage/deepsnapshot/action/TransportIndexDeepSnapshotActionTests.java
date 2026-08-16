/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

import org.opensearch.repositories.IndexId;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit coverage for {@link TransportIndexDeepSnapshotAction#identityConflictsWithExistingSnapshot}, the
 * predicate behind round 4's bug-hunt fix: a repository-facing {@link IndexId} this action builds from
 * an index's own real UUID (see that method's own javadoc for why) must not silently collide with a
 * different {@link IndexId} the target repository already holds under the same index name -- {@link
 * org.opensearch.repositories.RepositoryData}'s indices-by-name map has no merge function for that, so
 * an undetected collision would surface as an opaque {@code IllegalStateException: Duplicate key} deep
 * inside {@code Repository#finalizeSnapshot}'s own bookkeeping, after every shard's bytes were already
 * copied. Exercised here as a plain predicate rather than through the full transport-action/repository
 * machinery, the same "extract a testable seam" shape {@code PinLedgerSweeperTests} already uses for a
 * comparable decision.
 */
public class TransportIndexDeepSnapshotActionTests extends OpenSearchTestCase {

    public void testNoExistingEntryNeverConflicts() {
        IndexId candidate = new IndexId("my-index", "real-index-uuid");
        assertFalse(
            "an index this repository has never seen before has nothing to conflict with",
            TransportIndexDeepSnapshotAction.identityConflictsWithExistingSnapshot(null, candidate)
        );
    }

    public void testSameIdentityDoesNotConflict() {
        IndexId candidate = new IndexId("my-index", "real-index-uuid");
        IndexId existing = new IndexId("my-index", "real-index-uuid");
        assertFalse(
            "a repository entry with the exact identity this deep snapshot is about to use is not a"
                + " conflict -- this is the ordinary repeat-deep-snapshot case",
            TransportIndexDeepSnapshotAction.identityConflictsWithExistingSnapshot(existing, candidate)
        );
    }

    public void testDifferentRepositoryInternalIdConflicts() {
        IndexId candidate = new IndexId("my-index", "real-index-uuid");
        // The shape a real ordinary _snapshot call would have left behind: same index name, a
        // different (in practice, repository-assigned-random) repository-internal id.
        IndexId existing = new IndexId("my-index", "some-other-repository-internal-id");
        assertTrue(
            "an existing entry under the same name but a different repository-internal id must be"
                + " reported as a conflict, not silently overwritten or merged",
            TransportIndexDeepSnapshotAction.identityConflictsWithExistingSnapshot(existing, candidate)
        );
    }
}
