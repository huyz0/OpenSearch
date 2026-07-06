/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ManifestRetentionPolicyTests extends OpenSearchTestCase {

    private static CommitManifest manifest(String index, int shard, long term, long generation, long createdAtMillis) {
        return new CommitManifest(
            index,
            shard,
            term,
            generation,
            "segments_" + generation,
            Map.of("segments_" + generation, new FileReference("bundle-" + term + "-" + generation, 0, 10, 1L)),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            createdAtMillis
        );
    }

    public void testSingleManifestIsNeverDeletableRegardlessOfRetentionWindow() {
        CommitManifest only = manifest("idx", 0, 1, 0, 0L);
        List<CommitManifest> deletable = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(only),
            Long.MAX_VALUE,
            Set.of(),
            Set.of()
        );
        assertEquals(List.of(), deletable);
    }

    public void testOlderManifestPastRetentionAndUnpinnedIsDeletable() {
        CommitManifest oldGen = manifest("idx", 0, 1, 0, 1000L);
        CommitManifest latest = manifest("idx", 0, 1, 1, 2000L);

        List<CommitManifest> deletable = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(oldGen, latest),
            /*retentionCutoffMillis=*/1500L,
            Set.of(),
            Set.of()
        );

        assertEquals(List.of(oldGen), deletable);
    }

    public void testOlderManifestWithinRetentionWindowIsRetained() {
        CommitManifest oldGen = manifest("idx", 0, 1, 0, 1400L);
        CommitManifest latest = manifest("idx", 0, 1, 1, 2000L);

        // cutoff of 1000 means "created before 1000 has passed retention" -- oldGen at 1400 has not.
        List<CommitManifest> deletable = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(oldGen, latest),
            1000L,
            Set.of(),
            Set.of()
        );

        assertEquals(List.of(), deletable);
        assertEquals(
            List.of(oldGen, latest),
            ManifestRetentionPolicy.computeRetainedManifests(List.of(oldGen, latest), 1000L, Set.of(), Set.of())
        );
    }

    public void testLeasePinnedManifestIsRetainedEvenPastRetentionWindow() {
        CommitManifest oldGen = manifest("idx", 0, 1, 0, 0L);
        CommitManifest latest = manifest("idx", 0, 1, 1, 5000L);

        List<CommitManifest> deletable = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(oldGen, latest),
            Long.MAX_VALUE,
            Set.of(ManifestId.of(oldGen)),
            Set.of()
        );

        assertEquals(List.of(), deletable);
    }

    public void testDurablyPinnedManifestIsRetainedEvenWithoutALiveLease() {
        // A snapshot pin (rfc-serverless-opensearch.md section 14) protects a manifest with no
        // live reader holding it -- this is exactly the case a lease-only design would miss.
        CommitManifest snapshotted = manifest("idx", 0, 1, 0, 0L);
        CommitManifest latest = manifest("idx", 0, 1, 5, 5000L);

        List<CommitManifest> deletable = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(snapshotted, latest),
            Long.MAX_VALUE,
            Set.of(), // no lease
            Set.of(ManifestId.of(snapshotted))
        );

        assertEquals(List.of(), deletable);
    }

    public void testTermFencingMeansHigherGenerationUnderStaleTermIsNotLatest() {
        // A stale-term writer keeps publishing high generations harmlessly; a manifest from a
        // newer term (even generation 0) must still be treated as the authoritative latest.
        CommitManifest staleTermHighGen = manifest("idx", 0, 1, 999, 100L);
        CommitManifest newTermGenZero = manifest("idx", 0, 2, 0, 50L);

        List<CommitManifest> deletable = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(staleTermHighGen, newTermGenZero),
            Long.MAX_VALUE,
            Set.of(),
            Set.of()
        );

        // staleTermHighGen is older than newTermGenZero under the (term, generation) order, so it
        // is the deletable one -- even though its raw generation number is far larger.
        assertEquals(List.of(staleTermHighGen), deletable);
    }

    public void testMultipleOldGenerationsWithMixedPinning() {
        CommitManifest gen0 = manifest("idx", 0, 1, 0, 0L); // unpinned, old -> deletable
        CommitManifest gen1 = manifest("idx", 0, 1, 1, 100L); // lease-pinned -> retained
        CommitManifest gen2 = manifest("idx", 0, 1, 2, 200L); // unpinned, old -> deletable
        CommitManifest gen3Latest = manifest("idx", 0, 1, 3, 300L); // latest -> always retained

        List<CommitManifest> deletable = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(gen0, gen1, gen2, gen3Latest),
            Long.MAX_VALUE,
            Set.of(ManifestId.of(gen1)),
            Set.of()
        );

        assertEquals(Set.of(gen0, gen2), Set.copyOf(deletable));
    }

    public void testEmptyInputProducesEmptyOutput() {
        assertEquals(List.of(), ManifestRetentionPolicy.computeDeletableManifests(List.of(), 0L, Set.of(), Set.of()));
        assertEquals(List.of(), ManifestRetentionPolicy.computeRetainedManifests(List.of(), 0L, Set.of(), Set.of()));
    }

    public void testMixingDifferentShardsIsRejected() {
        CommitManifest shard0 = manifest("idx", 0, 1, 0, 0L);
        CommitManifest shard1 = manifest("idx", 1, 1, 0, 0L);

        expectThrows(
            IllegalArgumentException.class,
            () -> ManifestRetentionPolicy.computeDeletableManifests(List.of(shard0, shard1), Long.MAX_VALUE, Set.of(), Set.of())
        );
    }

    public void testMixingDifferentIndicesIsRejected() {
        CommitManifest indexA = manifest("index-a", 0, 1, 0, 0L);
        CommitManifest indexB = manifest("index-b", 0, 1, 0, 0L);

        expectThrows(
            IllegalArgumentException.class,
            () -> ManifestRetentionPolicy.computeDeletableManifests(List.of(indexA, indexB), Long.MAX_VALUE, Set.of(), Set.of())
        );
    }
}
