/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Which generation answers for an instant, asserted on the pure function rather than through a cluster.
 *
 * <p>Mirrors {@code PitrRetentionPolicyTests} in shape for the same reason the classes mirror each other:
 * the decision is arithmetic over a list, and everything that can be got wrong about it -- boundary
 * inclusivity, ties, an instant older than everything -- is cheaper and more legible to assert here than to
 * arrange in an integration test that would then only cover one of them.
 */
public class PitrRestoreResolutionTests extends OpenSearchTestCase {

    public void testTheNewestGenerationAtOrBeforeTheInstantAnswers() {
        List<CommitManifest> manifests = List.of(manifest(1, 10, 1_000), manifest(1, 11, 2_000), manifest(1, 12, 3_000));

        assertEquals(11, generationAt(manifests, 2_500));
        assertEquals(
            "the boundary is inclusive: a manifest created exactly at the instant is the state at that "
                + "instant, since it is what was current from then until the next one",
            11,
            generationAt(manifests, 2_000)
        );
        assertEquals("an instant after everything resolves to the newest", 12, generationAt(manifests, 9_999));
        assertEquals("and one exactly at the oldest resolves to the oldest", 10, generationAt(manifests, 1_000));
    }

    public void testAnInstantOlderThanEverySurvivingGenerationResolvesToNothing() {
        List<CommitManifest> manifests = List.of(manifest(1, 10, 1_000), manifest(1, 11, 2_000));

        assertTrue(
            "restoring to the oldest surviving generation instead would silently give a different point in "
                + "time than the one asked for",
            PitrRestoreResolution.newestAtOrBefore(manifests, 999).isEmpty()
        );
        assertEquals(
            "and the caller is told how far back they can go, which is the only actionable half of the answer",
            1_000L,
            PitrRestoreResolution.oldestInstant(manifests).getAsLong()
        );
    }

    public void testAShardWithNoManifestsResolvesToNothingAndHasNoOldestInstant() {
        assertTrue(PitrRestoreResolution.newestAtOrBefore(Collections.emptyList(), 1_000).isEmpty());
        assertTrue(
            "no manifests and no generation that old are different answers and must read differently",
            PitrRestoreResolution.oldestInstant(Collections.emptyList()).isEmpty()
        );
    }

    /**
     * The tie, which decides the answer far more often than it looks. Manifests are stamped from a wall
     * clock coarser than the rate a shard commits at, so two sharing a millisecond is ordinary; without an
     * order the result would follow iteration order, which is stable in a test and not in production.
     */
    public void testManifestsSharingAnInstantAreOrderedByTermThenGeneration() {
        assertEquals(
            "within a term, the higher generation is the later commit",
            11,
            generationAt(List.of(manifest(1, 10, 5_000), manifest(1, 11, 5_000)), 5_000)
        );
        assertEquals(
            "across terms the higher term wins even with a lower generation, because a term only advances "
                + "when a new writer takes over, so its generations came later in real time",
            3,
            generationAt(List.of(manifest(7, 3, 5_000), manifest(2, 90, 5_000)), 5_000)
        );
    }

    public void testOrderOfTheInputDoesNotChangeTheAnswer() {
        List<CommitManifest> shuffled = new ArrayList<>(List.of(manifest(1, 12, 3_000), manifest(1, 10, 1_000), manifest(1, 11, 2_000)));
        Collections.shuffle(shuffled, random());

        assertEquals(11, generationAt(shuffled, 2_999));
    }

    private static long generationAt(List<CommitManifest> manifests, long instantMillis) {
        Optional<CommitManifest> resolved = PitrRestoreResolution.newestAtOrBefore(manifests, instantMillis);
        assertTrue("expected a generation at or before [" + instantMillis + "]", resolved.isPresent());
        return resolved.get().generation();
    }

    private static CommitManifest manifest(long primaryTerm, long generation, long createdAtMillis) {
        // Same fixture shape as PitrRetentionPolicyTests, so the two read as one family.
        return new CommitManifest(
            "idx",
            0,
            primaryTerm,
            generation,
            "segments_" + generation,
            java.util.Map.of("segments_" + generation, new FileReference("bundle-" + primaryTerm + "-" + generation, 0, 10, 1L)),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            createdAtMillis
        );
    }
}
