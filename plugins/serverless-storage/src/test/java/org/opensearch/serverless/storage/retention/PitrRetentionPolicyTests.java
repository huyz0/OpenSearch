/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.serverless.storage.gc.ManifestId;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PitrRetentionPolicyTests extends OpenSearchTestCase {

    private static CommitManifest manifest(long term, long generation, long createdAtMillis) {
        return new CommitManifest(
            "idx",
            0,
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

    public void testWindowMillisMustBePositive() {
        expectThrows(IllegalArgumentException.class, () -> PitrRetentionPolicy.computeRequiredPins(List.of(), 1000L, 0));
        expectThrows(IllegalArgumentException.class, () -> PitrRetentionPolicy.computeRequiredPins(List.of(), 1000L, -1));
    }

    public void testEveryManifestInsideTheWindowIsRequired() {
        CommitManifest inWindow1 = manifest(1, 0, 9000L);
        CommitManifest inWindow2 = manifest(1, 1, 9500L);
        // now=10000, window=2000 -> cutoff=8000, both are >= cutoff.
        Set<ManifestId> required = PitrRetentionPolicy.computeRequiredPins(List.of(inWindow1, inWindow2), 10_000L, 2_000L);

        assertEquals(Set.of(ManifestId.of(inWindow1), ManifestId.of(inWindow2)), required);
    }

    public void testTheMostRecentManifestBeforeTheCutoffIsAlsoRequired() {
        CommitManifest beforeCutoffOld = manifest(1, 0, 5000L);
        CommitManifest beforeCutoffNewer = manifest(1, 1, 7000L); // the one that should be kept
        CommitManifest inWindow = manifest(1, 2, 9000L);
        // now=10000, window=2000 -> cutoff=8000. Both beforeCutoff* are < cutoff; only the newer
        // of the two must be required, plus the in-window one.
        Set<ManifestId> required = PitrRetentionPolicy.computeRequiredPins(
            List.of(beforeCutoffOld, beforeCutoffNewer, inWindow),
            10_000L,
            2_000L
        );

        assertEquals(Set.of(ManifestId.of(beforeCutoffNewer), ManifestId.of(inWindow)), required);
    }

    public void testNoManifestsBeforeTheCutoffMeansOnlyInWindowOnesAreRequired() {
        CommitManifest onlyManifest = manifest(1, 0, 9500L);
        Set<ManifestId> required = PitrRetentionPolicy.computeRequiredPins(List.of(onlyManifest), 10_000L, 2_000L);

        assertEquals(Set.of(ManifestId.of(onlyManifest)), required);
    }

    public void testEmptyManifestListRequiresNoPins() {
        assertEquals(Set.of(), PitrRetentionPolicy.computeRequiredPins(List.of(), 10_000L, 2_000L));
    }

    public void testPinsToAddOnlyIncludesNotAlreadyPinnedGenerations() {
        ManifestId gen1 = new ManifestId(1, 1);
        ManifestId gen2 = new ManifestId(1, 2);
        Set<ManifestId> required = Set.of(gen1, gen2);
        Set<PinRecord> current = Set.of(new PinRecord("pitr", 1, 1));

        List<PinRecord> toAdd = PitrRetentionPolicy.pinsToAdd(required, current);

        assertEquals(List.of(new PinRecord("pitr", 1, 2)), toAdd);
    }

    public void testPinsToAddIsEmptyWhenEverythingRequiredIsAlreadyPinned() {
        ManifestId gen1 = new ManifestId(1, 1);
        Set<PinRecord> current = Set.of(new PinRecord("pitr", 1, 1));

        assertEquals(List.of(), PitrRetentionPolicy.pinsToAdd(Set.of(gen1), current));
    }

    public void testPinsToRemoveOnlyIncludesGenerationsNoLongerRequired() {
        PinRecord stillRequired = new PinRecord("pitr", 1, 1);
        PinRecord noLongerRequired = new PinRecord("pitr", 1, 2);
        Set<PinRecord> current = Set.of(stillRequired, noLongerRequired);
        Set<ManifestId> required = Set.of(stillRequired.toManifestId());

        List<PinRecord> toRemove = PitrRetentionPolicy.pinsToRemove(required, current);

        assertEquals(List.of(noLongerRequired), toRemove);
    }

    public void testPinsToRemoveIsEmptyWhenEverythingCurrentIsStillRequired() {
        PinRecord pin = new PinRecord("pitr", 1, 1);
        assertEquals(List.of(), PitrRetentionPolicy.pinsToRemove(Set.of(pin.toManifestId()), Set.of(pin)));
    }
}
