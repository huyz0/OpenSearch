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
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * The manifest a restore publishes.
 *
 * <h2>The assertion that carries the fix</h2>
 *
 * Every test here exists for one field. A restore that rewinds the shard head onto an older generation is
 * undone by the next recovery, because the head is what recovery reads and the manifest it names supplies
 * WAL replay's floor -- so a rewound head means replay reapplies precisely the writes the restore rolled
 * back. Publishing forward with the <em>newest</em> manifest's WAL position is what fences replay off them,
 * and {@link #testTheWalPositionComesFromTheNewestManifestAndNotTheTarget} is the test that would fail if
 * someone simplified that field to {@code target.walPosition()} -- which is the natural-looking thing to do,
 * since every other field does come from the target.
 */
public class RestoreManifestSynthesisTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";

    public void testTheWalPositionComesFromTheNewestManifestAndNotTheTarget() {
        CommitManifest target = manifest(1, 5, new WalPosition("epoch-1", 100), "seg_5");
        CommitManifest newest = manifest(1, 9, new WalPosition("epoch-1", 400), "seg_9");

        CommitManifest restored = RestoreManifestSynthesis.restoredManifest(target, newest, 1, 10, 12_345);

        assertEquals(
            "the restored manifest must claim the newest WAL position, or replay resumes from behind the "
                + "rolled-back writes and puts every one of them back -- this single field is the whole fix",
            newest.walPosition(),
            restored.walPosition()
        );
    }

    public void testEveryOtherFieldComesFromTheTarget() {
        CommitManifest target = manifest(1, 5, new WalPosition("epoch-1", 100), "seg_5");
        CommitManifest newest = manifest(1, 9, new WalPosition("epoch-1", 400), "seg_9");

        CommitManifest restored = RestoreManifestSynthesis.restoredManifest(target, newest, 1, 10, 12_345);

        assertEquals("the segments file is the restored commit's, which is what restoring means", "seg_5", restored.segmentsFileName());
        assertEquals(
            "and its whole file map, copied rather than referenced so the live head names every restored "
                + "file itself and garbage collection keeps them once the pin is released",
            target.files(),
            restored.files()
        );
        assertEquals(target.maxSeqNo(), restored.maxSeqNo());
        assertEquals(target.localCheckpoint(), restored.localCheckpoint());
        assertEquals(target.mappingVersion(), restored.mappingVersion());
        assertEquals(target.totalDocCount(), restored.totalDocCount());
        assertEquals(target.deletedDocCount(), restored.deletedDocCount());
        assertEquals(INDEX_UUID, restored.indexUuid());
        assertEquals(0, restored.shardId());
    }

    public void testItIsStampedWithWhenTheRestoreHappenedNotWhenTheDataWasWritten() {
        CommitManifest target = manifest(1, 5, new WalPosition("epoch-1", 100), "seg_5");
        CommitManifest newest = manifest(1, 9, new WalPosition("epoch-1", 400), "seg_9");

        CommitManifest restored = RestoreManifestSynthesis.restoredManifest(target, newest, 1, 10, 99_999);

        assertEquals(
            "a later point-in-time restore must resolve this manifest at the instant the restore happened. "
                + "Stamping it with the target's own creation time would make two different commits claim "
                + "the same instant, and the newer one is not the one that was live then",
            99_999L,
            restored.createdAtMillis()
        );
    }

    public void testPublishingBackwardsIsRefused() {
        CommitManifest target = manifest(1, 5, new WalPosition("epoch-1", 100), "seg_5");
        CommitManifest newest = manifest(1, 9, new WalPosition("epoch-1", 400), "seg_9");

        IllegalArgumentException onto = expectThrows(
            IllegalArgumentException.class,
            () -> RestoreManifestSynthesis.restoredManifest(target, newest, 1, 9, 1)
        );
        assertTrue(
            "publishing onto a generation that already exists would overwrite a real commit with a copy of "
                + "an older one: "
                + onto.getMessage(),
            onto.getMessage().contains("must publish forward")
        );
        expectThrows(IllegalArgumentException.class, () -> RestoreManifestSynthesis.restoredManifest(target, newest, 1, 3, 1));
    }

    public void testPublishingUnderATermOlderThanTheRestoredCommitsIsRefused() {
        CommitManifest target = manifest(4, 5, new WalPosition("epoch-1", 100), "seg_5");
        CommitManifest newest = manifest(4, 9, new WalPosition("epoch-1", 400), "seg_9");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestoreManifestSynthesis.restoredManifest(target, newest, 2, 10, 1)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("older than the restored generation"));
    }

    public void testRestoringToTheNewestGenerationIsHarmless() {
        CommitManifest newest = manifest(1, 9, new WalPosition("epoch-1", 400), "seg_9");

        CommitManifest restored = RestoreManifestSynthesis.restoredManifest(newest, newest, 1, 10, 1);

        assertEquals("seg_9", restored.segmentsFileName());
        assertEquals(newest.walPosition(), restored.walPosition());
    }

    /**
     * A null WAL position is the shard-with-WAL-mirroring-disabled case, and it has to survive the copy: a
     * placeholder position instead of null is what once pinned WAL garbage collection's deletable bound at
     * zero cluster-wide, so this class must not reintroduce one.
     */
    public void testANullWalPositionIsCarriedRatherThanSubstituted() {
        CommitManifest target = manifest(1, 5, null, "seg_5");
        CommitManifest newest = manifest(1, 9, null, "seg_9");

        assertNull(RestoreManifestSynthesis.restoredManifest(target, newest, 1, 10, 1).walPosition());
    }

    public void testNewestIsByGenerationThenTerm() {
        CommitManifest a = manifest(1, 5, null, "seg_5");
        CommitManifest b = manifest(1, 9, null, "seg_9");
        CommitManifest sameGenerationHigherTerm = manifest(3, 9, null, "seg_9b");

        assertEquals(9, RestoreManifestSynthesis.newest(List.of(a, b)).generation());
        assertEquals("order must not matter", 9, RestoreManifestSynthesis.newest(List.of(b, a)).generation());
        assertEquals(
            "a tie on generation goes to the higher term, matching PitrRestoreResolution so both agree on " + "what newest means",
            3,
            RestoreManifestSynthesis.newest(List.of(b, sameGenerationHigherTerm)).primaryTerm()
        );
        expectThrows(IllegalArgumentException.class, () -> RestoreManifestSynthesis.newest(List.of()));
    }

    public void testNextGenerationClearsEveryManifestNotJustTheHead() {
        CommitManifest a = manifest(1, 5, null, "seg_5");
        CommitManifest b = manifest(1, 9, null, "seg_9");

        assertEquals(
            "taken from the whole list rather than the head, because a head may legitimately lag behind the "
                + "newest manifest on disk and publishing onto an existing generation overwrites a commit",
            10,
            RestoreManifestSynthesis.nextGeneration(List.of(a, b))
        );
        assertEquals("order must not matter", 10, RestoreManifestSynthesis.nextGeneration(List.of(b, a)));
        assertEquals("a shard with no manifests starts at zero", 0, RestoreManifestSynthesis.nextGeneration(List.of()));
    }

    private static CommitManifest manifest(long primaryTerm, long generation, WalPosition walPosition, String segmentsFileName) {
        return new CommitManifest(
            INDEX_UUID,
            0,
            primaryTerm,
            generation,
            segmentsFileName,
            Map.of(segmentsFileName, new FileReference("bundle-" + generation, 0, 120, 120)),
            generation * 10,
            generation * 10,
            walPosition,
            7,
            new PruningStats(50, null, null, Map.of()),
            1_000 + generation,
            50,
            5
        );
    }
}
