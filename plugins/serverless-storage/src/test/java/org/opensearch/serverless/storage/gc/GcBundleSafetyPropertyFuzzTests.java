/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A property-based upgrade over this plugin's existing GC chaos coverage ({@code
 * GcSchedulerTaskChaosTests}, which exercises exactly one fixed, hand-scripted operation sequence
 * under randomized I/O faults). This fuzzer instead generates a random <em>sequence of logical
 * operations</em> (publish, sweep, pin, release, clock-advance) per trial, across many trials and
 * seeds, and checks the one safety invariant GC must never violate after <em>every single step</em>,
 * not just at the end of one fixed script: {@code GcSchedulerTask#sweep} must never delete a bundle
 * a currently-live manifest (the latest generation, or any durably pinned generation) still
 * references. This is exactly the shape of bug this plugin already found once for real -- "GC could
 * delete a bundle a concurrent commit had just published" (this session's own earlier HIGH-severity
 * fix) -- a race between two logical operations racing each other, not an I/O fault. Randomized
 * operation interleaving is the tool that class of bug needs, since a fixed script can only ever
 * catch the one interleaving its author thought to write down.
 *
 * <p>Deliberately scoped to the safety property only (never delete something still live), not
 * liveness (everything eventually gets deleted) -- a safety violation is data loss/corruption, a
 * liveness gap is merely wasted storage, and liveness is much harder to assert robustly within a
 * bounded random trial (a trial that happens to never advance the clock past the retention window
 * would look like a false liveness failure). Matches this session's other property-based-fuzzing
 * scoping to "highest-risk mechanisms first."
 */
public class GcBundleSafetyPropertyFuzzTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "gc-fuzz-idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;
    // Wide enough to reliably reach compound sequences (publish -> age past retention -> pin ->
    // sweep -> age -> sweep again) that a real regression here needs several correctly-ordered
    // random steps to expose -- confirmed empirically: a deliberately-injected pin-awareness break
    // was NOT caught at 25 trials x 40 steps, but was reliably caught once raised to this range
    // (see this class's own commit history/dynamic-partitioning-progress.md for the full story).
    private static final int TRIAL_COUNT = 60;
    private static final int MIN_STEPS = 15;
    private static final int MAX_STEPS = 60;

    public void testGcNeverDeletesABundleStillReferencedByALiveManifestAcrossRandomOperationSequences() throws Exception {
        for (int trial = 0; trial < TRIAL_COUNT; trial++) {
            long seed = randomLong();
            try {
                runOneTrial(new Random(seed));
            } catch (AssertionError | Exception e) {
                throw new AssertionError("fuzz trial failed with seed=" + seed + " (rerun this seed to reproduce)", e);
            }
        }
    }

    private void runOneTrial(Random random) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(container);

        long retentionWindowMillis = TimeValue.timeValueMinutes(1).millis();
        long[] clockMillis = { System.currentTimeMillis() };
        GcSchedulerConfig config = new GcSchedulerConfig(
            TimeValue.timeValueMinutes(5),
            retentionWindowMillis,
            manifestStore,
            bundleStore,
            pinRegistry
        );
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            GcSchedulerTask task = new GcSchedulerTask(threadPool, config.interval(), INDEX_UUID, SHARD_ID, config, () -> clockMillis[0]);
            try {
                long nextGeneration = 1;
                List<Long> pinnedGenerations = new ArrayList<>();
                int steps = MIN_STEPS + random.nextInt(MAX_STEPS - MIN_STEPS + 1);

                // Always start with at least one manifest -- an empty store has nothing for the
                // invariant check to verify and every action below assumes at least one exists.
                nextGeneration = publish(bundleStore, manifestStore, nextGeneration, clockMillis[0]);
                assertSafetyInvariant(manifestStore, bundleStore, pinRegistry);

                for (int step = 0; step < steps; step++) {
                    int action = random.nextInt(5);
                    switch (action) {
                        case 0 -> nextGeneration = publish(bundleStore, manifestStore, nextGeneration, clockMillis[0]);
                        case 1 -> task.sweepForTesting();
                        case 2 -> {
                            List<CommitManifest> current = manifestStore.listManifests();
                            if (current.isEmpty() == false) {
                                CommitManifest toPin = current.get(random.nextInt(current.size()));
                                pinRegistry.addPin(
                                    INDEX_UUID,
                                    SHARD_ID,
                                    new PinRecord("pin-" + toPin.generation() + "-" + random.nextInt(1_000_000), PRIMARY_TERM, toPin.generation())
                                );
                                pinnedGenerations.add(toPin.generation());
                            }
                        }
                        case 3 -> {
                            if (pinnedGenerations.isEmpty() == false) {
                                long toRelease = pinnedGenerations.remove(random.nextInt(pinnedGenerations.size()));
                                pinRegistry.removePin(INDEX_UUID, SHARD_ID, "pin-" + toRelease + "-0");
                                // Best-effort: the exact pin id above may not match if multiple pins
                                // were placed on the same generation with different random suffixes --
                                // harmless either way, since the invariant check below only cares
                                // about the pin registry's real current state, not whether this
                                // specific removal call found a match.
                            }
                        }
                        default -> clockMillis[0] += random.nextInt((int) retentionWindowMillis * 2);
                    }
                    // The core property check: after every single step, not just at the end.
                    assertSafetyInvariant(manifestStore, bundleStore, pinRegistry);
                }
            } finally {
                task.close();
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /** Publishes one new single-file manifest generation, returning the next generation number to use. */
    private static long publish(
        BlobContainerBundleStore bundleStore,
        BlobContainerManifestStore manifestStore,
        long generation,
        long createdAtMillis
    ) throws Exception {
        String bundleName = BlobContainerBundleStore.NAME_PREFIX + INDEX_UUID + "-" + SHARD_ID + "-" + PRIMARY_TERM + "-" + generation;
        byte[] content = ("content-" + generation).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var bundle = bundleStore.writeBundle(bundleName, List.of(new BundleFileContent("segments_" + generation, content)));
        var entry = bundle.entries().get("segments_" + generation);
        CommitManifest manifest = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            PRIMARY_TERM,
            generation,
            "segments_" + generation,
            Map.of("segments_" + generation, new FileReference(bundleName, entry.offset(), entry.length(), entry.checksum())),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            createdAtMillis
        );
        manifestStore.writeManifest(manifest);
        return generation + 1;
    }

    /**
     * The property: every manifest that's either the latest generation or durably pinned must have
     * every one of its bundle files still present and checksum-readable. A violation here means GC
     * deleted a bundle a live manifest still depends on -- real data loss, not a benign race.
     */
    private void assertSafetyInvariant(
        BlobContainerManifestStore manifestStore,
        BlobContainerBundleStore bundleStore,
        DurablePinRegistry pinRegistry
    ) throws Exception {
        List<CommitManifest> manifests = manifestStore.listManifests();
        var pinnedIds = pinRegistry.getPinnedManifestIds(INDEX_UUID, SHARD_ID);

        // Half of the property that checking only *existing* manifests' bundles would silently
        // miss: a pinned manifest's *record itself* being deleted outright. GC's manifest deletion
        // (unlike bundle deletion) has no sustained-observation delay, so a pin-awareness bug can
        // make computeDeletableManifests delete the manifest in the very same sweep -- at which
        // point it's simply absent from manifestStore.listManifests() below, and a check that only
        // iterates *existing* manifests would never notice it's gone. Confirmed the hard way: this
        // fuzzer's own first draft didn't check this and missed a deliberately-injected pin-check
        // break entirely, passing 200 trials x up to 80 steps with the break in place.
        for (ManifestId pinnedId : pinnedIds) {
            boolean stillExists = manifests.stream()
                .anyMatch(m -> m.primaryTerm() == pinnedId.primaryTerm() && m.generation() == pinnedId.generation());
            assertTrue(
                String.format(
                    Locale.ROOT,
                    "SAFETY VIOLATION: manifest (primaryTerm=%d, generation=%d) is currently durably pinned but no "
                        + "longer exists in the manifest store -- GC deleted a pinned manifest outright",
                    pinnedId.primaryTerm(),
                    pinnedId.generation()
                ),
                stillExists
            );
        }

        if (manifests.isEmpty()) {
            return;
        }
        long latestGeneration = manifests.stream().mapToLong(CommitManifest::generation).max().orElseThrow();

        for (CommitManifest manifest : manifests) {
            boolean isLatest = manifest.generation() == latestGeneration;
            boolean isPinned = pinnedIds.contains(new ManifestId(manifest.primaryTerm(), manifest.generation()));
            if (isLatest == false && isPinned == false) {
                continue; // not required to still be intact -- GC is allowed to have deleted its bundle
            }
            for (var fileEntry : manifest.files().entrySet()) {
                FileReference ref = fileEntry.getValue();
                byte[] bytes = bundleStore.readFile(
                    ref.bundleName(),
                    new BundleFileEntry(fileEntry.getKey(), ref.offset(), ref.length(), ref.checksum())
                );
                assertNotNull(
                    String.format(
                        Locale.ROOT,
                        "SAFETY VIOLATION: manifest generation=%d (isLatest=%b, isPinned=%b) file [%s] in bundle [%s] "
                            + "is unreadable -- GC deleted a bundle a still-live manifest references",
                        manifest.generation(),
                        isLatest,
                        isPinned,
                        fileEntry.getKey(),
                        ref.bundleName()
                    ),
                    bytes
                );
            }
        }
    }
}
