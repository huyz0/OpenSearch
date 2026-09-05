/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.scheduling.JitteredScheduling;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Runs the GC sweep (rfc-serverless-opensearch.md &sect;6.5) on a fixed schedule for one shard --
 * {@link BundleReferenceCounter} and {@link ManifestRetentionPolicy} were fully implemented and
 * tested, but (like {@code CompactionSchedulerTask} before it) nothing ever actually invoked them
 * from a running node: no manifest or bundle this plugin ever writes was reachable by anything that
 * deletes it, so storage grows without bound. This closes that gap the same way
 * {@code CompactionSchedulerTask} closes its own.
 *
 * <p><b>The head is read first and nothing proceeds without it.</b> This sweep used to infer "the latest
 * manifest" from its own blob listing, which is a different predicate from the RFC's and an unsafe one: a
 * manifest blob is written before the head CAS that publishes it, so a writer killed in that gap leaves a
 * manifest that a listing ranks as newest and the head does not know about at all. Judged by listing, the
 * shard's genuine live head then looked superseded, aged out, and was deleted together with the bundles only
 * it referenced -- an ordinary crash during publish turned into a shard that cannot open and segments that
 * are gone. {@link #sweep()} now reads {@code ShardStateStore} first, anchors deletability to the head's own
 * {@code (primaryTerm, generation)}, and returns without deleting anything if the head cannot be read or has
 * never published -- fail closed, because a sweep that cannot see the head has no basis for calling anything
 * garbage. See {@link ManifestRetentionPolicy}'s own javadoc for the full interleaving and for what happens
 * to manifests that turn out to be newer than the head.
 *
 * <p><b>Safety design, deliberately conservative</b>: {@link ManifestRetentionPolicy#computeDeletableManifests}
 * takes both a lease-pin set and a durable-pin set. This task always passes an <em>empty</em>
 * lease-pin set, never derived from {@code ShardDirectory} -- that tier is explicitly documented as
 * node-local, in-memory "hints" (see {@code ShardDirectoryEntry}'s own javadoc and {@code
 * ServerlessStoragePlugin}'s field comment: one instance per node, not a real cluster-wide gossip
 * store yet), so it would silently miss every reader open on any <em>other</em> node -- using it as
 * a delete-safety signal would be worse than not checking at all, since it looks like a check. The
 * two real safety mechanisms this sweep relies on instead are ({@link GcSchedulerConfig#retentionWindowMillis},
 * generous by default) and durable pins ({@link DurablePinRegistry}, real and CAS-backed, correct
 * cluster-wide regardless of which node evaluates it). A manifest only becomes deletable once it is
 * both superseded <em>and</em> has sat unpinned past the retention window -- comfortably longer than
 * any legitimate reader's own manifest-generation lag (bounded by {@code ObjectStoreReaderEngine}'s
 * 5&nbsp;s poll interval), so a reader that is merely slow to advance is never at risk of having its
 * currently-open generation deleted out from under it.
 *
 * <p>Bundles get their own, separate safety window (as required by {@link
 * BundleReferenceCounter#computeDeletableBundles}'s own javadoc): the manifest list and the bundle
 * list are two independent snapshots taken at different instants during {@link #sweep()}, so a
 * bundle written and referenced by a manifest published in between can look orphaned this tick even
 * though it is not. Rather than deleting a bundle the first tick it looks unreferenced, this task
 * tracks, per bundle name, when it was <em>first</em> continuously observed as unreferenced
 * ({@link #firstObservedOrphanedAtMillis}) and only deletes it once that has held for a full {@code
 * retentionWindowMillis} -- any manifest publish racing a sweep is picked up as soon as the very next
 * tick, which is always far inside that window given the poll/sweep cadence this class already
 * assumes elsewhere in this javadoc, removing the bundle from consideration before it can ever reach
 * the threshold.
 *
 * <p>Bundles are deleted before the manifests that stopped referencing them, never the reverse --
 * see {@code BlobContainerManifestStore#deleteManifests}'s own javadoc for why that ordering is what
 * makes a mid-sweep crash merely retry-safe rather than bundle-orphaning.
 *
 * <p>A failure here is logged-and-swallowed, same tolerance {@code CompactionSchedulerTask} already
 * has for its own tick: nothing is corrupted by a skipped or failed sweep, only deferred to the next
 * tick, since GC is strictly a space-reclamation concern, never a correctness one for anything still
 * live.
 */
public final class GcSchedulerTask implements Closeable {

    private final String indexUuid;
    private final int shardId;
    private final BlobContainerManifestStore manifestStore;
    private final BlobContainerBundleStore bundleStore;
    private final DurablePinRegistry pinRegistry;
    private final ShardStateStore shardStateStore;
    private final GcSweepStateStore sweepStateStore;
    private final long retentionWindowMillis;
    private final long maxClockSkewAllowanceMillis;
    private final LongSupplier clock;
    private final Scheduler.Cancellable task;

    /**
     * How far another node's wall clock is assumed to be allowed to differ from this one's before this
     * sweep stops trusting a cross-node timestamp comparison.
     *
     * <p>Two of the three inputs this sweep judges against time were stamped somewhere else: a manifest's
     * {@code createdAtMillis} comes from the writing node, and a pin's expiry from the pinning coordinator.
     * A sweeping node whose clock runs fast therefore sees superseded manifests as older than they are and
     * pins as expired before they are -- and both errors point the same, wrong way, towards deleting
     * something still wanted. Widening the window by this allowance costs a bounded amount of retained
     * storage; not widening it costs correctness under a failure mode (NTP drift on one node) that needs no
     * bug anywhere to occur. Five minutes is generous against any cluster whose clocks are disciplined at
     * all, and small against a thirty-minute default retention window.
     *
     * <p><b>It is a maximum, not an addition</b> -- the allowance actually applied is
     * {@code min(this, retentionWindowMillis)}. A margin that protects a configured window must not be able
     * to exceed it: added unconditionally, this turned a deliberately-tiny window into a five-minute one, a
     * three-hundred-thousand-fold amplification of a number an operator set on purpose, and the setting
     * silently stopped meaning anything below five minutes. Capping it keeps the effective window at most
     * twice what was configured, leaves the default thirty-minute window with the full allowance, and lets
     * "no retention delay" continue to mean no retention delay for the aggressive configurations (and the
     * tests) that ask for it -- which is also the honest reading of such a configuration: an operator who
     * says "delete as soon as it is superseded" has already accepted that a fast clock deletes a little
     * sooner, and the head anchoring and the durable-pin check, not this margin, are what keep that safe.
     */
    public static final long MAX_CLOCK_SKEW_ALLOWANCE_MILLIS = TimeValue.timeValueMinutes(5).millis();

    /**
     * How many retention windows an <em>unpublished</em> manifest -- one strictly newer than the head, so
     * one whose writer died between writing it and CASing the head, or is simply still in that gap -- must
     * outlive before this sweep will delete it.
     *
     * <p>It gets its own, longer window because the ordinary one is calibrated against a different question.
     * The ordinary window asks "has anything that wanted to pin this had a chance to"; this one asks "is the
     * writer that wrote this definitely not about to publish it", and a writer stalled on a slow object
     * store is a far longer event than a reader lagging a poll interval. Four windows is a judgment, not a
     * measurement: the cost of being wrong on the high side is one retained manifest and its bundles, and on
     * the low side is deleting a commit moments before it becomes the head.
     */
    public static final int ORPHAN_RETENTION_WINDOW_MULTIPLIER = 4;

    // Cross-tick bookkeeping. Still an in-memory map, because a sweep that fails partway (an injected
    // object-store fault, an outage) must not lose what it observed before failing -- but now seeded from,
    // and written back to, sweepStateStore, so that a node restart, a shard relocation, or a close/reopen no
    // longer resets the orphan clock and thereby stops any bundle from ever reaching the sustained-orphan
    // threshold at all. See GcSweepState's own javadoc for what that silently cost.
    private final Map<String, Long> firstObservedOrphanedAtMillis = new HashMap<>();

    // Node-local, in-memory, reset on restart. Manifests are immutable and write-once (see
    // BlobContainerManifestStore#writeManifest's own javadoc), so once this task has read a given
    // manifest's body, it never needs to re-fetch it on a later tick -- only whether it still
    // exists needs to be current, which listManifests(Map) still checks fresh via a real listing
    // every tick regardless of this cache. Without this, every tick pays one readBlob call per
    // manifest the shard has EVER retained, not per manifest newly written since the last tick --
    // a real, unbounded-with-history S3 GET cost this cache turns into a bounded one.
    private final Map<String, CommitManifest> manifestReadCache = new HashMap<>();

    /**
     * Schedules a recurring GC sweep for one shard.
     *
     * @param threadPool the node's thread pool, used to schedule the recurring sweep.
     * @param interval how often to run the sweep -- jittered once per instance (see {@link
     *                 JitteredScheduling}) before being handed to the scheduler, so many shards'
     *                 sweeps starting around the same moment (e.g. after a coordinated outage
     *                 recovery, rfc-serverless-opensearch.md &sect;13) don't tick in lockstep forever.
     * @param indexUuid the UUID of the index the shard belongs to.
     * @param shardId the shard's numeric id within the index.
     * @param config the shard's manifest/bundle stores, pin registry, and retention window.
     */
    public GcSchedulerTask(ThreadPool threadPool, TimeValue interval, String indexUuid, int shardId, GcSchedulerConfig config) {
        this(threadPool, interval, indexUuid, shardId, config, System::currentTimeMillis);
    }

    /** Test-only visibility: lets tests control time instead of waiting out a real retention window. */
    GcSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        String indexUuid,
        int shardId,
        GcSchedulerConfig config,
        LongSupplier clock
    ) {
        this(threadPool, interval, indexUuid, shardId, config, clock, MAX_CLOCK_SKEW_ALLOWANCE_MILLIS);
    }

    /**
     * Test-only visibility, additionally taking the cross-node clock-skew allowance. A test driving a
     * one-millisecond retention window against manifests it stamped "now" is asserting the policy, not the
     * skew margin, and would otherwise have to add five minutes of imaginary time to every timestamp to say
     * so; production always uses {@link #MAX_CLOCK_SKEW_ALLOWANCE_MILLIS}.
     */
    GcSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        String indexUuid,
        int shardId,
        GcSchedulerConfig config,
        LongSupplier clock,
        long maxClockSkewAllowanceMillis
    ) {
        // Capped at the configured window -- see MAX_CLOCK_SKEW_ALLOWANCE_MILLIS's own javadoc for why a
        // safety margin must never be larger than the thing it is a margin on.
        this.maxClockSkewAllowanceMillis = Math.min(maxClockSkewAllowanceMillis, config.retentionWindowMillis());
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.manifestStore = config.manifestStore();
        this.bundleStore = config.bundleStore();
        this.pinRegistry = config.pinRegistry();
        this.shardStateStore = config.shardStateStore();
        this.sweepStateStore = config.sweepStateStore();
        this.retentionWindowMillis = config.retentionWindowMillis();
        this.clock = clock;
        this.task = threadPool.scheduleWithFixedDelay(this::sweepSafely, JitteredScheduling.jitter(interval), ThreadPool.Names.GENERIC);
    }

    private void sweepSafely() {
        try {
            sweep();
        } catch (IOException e) {
            // See class javadoc: swallow and let the next scheduled tick reevaluate.
        }
    }

    private void sweep() throws IOException {
        long now = clock.getAsLong();

        // THE HEAD FIRST, AND NOTHING WITHOUT IT. Everything below depends on knowing which manifest was
        // actually published, and the head register is the only place that is recorded. Reading it costs one
        // register GET -- the cheapest call this sweep makes -- and an IOException here propagates out of
        // sweep() to be swallowed by sweepSafely, which is exactly the intended failure: a sweep that cannot
        // see the head deletes nothing at all and waits for the next tick. See ManifestRetentionPolicy's own
        // javadoc for the interleaving that makes "infer the latest from a listing" a data-loss bug rather
        // than an approximation.
        Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, shardId);
        if (head.isEmpty() || head.get().head().latestManifestGeneration() == 0) {
            // No head at all, or a lease-only head at generation 0 (acquireOrRenewLease can put-if-absent
            // one before anything is ever published). Either way nothing has been published, so every
            // manifest blob present is unpublished and nothing here is entitled to decide it is garbage.
            return;
        }
        ManifestId liveHead = new ManifestId(head.get().head().primaryTerm(), head.get().head().latestManifestGeneration());

        // One instant, one skew allowance, for every time comparison this sweep makes -- including the pin
        // registry's, which otherwise reads System.currentTimeMillis() itself and so disagrees with this
        // task's own (injected, in tests deliberately controlled) clock. Judging pins at now - skew, and
        // retention at now - window - skew, both widen the protected set: this sweep would rather keep
        // something a moment too long than delete something a moment too early against a clock it did not
        // set.
        long retentionCutoffMillis = now - retentionWindowMillis - maxClockSkewAllowanceMillis;
        long orphanRetentionCutoffMillis = retentionCutoffMillis - ((ORPHAN_RETENTION_WINDOW_MULTIPLIER - 1) * retentionWindowMillis);
        long pinLivenessInstantMillis = now - maxClockSkewAllowanceMillis;
        Set<ManifestId> durablyPinnedManifests = pinRegistry.getPinnedManifestIds(indexUuid, shardId, pinLivenessInstantMillis);

        GcSweepState previousState = sweepStateStore.read();
        // Merged, not replaced: an observation this process already holds is its own first-hand evidence of
        // continuous unreferencedness, whereas the persisted copy may have been written by a different node
        // or a previous incarnation. Filling in only what is missing means a restart inherits a clock that
        // was already running rather than starting a fresh one, without a stale blob ever overwriting what
        // this task saw itself.
        for (Map.Entry<String, Long> observed : previousState.firstObservedOrphanedAtMillis().entrySet()) {
            firstObservedOrphanedAtMillis.putIfAbsent(observed.getKey(), observed.getValue());
        }
        long pinsFingerprint = fingerprintOf(durablyPinnedManifests);

        // The two listings below are this sweep's entire cost, and listBlobsByPrefix is priced in the object
        // store's write tier. If the head has not moved and the pin set is byte-for-byte the one the last
        // full sweep judged, then no manifest was published and nothing was pinned or unpinned since, so the
        // answer this sweep would compute is the answer the last one already computed and acted on -- two
        // register reads (already paid for above) instead of two listings. The floor keeps this from
        // becoming "never sweep an idle shard again": a bundle written by a publish that crashed before its
        // manifest changes neither the head nor the pins, so a full sweep still runs at least once per
        // retention window regardless of how quiet the shard is.
        boolean nothingObservablyChanged = liveHead.equals(previousState.lastSweptHead())
            && pinsFingerprint == previousState.lastSweptPinsFingerprint()
            && previousState.lastFullSweepAtMillis() != 0L;
        if (nothingObservablyChanged && now - previousState.lastFullSweepAtMillis() < retentionWindowMillis) {
            return;
        }

        List<CommitManifest> manifests = manifestStore.listManifests(manifestReadCache);
        if (manifests.isEmpty()) {
            return; // never activated, or every manifest already swept -- nothing to do
        }

        List<CommitManifest> deletableManifests = ManifestRetentionPolicy.computeDeletableManifests(
            manifests,
            liveHead,
            retentionCutoffMillis,
            orphanRetentionCutoffMillis,
            Set.of(),
            durablyPinnedManifests
        );

        // Deliberately NOT an early return when deletableManifests is empty: a bundle already
        // mid-observation from an earlier tick (see below) can become deletable on a tick that has
        // no new manifest to delete, and must still be revisited -- an early return here would
        // starve that bundle of ever getting its second look.
        List<CommitManifest> retainedManifests = ManifestRetentionPolicy.computeRetainedManifests(
            manifests,
            liveHead,
            retentionCutoffMillis,
            orphanRetentionCutoffMillis,
            Set.of(),
            durablyPinnedManifests
        );
        Set<String> liveBundles = BundleReferenceCounter.computeLiveBundles(retainedManifests);
        // Unlike manifestStore.listManifests(manifestReadCache) above, this can't be given the same
        // read-caching treatment: a manifest's BODY is immutable once written, so caching it across
        // ticks is safe, but a bundle's EXISTENCE is exactly what this call needs fresh every tick --
        // this same sweep is what deletes bundles, so a stale cached name set could either miss a
        // bundle this sweep itself already removed (harmless, just wasted work re-checking a name
        // that's already gone) or, worse, miss one written since the last tick (a real correctness
        // gap in orphan detection). There's also no cheaper way to ask for "just the bundles new
        // since last time": BlobContainer#listBlobsByPrefix has no marker/cursor parameter to list
        // incrementally, only a full prefix listing every call -- investigated and confirmed there is
        // no plugin-scoped way to reduce this call's cost without extending that core interface.
        Set<String> allKnownBundles = bundleStore.listBundleNames();
        Set<String> orphanCandidateBundles = BundleReferenceCounter.computeDeletableBundles(allKnownBundles, liveBundles);

        // BundleReferenceCounter#computeDeletableBundles's own javadoc requires a safety delay before
        // actually deleting: a bundle can be written and referenced by a manifest published after this
        // sweep's manifest-list snapshot (taken above) but before this sweep's bundle-list snapshot
        // (taken above too) -- that bundle looks orphaned this tick even though a commit that references
        // it is landing concurrently. Requiring a bundle to be observed as an orphan candidate on every
        // sweep for a full retentionWindowMillis before it is actually deleted closes that window: any
        // manifest publish racing a sweep is picked up by the very next tick (well inside the window),
        // which removes the bundle from firstObservedOrphanedAtMillis before it ever reaches the
        // sustained-orphan threshold.
        //
        // The map itself is read from and written back to sweepStateStore around this sweep, so "every
        // sweep in between" means every sweep of this shard by anyone, not every sweep by this process --
        // see GcSweepState's own javadoc for why the in-memory-only version could leave a shard leaking
        // every bundle it ever merged, indefinitely and without a signal.
        firstObservedOrphanedAtMillis.keySet().retainAll(orphanCandidateBundles);
        Set<String> deletableBundles = new HashSet<>();
        for (String bundle : orphanCandidateBundles) {
            Long firstObserved = firstObservedOrphanedAtMillis.get(bundle);
            if (firstObserved == null) {
                firstObservedOrphanedAtMillis.put(bundle, now);
            } else if (now - firstObserved >= retentionWindowMillis) {
                deletableBundles.add(bundle);
            }
        }

        // Re-check pins immediately before the actual delete, not just once near the top of this
        // method: a snapshot/PITR pin request landing on this shard concurrently, after the
        // durablyPinnedManifests read above but before this point (the blob-store listing and
        // bookkeeping loops in between leave a real, if narrow, window), would otherwise never be
        // seen by this sweep and get deleted anyway. This is a cheap second read on the same
        // register blob, not a lock, so it doesn't fully close the window against a pin landing in
        // the few remaining lines between this read and the delete call itself -- but it shrinks
        // that window from "the whole sweep method" to "a few lines," which is the best available
        // without introducing real cross-writer coordination this shard's GC has never needed.
        Set<ManifestId> pinnedImmediatelyBeforeDelete = pinRegistry.getPinnedManifestIds(indexUuid, shardId, pinLivenessInstantMillis);
        List<CommitManifest> finalDeletableManifests = deletableManifests;
        if (pinnedImmediatelyBeforeDelete.isEmpty() == false) {
            finalDeletableManifests = new java.util.ArrayList<>(deletableManifests.size());
            for (CommitManifest manifest : deletableManifests) {
                if (pinnedImmediatelyBeforeDelete.contains(ManifestId.of(manifest)) == false) {
                    finalDeletableManifests.add(manifest);
                }
            }
        }

        // The same late-pin race applies to bundles, not just manifests: deletableBundles above was
        // derived from liveBundles, which was computed from the *early* durablyPinnedManifests read.
        // A pin landing in the window between that early read and pinnedImmediatelyBeforeDelete above
        // is correctly kept out of finalDeletableManifests -- but without this second filter, its
        // now-surviving manifest's exclusive bundle would still be deleted here anyway, leaving a
        // pinned manifest pointing at a bundle that no longer exists. Recompute live bundles from the
        // late pin snapshot and drop anything newly-live from this tick's delete set; a bundle
        // excluded this way simply falls out of orphanCandidateBundles on the next tick once the late
        // pin is reflected in the early read too, same as any other manifest publish racing a sweep.
        if (pinnedImmediatelyBeforeDelete.isEmpty() == false) {
            List<CommitManifest> retainedManifestsAtDelete = ManifestRetentionPolicy.computeRetainedManifests(
                manifests,
                liveHead,
                retentionCutoffMillis,
                orphanRetentionCutoffMillis,
                Set.of(),
                pinnedImmediatelyBeforeDelete
            );
            Set<String> liveBundlesAtDelete = BundleReferenceCounter.computeLiveBundles(retainedManifestsAtDelete);
            deletableBundles.removeAll(liveBundlesAtDelete);
        }

        // Bundles before manifests -- see this class's own javadoc for why that ordering, not the
        // reverse, is what keeps a mid-sweep crash merely retry-safe.
        bundleStore.deleteBundles(deletableBundles);
        manifestStore.deleteManifests(finalDeletableManifests);
        firstObservedOrphanedAtMillis.keySet().removeAll(deletableBundles);

        // Written last, and only after the deletes it describes actually landed: a state blob claiming a
        // bundle has been under observation since T, written before the sweep that would have deleted it
        // failed, is harmless; the reverse -- recording a full sweep that never happened -- would let the
        // skip check above stand down for a whole window on evidence that was never gathered. A failure
        // here propagates and the next tick simply re-observes, which is why this is not in a finally.
        sweepStateStore.write(
            new GcSweepState(
                boundedObservations(firstObservedOrphanedAtMillis),
                liveHead.primaryTerm(),
                liveHead.generation(),
                pinsFingerprint,
                now
            )
        );
    }

    /**
     * Order-independent digest of the pinned-generation set, used only to answer "is this exactly the set
     * the last full sweep judged". A {@code Set}'s own {@code hashCode} would do the same job, but writing
     * this out makes the width explicit: this value is persisted, so it has to mean the same thing across
     * JVMs and builds, which {@code Objects.hash} on a collection does not promise.
     */
    private static long fingerprintOf(Set<ManifestId> pinned) {
        long fingerprint = 0L;
        for (ManifestId id : pinned) {
            long mixed = id.primaryTerm() * 0x9E3779B97F4A7C15L ^ (id.generation() + 0x165667B19E3779F9L);
            // XOR, so the result does not depend on iteration order of a HashSet.
            fingerprint ^= mixed;
        }
        return fingerprint;
    }

    /**
     * Trims the observation map to {@link GcSweepState#MAX_TRACKED_ORPHANS}, keeping the oldest
     * observations -- they are the ones closest to becoming deletable, so dropping the newest costs at most
     * a re-observation on a later tick, while dropping the oldest would restart exactly the clocks that were
     * about to expire.
     */
    private static Map<String, Long> boundedObservations(Map<String, Long> observations) {
        if (observations.size() <= GcSweepState.MAX_TRACKED_ORPHANS) {
            return observations;
        }
        List<Map.Entry<String, Long>> byAge = new java.util.ArrayList<>(observations.entrySet());
        byAge.sort(Map.Entry.comparingByValue());
        Map<String, Long> bounded = new java.util.LinkedHashMap<>();
        for (int i = 0; i < GcSweepState.MAX_TRACKED_ORPHANS; i++) {
            bounded.put(byAge.get(i).getKey(), byAge.get(i).getValue());
        }
        return bounded;
    }

    /** Invokes {@link #sweep()} synchronously, rather than waiting out the scheduled interval -- test-only visibility. */
    void sweepForTesting() throws IOException {
        sweep();
    }

    @Override
    public void close() {
        task.cancel();
    }
}
