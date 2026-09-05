/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.gc.BlobGcCandidateLog.LoggedCandidate;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Consumes {@link BlobGcCandidateLog}: deletes what has passed its retention window and is unpinned,
 * drops the queue entry for what turns out to be durably pinned, and leaves everything else for the next
 * pass. This is the cheap, event-driven replacement for the manifest-discovery half of {@code
 * GcSchedulerTask}'s own sweep -- see that class and {@link GcCandidate}'s own javadoc for the fuller
 * reasoning and for what this deliberately does not take over (whole-shard bundle orphan detection, and
 * manifests that were written but never became head).
 *
 * <h2>Bundles</h2>
 *
 * Deleting a manifest and leaving its segment data behind reclaims kilobytes and leaks gigabytes, and both
 * this tailer and {@code GcSchedulerTask} are independently off by default -- so an operator who read this
 * class's own description of itself as "the cheap replacement for the manifest-discovery half" and enabled
 * only this one got exactly that outcome, silently. This pass therefore deletes the bundles the manifest it
 * is about to delete references <em>exclusively</em>, before deleting the manifest (see {@link
 * #exclusivelyReferencedBundles} for why that narrower question needs no sustained-orphan window). It still
 * does not do whole-shard orphan detection: a bundle written by a publish that crashed before writing any
 * manifest is named by no candidate and remains {@code GcSchedulerTask}'s job.
 *
 * <h2>Why this can safely run on more than one node, unsynchronised</h2>
 *
 * Every step here is naturally idempotent: reading an already-deleted manifest is a clean "already
 * resolved" outcome (see {@link #evaluate}), deleting an already-deleted manifest via {@code
 * deleteManifests} is a no-op (see that method's own javadoc), and resolving an already-resolved queue
 * entry is a no-op (see {@link BlobGcCandidateLog#resolve}). So two nodes racing to process the same
 * candidate do redundant work, never conflicting work -- the same property that lets this run on the
 * elected cluster manager alone (the actual wiring, chosen to avoid paying the log's bounded listing N
 * times over for one pass' worth of work) without needing a leader-election guard to be correct if that
 * assumption is ever relaxed.
 *
 * <h2>Why the retention-window check reads the manifest instead of trusting a carried timestamp</h2>
 *
 * {@link GcCandidate} deliberately does not carry {@code createdAtMillis} -- seeing it means reading the
 * superseded manifest's own body, and {@link GcCandidate}'s own javadoc explains why that read belongs
 * here, on a background tailer, rather than on {@code ObjectStoreCommitHeadPublisher}'s hot path. The read
 * is cached per candidate identity (manifests are immutable once written) so a candidate that is not yet
 * eligible does not pay for a fresh read on every subsequent pass until it becomes so.
 */
public final class GcCandidateTailer {

    private static final Logger logger = LogManager.getLogger(GcCandidateTailer.class);

    /** Resolves the {@link BlobContainer} backing one shard's own manifest/pin state, by identity alone. */
    public interface ShardContainerResolver {
        BlobContainer resolve(String indexUuid, int shardId) throws IOException;
    }

    private final BlobGcCandidateLog log;
    private final ShardContainerResolver containerResolver;
    private final long retentionWindowMillis;
    private final long lookbackMillis;
    private final long maxClockSkewAllowanceMillis;
    private final LongSupplier clock;

    // Node-local, in-memory, reset on restart -- same status as every other read-cache in this package.
    // Keyed by GcCandidate#entryName() rather than by manifest name, because this cache's whole reason to
    // exist is per-candidate: a candidate resolved and later re-superseded at the same (term, generation)
    // cannot happen (generations only increase), so there is no staleness risk to guard against here the
    // way BlobContainerManifestStore#listManifests(Map) has to for a container that keeps evolving.
    private final Map<String, Long> createdAtMillisCache = new HashMap<>();

    /**
     * @param log the candidate log to tail.
     * @param containerResolver resolves a shard's own container so this can build its manifest store and
     *                          pin registry, without needing to know every shard's container ahead of time.
     * @param retentionWindowMillis the same safety margin {@code GcSchedulerTask} uses -- a manifest must
     *                              be at least this old before it is eligible, regardless of how quickly it
     *                              was superseded, so anything that should durably pin it has had a real
     *                              chance to do so first.
     * @param lookbackMillis how far back a pass looks for pending entries -- deliberately a separate,
     *                       larger knob than {@code retentionWindowMillis}, not derived from it. If they
     *                       were the same value, any tailer downtime longer than one retention window (a
     *                       cluster-manager failover, a rolling restart) would age a genuinely still-pending
     *                       candidate's bucket out of every future pass' lookback, silently and
     *                       permanently, with nothing but {@code GcSchedulerTask}'s own unrelated sweep
     *                       ever reclaiming the manifest it named. Must be {@code >= retentionWindowMillis}
     *                       -- see {@link BlobGcCandidateLog#pruneOlderThan}, which should be configured
     *                       with this same value so the log never prunes an entry this tailer still
     *                       promises to look for.
     */
    public GcCandidateTailer(
        BlobGcCandidateLog log,
        ShardContainerResolver containerResolver,
        long retentionWindowMillis,
        long lookbackMillis
    ) {
        this(
            log,
            containerResolver,
            retentionWindowMillis,
            lookbackMillis,
            System::currentTimeMillis,
            GcSchedulerTask.MAX_CLOCK_SKEW_ALLOWANCE_MILLIS
        );
    }

    /**
     * Test seam for the clock, with the cross-node skew allowance set to zero -- a test driving a
     * one-second retention window against timestamps it stamped itself is asserting the eligibility rule,
     * not the margin, and would otherwise have to add five minutes of imaginary time to every assertion to
     * say so. Production always uses {@link GcSchedulerTask#MAX_CLOCK_SKEW_ALLOWANCE_MILLIS}.
     */
    GcCandidateTailer(
        BlobGcCandidateLog log,
        ShardContainerResolver containerResolver,
        long retentionWindowMillis,
        long lookbackMillis,
        LongSupplier clock
    ) {
        this(log, containerResolver, retentionWindowMillis, lookbackMillis, clock, 0L);
    }

    /** Test seam for the clock and the skew allowance together, for the tests that are about the margin itself. */
    GcCandidateTailer(
        BlobGcCandidateLog log,
        ShardContainerResolver containerResolver,
        long retentionWindowMillis,
        long lookbackMillis,
        LongSupplier clock,
        long maxClockSkewAllowanceMillis
    ) {
        if (retentionWindowMillis <= 0) {
            throw new IllegalArgumentException("retentionWindowMillis must be > 0, got " + retentionWindowMillis);
        }
        if (lookbackMillis < retentionWindowMillis) {
            throw new IllegalArgumentException(
                "lookbackMillis ("
                    + lookbackMillis
                    + ") must be >= retentionWindowMillis ("
                    + retentionWindowMillis
                    + "), or a candidate that has not reached its own retention window yet could age out of "
                    + "every future pass before it ever became eligible"
            );
        }
        this.log = log;
        this.containerResolver = containerResolver;
        this.retentionWindowMillis = retentionWindowMillis;
        this.lookbackMillis = lookbackMillis;
        // Same cap, same reason, as GcSchedulerTask: the allowance may not exceed the window it protects,
        // or a deliberately short window silently becomes a five-minute one.
        this.maxClockSkewAllowanceMillis = Math.min(maxClockSkewAllowanceMillis, retentionWindowMillis);
        this.clock = clock;
    }

    /** How many candidates this pass touched, so callers/tests can observe what happened without re-listing. */
    public record TailResult(int resolved, int manifestsDeleted) {
    }

    /**
     * One pass: lists every candidate the log still has pending within the retention-window lookback,
     * evaluates and (where eligible) deletes each, then evicts this tailer's local cache of anything no
     * longer pending.
     *
     * <p>The lookback is {@link #lookbackMillis}, not {@code retentionWindowMillis} -- see the constructor's
     * own javadoc for why the two must not be the same value. Either way, this is what keeps a pass' cost
     * fixed regardless of fleet size: it scales with how far back the configured lookback reaches divided
     * by the bucket width, never with how many shards exist or are currently warm.
     */
    public TailResult tailOnce() {
        List<LoggedCandidate> pending = log.entriesSince(log.bucketAtOrBefore(lookbackMillis));

        java.util.Set<String> stillPendingKeys = new java.util.HashSet<>();
        for (LoggedCandidate logged : pending) {
            stillPendingKeys.add(logged.candidate().entryName());
        }
        createdAtMillisCache.keySet().retainAll(stillPendingKeys);

        int resolved = 0;
        int deleted = 0;
        for (LoggedCandidate logged : pending) {
            try {
                Outcome outcome = evaluate(logged);
                if (outcome == Outcome.RESOLVED_DELETED) {
                    resolved++;
                    deleted++;
                } else if (outcome == Outcome.RESOLVED_PROTECTED || outcome == Outcome.RESOLVED_ALREADY_GONE) {
                    resolved++;
                }
                // Outcome.NOT_YET_ELIGIBLE: left exactly as it was, for the next pass to find again.
            } catch (IOException | RuntimeException e) {
                // One candidate failing must not lose the rest of the pass -- same tolerance every other
                // sweep in this package already has for its own per-item failures.
                logger.debug(
                    "could not evaluate GC candidate [{}/{} {}/{}], will retry next pass: {}",
                    logged.candidate().indexUuid(),
                    logged.candidate().shardId(),
                    logged.candidate().primaryTerm(),
                    logged.candidate().generation(),
                    e
                );
            }
        }
        return new TailResult(resolved, deleted);
    }

    private enum Outcome {
        NOT_YET_ELIGIBLE,
        RESOLVED_ALREADY_GONE,
        RESOLVED_PROTECTED,
        RESOLVED_DELETED
    }

    private Outcome evaluate(LoggedCandidate logged) throws IOException {
        GcCandidate candidate = logged.candidate();
        BlobContainer container = containerResolver.resolve(candidate.indexUuid(), candidate.shardId());
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);

        CommitManifest manifest;
        Long cachedCreatedAtMillis = createdAtMillisCache.get(candidate.entryName());
        if (cachedCreatedAtMillis != null) {
            // Already known eligible-by-age on a prior pass but skipped that time for some other reason
            // (a transient failure resolving the pin registry, say) -- re-check the rest without paying
            // for the read again. manifest itself is not needed below in this branch: the age check is
            // already satisfied, and deletion only needs the manifest's own identity, which the candidate
            // already carries.
            manifest = null;
        } else {
            try {
                manifest = manifestStore.readManifest(candidate.primaryTerm(), candidate.generation());
            } catch (NoSuchFileException e) {
                // Already deleted -- by GcSchedulerTask's own sweep, which keeps running underneath this,
                // or by another node's tailer pass. Either way there is nothing left to do but retire the
                // queue entry.
                log.resolve(logged);
                return Outcome.RESOLVED_ALREADY_GONE;
            }
            createdAtMillisCache.put(candidate.entryName(), manifest.createdAtMillis());
            cachedCreatedAtMillis = manifest.createdAtMillis();
        }

        // Both comparisons are against timestamps another node stamped -- the manifest's createdAtMillis by
        // its writer, a pin's expiry by its coordinator -- so both get the same skew allowance
        // GcSchedulerTask applies, and in the same direction: wait longer before deleting, treat a pin as
        // live for longer. A node whose clock ran fast would otherwise consider a manifest past retention
        // before it was, and every provisional pin expired before it was, which are the two errors that
        // combine into "deleted the generation a snapshot was still being taken of".
        long nowMillis = clock.getAsLong();
        if (nowMillis - cachedCreatedAtMillis < retentionWindowMillis + maxClockSkewAllowanceMillis) {
            return Outcome.NOT_YET_ELIGIBLE;
        }

        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(container);
        Set<ManifestId> pinned = pinRegistry.getPinnedManifestIds(
            candidate.indexUuid(),
            candidate.shardId(),
            nowMillis - maxClockSkewAllowanceMillis
        );
        if (pinned.contains(candidate.manifestId())) {
            // Durably pinned -- this generation is protected, not garbage. Retiring the queue entry here
            // is deliberate, not merely "nothing to do": if the pin is later released, nothing currently
            // re-appends a fresh candidate for it (a stated, honest limitation -- see this class's own
            // package notes), so GcSchedulerTask's own periodic sweep remains what eventually reclaims a
            // manifest that was pinned when this tailer looked and is not any more.
            log.resolve(logged);
            return Outcome.RESOLVED_PROTECTED;
        }

        // Re-read the manifest here if the cache satisfied the age check above without ever fetching it
        // (the cachedCreatedAtMillis-from-cache branch): deletion needs the manifest object itself, not
        // just its age, and NoSuchFileException here means the same "already gone" outcome as above --
        // resolved either way, just not by this call.
        if (manifest == null) {
            try {
                manifest = manifestStore.readManifest(candidate.primaryTerm(), candidate.generation());
            } catch (NoSuchFileException e) {
                log.resolve(logged);
                return Outcome.RESOLVED_ALREADY_GONE;
            }
        }
        // Bundles first, then the manifest -- the ordering BlobContainerManifestStore#deleteManifests
        // documents, and the one this class used to invert by not handling bundles at all. See
        // exclusivelyReferencedBundles below for why an operator who enables only the tailer no longer
        // reclaims kilobytes of manifest while leaking every byte of segment data underneath it.
        Set<String> exclusiveBundles = exclusivelyReferencedBundles(manifestStore, manifest);
        if (exclusiveBundles.isEmpty() == false) {
            new BlobContainerBundleStore(container).deleteBundles(exclusiveBundles);
        }
        manifestStore.deleteManifests(List.of(manifest));
        log.resolve(logged);
        return Outcome.RESOLVED_DELETED;
    }

    /**
     * The bundles {@code doomed} references that no other manifest of the same shard does.
     *
     * <h4>Why this is safe without the sustained-orphan window {@code GcSchedulerTask} needs</h4>
     *
     * That window exists because a periodic sweep takes two independent listings and asks "is this bundle
     * referenced by anything", which a bundle written moments ago by a publish still in flight answers
     * wrongly. This asks a strictly narrower question: of the bundles <em>this one already-superseded,
     * past-retention, unpinned manifest</em> names, which does nothing else name. A bundle in that set
     * cannot become referenced later, because a future commit's file map is derived from the head's, and the
     * head is in the listing taken here -- so any bundle a future commit will inherit is referenced by a
     * manifest this computation already counted as live. And a bundle written but not yet named by any
     * manifest (the crashed-publish case the sweep's window is really for) is never in {@code doomed}'s own
     * reference set to begin with, so it is never a candidate here at all.
     *
     * <p>The listing is taken fresh rather than from a cache: what matters is which manifests exist right
     * now, and this is the one place in this class where a stale answer would delete live data rather than
     * merely defer work.
     *
     * @param manifestStore the shard's manifest store.
     * @param doomed the manifest about to be deleted.
     * @return bundle names safe to delete along with it; empty if every one is still shared.
     */
    private static Set<String> exclusivelyReferencedBundles(BlobContainerManifestStore manifestStore, CommitManifest doomed)
        throws IOException {
        Set<String> exclusive = new java.util.HashSet<>(doomed.referencedBundles());
        if (exclusive.isEmpty()) {
            return exclusive;
        }
        for (CommitManifest other : manifestStore.listManifests()) {
            if (other.primaryTerm() == doomed.primaryTerm() && other.generation() == doomed.generation()) {
                continue;
            }
            exclusive.removeAll(other.referencedBundles());
            if (exclusive.isEmpty()) {
                return exclusive;
            }
        }
        return exclusive;
    }
}
