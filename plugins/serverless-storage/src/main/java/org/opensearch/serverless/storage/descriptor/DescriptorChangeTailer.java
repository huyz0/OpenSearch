/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * How a node learns about a descriptor another node wrote.
 *
 * <h2>The gap this closes</h2>
 *
 * A gated index has no cluster state entry, which is the point, and it means none of the machinery that
 * normally tells every node about an index applies. {@code DescriptorGate} appends each write to
 * {@link BlobDescriptorChangeLog} and applies it to the writing node's own name index, and its own comment
 * said what was missing: "the tailer that consumes it on those nodes is the piece still missing: today a
 * remote node learns of the change only through a rebuild."
 *
 * <p>So the log had a writer and no reader. Until this, the only bound on how stale another node's view
 * could be was the descriptor cache's freshness window, and the name index on a remote node simply never
 * heard about the change at all.
 *
 * <h2>What it does with a change</h2>
 *
 * Two things, and they are separate because they fail differently. The name index is told, so wildcards on
 * this node reflect indices created elsewhere. And the descriptor cache is invalidated for the name, so the
 * next point read goes to the store instead of answering from a copy taken before the change.
 *
 * <p>Both are idempotent, which is what lets this be a poll rather than a subscription. Re-applying a change
 * already seen costs an invalidation of something already invalid.
 *
 * <h2>Why it re-reads the bucket it stopped in</h2>
 *
 * The log's buckets are ordered and entries within a bucket are not, so there is no cursor finer than a
 * bucket. Resuming from the bucket last consumed re-reads entries already applied, which idempotency makes
 * harmless, and resuming from the <em>next</em> one would skip anything written to that bucket after the
 * read. Given the choice between repeating work and losing a change, this repeats.
 *
 * <h2>What it is not</h2>
 *
 * Not a durability mechanism and not a source of truth. A change log entry that never lands, or a tailer
 * that never runs, costs freshness: the descriptor cache still expires on its own window, and D2's sweep
 * still re-derives which gated indices are gone. That is what makes it safe for this to be best effort
 * and to swallow its own failures.
 */
public final class DescriptorChangeTailer {

    private static final Logger logger = LogManager.getLogger(DescriptorChangeTailer.class);

    private final BlobDescriptorChangeLog changeLog;
    private final DescriptorBackend backend;

    /**
     * The bucket to resume from, set at construction to the bucket current at start-up.
     *
     * <p>This used to start at null, so the first pass read the whole log back to the beginning of time.
     * That was right when the tailer fed a per-node name index, which had to be built from the history
     * before it could answer anything. It has not been right since that index was removed, and it was
     * unbounded: the log is never pruned, so a node joining a year-old cluster read a year of changes.
     *
     * <p>What the tailer does now is invalidate cache entries and release shards of deleted gated indices.
     * A starting node has an empty descriptor cache and an empty {@code openedOnDemand} set, both being
     * plain in-memory structures, so there is nothing for any of that history to act on. Every entry it
     * used to read was applied to nothing.
     */
    private final AtomicReference<String> resumeFrom;

    /**
     * Keys already consumed inside {@link #resumeFrom}'s bucket, so a revisit does not redeliver them.
     *
     * <p>Bounded by one bucket's writes and cleared whenever the cursor moves to a new bucket, because a
     * bucket the cursor has passed is never read again. Entry names are random, so there is no "everything
     * after key K" to resume from and the individual keys have to be named.
     */
    private final AtomicReference<Set<String>> consumedInBucket = new AtomicReference<>(Set.of());

    private final AtomicLong appliedCount = new AtomicLong();
    private final AtomicLong passCount = new AtomicLong();

    public DescriptorChangeTailer(BlobDescriptorChangeLog changeLog, DescriptorBackend backend) {
        this.changeLog = changeLog;
        this.backend = backend;
        // Captured now rather than on the first pass, so a change written between start-up and that pass is
        // still seen: the cursor names the bucket it starts in and entries are filtered by key, not skipped
        // wholesale.
        this.resumeFrom = new AtomicReference<>(changeLog.currentBucket());
    }

    /**
     * Reads everything since the last pass and applies it locally.
     *
     * <p>Blocking, because reading a change log is object-store I/O. The caller decides where that runs, and
     * it must not be a transport or cluster state thread.
     *
     * @return how many changes were applied on this pass
     */
    public int tailOnce() {
        passCount.incrementAndGet();
        String from = resumeFrom.get();
        // Captured before the read, not after. A bucket that rolls over mid-read would otherwise have its
        // early entries skipped next time: the reader would resume from the new bucket having never seen
        // what was written to the old one after the listing.
        String nextResume = changeLog.currentBucket();

        List<BlobDescriptorChangeLog.LoggedChange> entries;
        try {
            entries = changeLog.entriesSince(from, consumedInBucket.get());
        } catch (RuntimeException e) {
            // Left un-advanced on purpose, so the next pass retries the same range rather than stepping
            // over changes it never read.
            logger.warn("could not read the descriptor change log from bucket [{}]; will retry: {}", from, e);
            return 0;
        }
        if (entries.isEmpty()) {
            advanceTo(nextResume, entries);
            return 0;
        }

        List<DescriptorChange> changes = new ArrayList<>(entries.size());
        for (BlobDescriptorChangeLog.LoggedChange entry : entries) {
            changes.add(entry.change());
        }

        // Invalidate every name including deletes: a
        // cached live descriptor for a name deleted elsewhere is the dangerous direction, because a write
        // routed on it lands against a shard the cluster no longer believes in.
        Set<String> touched = new LinkedHashSet<>();
        for (DescriptorChange change : changes) {
            touched.add(change.name());
        }
        for (String name : touched) {
            try {
                backend.invalidate(name);
            } catch (RuntimeException e) {
                logger.debug("could not invalidate the cached descriptor for [{}]: {}", name, e);
            }
        }

        // A delete is the one change with a shard behind it. Nothing else tells a node that a gated index is
        // gone, because a gated delete produces no cluster state diff, so without this the shard stays open
        // until D2's sweep re-derives the same conclusion on its next tick. The sweep stays as the backstop:
        // a push arrives once and can be missed, and re-deriving from scratch converges regardless.
        //
        // By uuid as well as name, because a name alone cannot tell a deleted index from a new one created
        // with the same name moments later, and closing the wrong one would take down a live shard.
        for (DescriptorChange change : changes) {
            if (change.live() == false) {
                org.opensearch.cluster.metadata.GatedIndexRelease.release(
                    new org.opensearch.core.index.Index(change.name(), change.uuid())
                );
            }
        }

        // Counted as changes consumed rather than name-index entries written. This used to return what
        // NameIndexService.apply reported, which meant a tailer that invalidated caches and released
        // shards correctly still reported zero once the name index was gone.
        int applied = changes.size();

        advanceTo(nextResume, entries);
        appliedCount.addAndGet(applied);
        logger.debug("tailed [{}] descriptor changes over [{}] names from bucket [{}]", changes.size(), touched.size(), from);
        return applied;
    }

    /**
     * Moves the cursor, carrying forward the keys consumed inside the bucket that will be revisited.
     *
     * <p>Only that bucket's keys are kept. Anything older is behind the cursor and will never be listed
     * again, so remembering it would grow without bound for no benefit.
     */
    private void advanceTo(String nextResume, List<BlobDescriptorChangeLog.LoggedChange> justRead) {
        Set<String> carried = new HashSet<>();
        if (nextResume != null && nextResume.equals(resumeFrom.get())) {
            carried.addAll(consumedInBucket.get());
        }
        for (BlobDescriptorChangeLog.LoggedChange entry : justRead) {
            if (entry.bucket().equals(nextResume)) {
                carried.add(entry.key());
            }
        }
        resumeFrom.set(nextResume);
        consumedInBucket.set(Set.copyOf(carried));
    }

    /** How many changes this tailer has applied, which a test asserts rather than infers from timing. */
    public long appliedCount() {
        return appliedCount.get();
    }

    /** How many passes have run, so a test can tell "did nothing" from "never ran". */
    public long passCount() {
        return passCount.get();
    }

    /** The bucket the next pass will resume from, exposed so a test can assert the cursor advances. */
    public String resumeFrom() {
        return resumeFrom.get();
    }
}
