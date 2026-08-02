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
import org.opensearch.serverless.storage.nameindex.NameIndexService;

import java.util.ArrayList;
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
 * that never runs, costs freshness: the descriptor cache still expires on its own window and
 * {@code DescriptorEnumerator} can still rebuild the name index from the store. That is what makes it safe
 * for this to be best effort and to swallow its own failures.
 */
public final class DescriptorChangeTailer {

    private static final Logger logger = LogManager.getLogger(DescriptorChangeTailer.class);

    private final BlobDescriptorChangeLog changeLog;
    private final DescriptorBackend backend;
    private final NameIndexService nameIndexService;

    /**
     * The bucket to resume from, null until the first pass.
     *
     * <p>Starting from null means the first pass reads the whole log rather than only what arrives after
     * start-up. That is deliberate for a node joining an existing cluster, which has missed everything.
     */
    private final AtomicReference<String> resumeFrom = new AtomicReference<>();

    private final AtomicLong appliedCount = new AtomicLong();
    private final AtomicLong passCount = new AtomicLong();

    public DescriptorChangeTailer(BlobDescriptorChangeLog changeLog, DescriptorBackend backend, NameIndexService nameIndexService) {
        this.changeLog = changeLog;
        this.backend = backend;
        this.nameIndexService = nameIndexService;
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

        List<DescriptorChange> changes;
        try {
            changes = changeLog.since(from);
        } catch (RuntimeException e) {
            // Left un-advanced on purpose, so the next pass retries the same range rather than stepping
            // over changes it never read.
            logger.warn("could not read the descriptor change log from bucket [{}]; will retry", from, e);
            return 0;
        }
        if (changes.isEmpty()) {
            resumeFrom.set(nextResume);
            return 0;
        }

        // Invalidate before applying to the name index, and invalidate every name including deletes: a
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
                logger.debug("could not invalidate the cached descriptor for [{}]", name, e);
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

        int applied = 0;
        if (nameIndexService != null) {
            try {
                applied = nameIndexService.apply(new ArrayList<>(changes));
            } catch (RuntimeException e) {
                logger.warn("could not apply [{}] descriptor changes to the name index", changes.size(), e);
            }
        }

        resumeFrom.set(nextResume);
        appliedCount.addAndGet(applied);
        logger.debug("tailed [{}] descriptor changes over [{}] names from bucket [{}]", changes.size(), touched.size(), from);
        return applied;
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
