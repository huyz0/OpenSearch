/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.serverless.storage.clone.ShardCloner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Round 006 item 6, built once the obstacle the plan named was resolved: {@link
 * BlobContainerPinLedgerStore} already knows how to list, read and delete a {@link PinLedger}, but
 * nothing decided <em>when</em> a ledger stops being needed. That decision needs every shard the
 * ledger names, not just the one whose container the sweep happens to be running from -- a ledger
 * lives in shard 0's container (that class's own javadoc explains why), and deleting it on shard 0's
 * evidence alone would destroy the only record of pins still held on shards 1..N. {@link
 * ShardCloner.ContainerResolver} is the per-index resolver the plan asked for: it already exists, for
 * exactly the same "one shard's container is not enough" reason clone lineage-chasing needed it.
 *
 * <h2>What "cleared" means</h2>
 *
 * A ledger is deleted only once every shard it names has been read and none reports a live {@link
 * PinRecord} under the ledger's {@code pinId} -- {@link PinRecord#isLiveAt} honours expiry, so an
 * unconfirmed pin that has lapsed on its own counts as gone, not as still held. A shard that cannot be
 * read counts as still holding the pin, not as absent: a transient failure must not look like a
 * released pin, since the cost of being wrong that way is a real leak with no record left to find it.
 *
 * <h2>What this does not do</h2>
 *
 * It does not release anything. Release is {@code SnapshotReleaseAction}'s job, driven by an operator
 * or a caller with a reason to release. This only removes the bookkeeping once release (by whatever
 * caller did it, including a caller that crashed after releasing every shard but before deleting its
 * own ledger) has already happened -- the same "record survives a failed release, and the next attempt
 * finishes the job" contract the ledger was built for, closed on the other end.
 */
public final class PinLedgerSweeper {

    private static final Logger logger = LogManager.getLogger(PinLedgerSweeper.class);

    private final BlobContainerPinLedgerStore ledgerStore;
    private final ShardCloner.ContainerResolver containerResolver;

    /**
     * @param ledgerStore the index's shard-0 ledger store to sweep.
     * @param containerResolver resolves any shard of this index's own container, for the shards a
     *                          ledger names beyond shard 0.
     */
    public PinLedgerSweeper(BlobContainerPinLedgerStore ledgerStore, ShardCloner.ContainerResolver containerResolver) {
        this.ledgerStore = ledgerStore;
        this.containerResolver = containerResolver;
    }

    /**
     * One pass: every ledger is read, judged against {@code nowMillis}, and either left alone, deleted
     * (nothing named in it is still live), or reported as abandoned (still live, but older than {@code
     * unconfirmedAbandonedAfterMillis} -- worth an operator's attention rather than silent action).
     *
     * @param nowMillis the instant every ledger in this pass is judged against, so one sweep judges
     *                  every ledger consistently and the decision is testable without a real clock.
     * @param unconfirmedAbandonedAfterMillis how old a still-live ledger has to be before it is
     *                                        reported rather than left quietly to age further.
     */
    public SweepResult sweepOnce(long nowMillis, long unconfirmedAbandonedAfterMillis) throws IOException {
        List<PinLedger> ledgers = ledgerStore.list();
        int cleared = 0;
        int withinFanOutWindow = 0;
        List<String> abandonedPastTtl = new ArrayList<>();
        for (PinLedger ledger : ledgers) {
            boolean stillLive;
            try {
                stillLive = anyShardStillHoldsPin(ledger, nowMillis);
            } catch (IOException unreadable) {
                // Fail-safe direction: a shard this sweep could not read is treated as still holding
                // the pin, not as clear. The alternative -- treating an unreadable shard as absent --
                // would delete the ledger while a real pin on that shard survives it, which is the
                // leak this class exists to close, not create.
                logger.warn(
                    "could not check every shard for pin ledger [{}] on index [{}]; leaving it in place: {}",
                    ledger.pinId(),
                    ledger.indexUuid(),
                    unreadable.toString()
                );
                continue;
            }
            if (stillLive == false) {
                // A ledger is written BEFORE the first shard is pinned, deliberately -- it is a record of
                // intent, so that a coordinator that dies mid-fan-out leaves something behind saying what it
                // was doing. That makes "no shard holds this pin" ambiguous for as long as the fan-out could
                // still be running: it means either "already released" or "not taken yet". Deleting on the
                // second reading destroys the only enumeration of a pin that is about to exist on every
                // shard of the index, and release then falls back to the index's current shard count, which
                // is wrong after a reshard. So a ledger younger than the fan-out's own provisional TTL is
                // left alone regardless of what the shards say; past that TTL the ambiguity is gone, because
                // an unconfirmed pin would have lapsed by then anyway.
                if (nowMillis - ledger.createdAtMillis() <= PinLedger.UNCONFIRMED_PIN_TTL_MILLIS) {
                    // Counted rather than silently skipped: "the sweep did nothing" and "the sweep
                    // deliberately declined to judge this one yet" are different answers, and a caller that
                    // cannot tell them apart -- a test asserting a pass really deletes, an operator asking
                    // why a ledger is still there -- is left guessing at a decision this class made on
                    // purpose.
                    withinFanOutWindow++;
                    continue;
                }
                ledgerStore.delete(ledger.pinId());
                cleared++;
                continue;
            }
            if (nowMillis - ledger.createdAtMillis() > unconfirmedAbandonedAfterMillis) {
                abandonedPastTtl.add(ledger.pinId());
            }
        }
        return new SweepResult(ledgers.size(), cleared, withinFanOutWindow, List.copyOf(abandonedPastTtl));
    }

    private boolean anyShardStillHoldsPin(PinLedger ledger, long nowMillis) throws IOException {
        for (int shardId = 0; shardId < ledger.shardCount(); shardId++) {
            BlobContainer container = containerResolver.resolve(ledger.indexUuid(), shardId);
            Set<PinRecord> pins = new BlobContainerDurablePinRegistry(container).getPins(ledger.indexUuid(), shardId);
            for (PinRecord pin : pins) {
                if (pin.pinId().equals(ledger.pinId()) && pin.isLiveAt(nowMillis)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param ledgersSeen how many ledger entries this pass read.
     * @param ledgersCleared how many were deleted because nothing they named was still live.
     * @param ledgersWithinFanOutWindow how many named no live pin but were too young to judge -- a ledger is
     *                                  written before the first shard is pinned, so until {@link
     *                                  PinLedger#UNCONFIRMED_PIN_TTL_MILLIS} has passed, "no live pin" means
     *                                  "not taken yet" as readily as "already released". These are neither
     *                                  cleared nor abandoned; they are simply not yet answerable.
     * @param abandonedPastTtl pin ids still live but older than the pass's own TTL -- worth logging,
     *                         not worth acting on: this class releases nothing.
     */
    public record SweepResult(int ledgersSeen, int ledgersCleared, int ledgersWithinFanOutWindow, List<String> abandonedPastTtl) {
    }
}
