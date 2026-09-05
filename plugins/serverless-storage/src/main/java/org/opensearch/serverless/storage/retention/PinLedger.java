/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * The record that an index-wide pin exists, and what it covers.
 *
 * <h2>Why a pin needs a record of its own</h2>
 *
 * A pin is written per shard, so an index-wide "snapshot" is really N pins under one id. Until this existed,
 * nothing anywhere said those N pins were one thing. That had three consequences, all of the same shape --
 * there was no way to enumerate what a pin covered without already knowing:
 *
 * <ul>
 * <li><b>Release was not retryable.</b> It walked shards 0..N from the index's current shard count. A
 *     release that failed halfway left pins behind and nothing recording which, so the operator had to know
 *     to run it again, and a shard count that had since changed would walk the wrong set.</li>
 * <li><b>A crashed coordinator leaked silently.</b> The index-level pin rolls back what it pinned if a shard
 *     <em>fails</em>, but a node that dies mid-fan-out rolls back nothing and leaves no trace. Those pins
 *     hold generations against garbage collection forever, and nothing can find them to say so.</li>
 * <li><b>Nothing could be swept.</b> A sweeper needs a list of what ought to exist; per-shard pins are only
 *     discoverable by walking every shard of every index.</li>
 * </ul>
 *
 * <p>This is the same shape core's remote-store shallow copy already uses, arrived at from the other
 * direction: there, every lock on a remote segment file is paired with a {@code shallow-snap-<uuid>} blob in
 * the repository, and snapshot deletion reads that blob to know which locks to release -- deliberately
 * releasing the lock <em>before</em> deleting the record, so a failed release leaves the record behind for
 * the next attempt. The repository is the ledger. Our pins had no repository entry to play that role,
 * because a pin taken through {@code _snapshot_pin} never goes near one, which is exactly what makes it
 * cheap. So the ledger has to be written explicitly.
 *
 * <h2>What it deliberately does not carry</h2>
 *
 * The pinned generations. Release is by pin id, so it does not need them, and a record that repeated them
 * would be a second copy of something the pins themselves already state -- free to drift, and consulted by
 * nobody. What it carries is the set membership: which shards this pin was taken across.
 */
public record PinLedger(String pinId, String indexName, String indexUuid, int shardCount, long createdAtMillis) implements Writeable {

    /**
     * How long an index-wide pin's fan-out is allowed to take before anything is entitled to conclude that
     * the pins it names do not exist.
     *
     * <p>Lives here rather than on the transport action that stamps it because two independent parties need
     * the same number: the action, which gives each provisional pin this expiry, and {@link
     * PinLedgerSweeper}, which must not delete a ledger written moments ago by a fan-out that has not
     * reached shard 0 yet. Without the second use, a sweep landing between "ledger written" and "first pin
     * taken" saw no live pin anywhere, concluded the pin had been released, and deleted the only record of a
     * 200-shard pin that then completed successfully -- leaving pins nothing could enumerate, which is
     * precisely the condition this record exists to prevent.
     *
     * <p>Ten minutes: long enough that a slow but healthy fan-out finishes well inside it, short enough that
     * an abandoned one does not hold storage meaningfully longer than the operation itself would have.
     */
    public static final long UNCONFIRMED_PIN_TTL_MILLIS = 10 * 60 * 1000L;

    /**
     * Creates a ledger entry.
     *
     * @param pinId          the pin id every shard's {@link PinRecord} was written under.
     * @param indexName      the index's name at the time of pinning, for diagnostics only.
     * @param indexUuid      the index's uuid, which is what the pins are actually addressed by.
     * @param shardCount     how many shards this pin was taken across.
     * @param createdAtMillis when the pin was taken, in epoch millis.
     */
    public PinLedger {
        Objects.requireNonNull(pinId, "pinId is required");
        Objects.requireNonNull(indexUuid, "indexUuid is required");
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be > 0, got " + shardCount);
        }
    }

    /** Reads a ledger entry from a stream. */
    public PinLedger(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readString(), in.readVInt(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pinId);
        out.writeString(indexName == null ? "" : indexName);
        out.writeString(indexUuid);
        out.writeVInt(shardCount);
        out.writeLong(createdAtMillis);
    }
}
