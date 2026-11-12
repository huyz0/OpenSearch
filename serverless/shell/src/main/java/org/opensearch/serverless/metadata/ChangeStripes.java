/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.core.common.bytes.BytesArray;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * A few registers that every update and delete of an index descriptor bumps, so that a writer can learn that the descriptors of the
 * indices it writes have not changed by reading one register for many indices, rather than one descriptor each.
 *
 * <p><b>What it is for.</b> A write is acknowledged only if its index's incarnation was confirmed by a read that began
 * within the fence window (see {@code WriteFences#INCARNATION_FENCE_MILLIS}), and the read is what brings the shard's
 * mapping and settings up to date. Read per index, that is a descriptor GET for nearly every write to an index written
 * less than once a window, on the write's path. An index's name hashes to one of {@link #STRIPES} stripes, and a stripe
 * that names no change to an index since its descriptor was last read confirms it as well as reading it again would.
 *
 * <p><b>The value.</b> A stripe holds an epoch, a generation that every bump increments, and the most recent changes,
 * each a generation, a kind and an index name. A change is either {@code D}, a change that has been applied, or
 * {@code P}, a delete about to be applied. A delete is bumped twice, {@code P} before and {@code D} after (see
 * {@link Kind#DELETED}), because a
 * reader that read the stripe and then the descriptor between the two would otherwise see a live descriptor at a
 * generation already past the delete's only entry. A {@code P} that no {@code D} follows (its deleter failed or
 * crashed) is kept, so that its index is never confirmed by the stripe again. Older entries are dropped, and a reader
 * whose last read is older than the oldest entry kept reads the descriptor; if pending entries alone would overflow,
 * the epoch moves on and every reader reads again.
 */
public final class ChangeStripes {

    /** How many stripes there are. Fixed: every node of a cluster must hash a name to the same one. */
    public static final int STRIPES = 16;
    /** How many applied changes a stripe keeps. */
    static final int KEPT = 256;
    /** How many unfinished deletes a stripe keeps before it starts a new epoch instead. */
    static final int KEPT_PENDING = 64;
    private static final int ATTEMPTS = 64;

    /** What a bump records. */
    public enum Kind {
        /** A delete about to be applied. */
        PENDING_DELETE,
        /** A delete attempt that has finished, whether it was applied or refused; it finishes the pending one before it. */
        DELETED,
        /** Any other change, applied: a create, a mapping or settings update. */
        CHANGED
    }

    private record Entry(long generation, boolean pending, String name) {
    }

    /**
     * One read of a stripe.
     *
     * @param epoch the stripe's epoch
     * @param generation its generation
     * @param oldestKept the generation of the oldest applied change it keeps, or {@code generation + 1} when none
     * @param changed every index named by an entry, by the highest generation naming it
     * @param pending every index named by a delete no applied change has followed
     * @param readStartedNanos when the read began, in {@link System#nanoTime} terms
     */
    public record Snapshot(long epoch, long generation, long oldestKept, java.util.Map<String, Long> changed, Set<String> pending,
        long readStartedNanos) {
        /**
         * Whether nothing has changed an index's descriptor since a read of the stripe at {@code epoch}/{@code since}.
         *
         * @param name the index
         * @param sinceEpoch the epoch of the earlier read
         * @param since the generation of the earlier read
         * @return true if this stripe proves the descriptor unchanged since then
         */
        public boolean unchangedSince(String name, long sinceEpoch, long since) {
            if (sinceEpoch != epoch || pending.contains(name) || since > generation) {
                return false;
            }
            if (since == generation) {
                return true;
            }
            if (oldestKept > since + 1) {
                // Entries after the earlier read were dropped: one of them may have named it.
                return false;
            }
            final Long last = changed.get(name);
            return last == null || last <= since;
        }
    }

    private final ConditionalBlobContainer container;

    /**
     * Creates the stripes of a cluster.
     *
     * @param container where they live
     */
    public ChangeStripes(BlobContainer container) {
        this.container = ConditionalBlobContainer.of(container);
    }

    /**
     * Returns the stripe an index's name hashes to.
     *
     * @param name the index
     * @return the stripe
     */
    public static int stripeOf(String name) {
        return Math.floorMod(name.hashCode(), STRIPES);
    }

    private static String blob(int stripe) {
        return String.format(Locale.ROOT, "stripe-%02d", stripe);
    }

    /**
     * Records a change to an index's descriptor in its stripe; returns once that has landed.
     *
     * @param name the index
     * @param kind what is recorded
     * @throws IOException if it could not be recorded
     */
    public void bump(String name, Kind kind) throws IOException {
        final String blob = blob(stripeOf(name));
        IOException last = null;
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            final Optional<BlobRegister> current = container.readRegister(blob);
            final State state = current.map(r -> State.parse(r.value().utf8ToString())).orElseGet(State::new);
            state.add(name, kind);
            final long expected = current.map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
            final BlobRegisterCasResult swapped = container.compareAndSwapRegister(
                blob,
                expected,
                new BytesArray(state.encode().getBytes(StandardCharsets.UTF_8))
            );
            if (swapped.applied()) {
                return;
            }
            last = new IOException("the change stripe of [" + name + "] moved under this bump");
            try {
                Thread.sleep(Math.min(50, 1L << Math.min(attempt, 5)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted bumping the change stripe of [" + name + "]", e);
            }
        }
        throw last;
    }

    /**
     * Reads one stripe.
     *
     * @param stripe the stripe
     * @return what it holds
     * @throws IOException if it could not be read
     */
    public Snapshot read(int stripe) throws IOException {
        final long started = System.nanoTime();
        final State state = container.readRegister(blob(stripe)).map(r -> State.parse(r.value().utf8ToString())).orElseGet(State::new);
        return state.snapshot(started);
    }

    /** A stripe's value, parsed. */
    private static final class State {
        private long epoch;
        private long generation;
        private final List<Entry> entries = new ArrayList<>();

        static State parse(String text) {
            final State state = new State();
            final String[] lines = text.split("\n");
            final String[] head = lines[0].split(" ");
            if (head.length != 3 || head[0].equals("v1") == false) {
                throw new IllegalArgumentException("not a change stripe: " + lines[0]);
            }
            state.epoch = Long.parseLong(head[1]);
            state.generation = Long.parseLong(head[2]);
            for (int i = 1; i < lines.length; i++) {
                if (lines[i].isEmpty()) {
                    continue;
                }
                final String[] parts = lines[i].split(" ", 3);
                state.entries.add(new Entry(Long.parseLong(parts[0]), parts[1].equals("P"), parts[2]));
            }
            return state;
        }

        String encode() {
            final StringBuilder out = new StringBuilder("v1 ").append(epoch).append(' ').append(generation).append('\n');
            for (Entry entry : entries) {
                out.append(entry.generation()).append(entry.pending() ? " P " : " D ").append(entry.name()).append('\n');
            }
            return out.toString();
        }

        void add(String name, Kind kind) {
            generation++;
            if (kind == Kind.DELETED) {
                // The delete is applied, so its pending entry is finished. Only a delete finishes one: an update that
                // lands between a delete's two bumps says nothing about whether the delete has been applied.
                entries.removeIf(entry -> entry.pending() && entry.name().equals(name));
            }
            entries.add(new Entry(generation, kind == Kind.PENDING_DELETE, name));
            int applied = 0;
            int pendingCount = 0;
            for (Entry entry : entries) {
                if (entry.pending()) {
                    pendingCount++;
                } else {
                    applied++;
                }
            }
            if (pendingCount > KEPT_PENDING) {
                // Too many unfinished deletes to keep: a new epoch makes every reader read its descriptors again.
                epoch++;
                entries.clear();
                return;
            }
            for (var it = entries.iterator(); it.hasNext() && applied > KEPT;) {
                final Entry entry = it.next();
                if (entry.pending() == false) {
                    it.remove();
                    applied--;
                }
            }
        }

        Snapshot snapshot(long startedNanos) {
            long oldest = generation + 1;
            final java.util.Map<String, Long> changed = new java.util.HashMap<>();
            final Set<String> pending = new HashSet<>();
            for (Entry entry : entries) {
                if (entry.pending()) {
                    pending.add(entry.name());
                } else {
                    oldest = Math.min(oldest, entry.generation());
                }
                changed.merge(entry.name(), entry.generation(), Math::max);
            }
            return new Snapshot(epoch, generation, oldest, java.util.Map.copyOf(changed), Set.copyOf(pending), startedNanos);
        }
    }
}
