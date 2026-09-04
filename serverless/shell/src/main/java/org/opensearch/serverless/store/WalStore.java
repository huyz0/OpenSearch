/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The write-ahead log that closes the durability window.
 *
 * <p>Before this, a shard's data reached the object store only when a commit was published, so
 * everything written since the last publish was lost on failover. Phase 6's zombie test measured
 * exactly that: the zombie's unpublished document was gone, and the notes recorded it as the WAL gap
 * rather than a fencing failure. This is that gap closed — a record is durable before the write is
 * acknowledged, so a successor can replay what the previous writer never published.
 *
 * <p><b>Term-scoped, like segments.</b> A writer at term T appends under {@code wal/t=T}, so a zombie
 * appending after it lost the shard writes where the successor is not reading, for the same reason and
 * by the same mechanism as §9.6's fencing of segment blobs.
 *
 * <p><b>Truncation happens on two different rules, and they are not the same rule.</b> Records under the
 * publishing writer's own term are trimmed a cycle late, for the reason below. Records under
 * <em>older</em> terms are dropped outright, because a successor replays them all before it starts and
 * a publish at a higher term therefore contains every one of them.
 *
 * <p><b>Same-term truncation is deliberately conservative.</b> Records are appended <em>before</em> being applied
 * to the engine, so a record present at the moment of a flush is not necessarily <em>in</em> that
 * flush. Deleting what the current publish snapshotted could therefore drop a write that had not landed
 * yet. Instead each publish deletes what the <em>previous</em> publish saw, which guarantees a full
 * cycle elapsed and the record was applied and committed. Replaying a few extra records costs nothing
 * because replay is idempotent — see {@link WalRecord}.
 *
 * <p><b>One blob per append, and an append may be a batch.</b> A single write still costs one PUT; a
 * bulk request landing on one shard costs one PUT for the whole batch rather than one per document.
 * See {@link #append(long, List)} for the container format and why an older writer's log still reads.
 */
public final class WalStore {

    private final BlobStore blobStore;
    private final BlobPath shardBase;
    /** A record's blob name: a zero-padded ordinal, and nothing else in the container is ours. */
    private static final java.util.regex.Pattern RECORD_NAME = java.util.regex.Pattern.compile("\\d{20}");
    /** Where takeover cutoffs are recorded. Not a term, so term listings and term pruning both skip it. */
    private static final String SEALS = "seals";
    /** A seal's blob name. Deliberately not a bare ordinal, so nothing counting records can count a seal. */
    private static final java.util.regex.Pattern SEAL_NAME = java.util.regex.Pattern.compile("seal-\\d{20}");

    private final AtomicLong ordinal = new AtomicLong();
    private volatile long seededTerm = -1L;
    private volatile List<String> previousSnapshot = List.of();

    /**
     * Creates a WAL for one shard.
     *
     * @param blobStore the backing store
     * @param shardBase the shard's base path
     */
    public WalStore(BlobStore blobStore, BlobPath shardBase) {
        this.blobStore = blobStore;
        this.shardBase = shardBase;
    }

    private BlobContainer containerFor(long term) {
        return blobStore.blobContainer(shardBase.add("wal").add(SegmentPublisher.termSegment(term)));
    }

    /**
     * Appends a record, durably, before the caller applies the write.
     *
     * @param term the writing node's term
     * @param record the write
     * @throws IOException if the append fails
     */
    public void append(long term, WalRecord record) throws IOException {
        append(term, List.of(record));
    }

    /**
     * Appends a batch as <em>one</em> blob, durably, before the caller applies any of it.
     *
     * <p>This is the group commit the class documentation used to say belonged here and was not built.
     * A bulk request landing on one shard costs one object-store PUT rather than one per document, which
     * is the difference between a write path priced per document and one priced per request.
     *
     * <p><b>All of it is durable before any of it is applied</b>, which is the same contract a single
     * write has and not a weaker one. A failure between the append and the application replays the whole
     * batch, and replay is an idempotent redo of state.
     *
     * <p><b>The container is newline-delimited JSON, and that is chosen rather than convenient.</b> A
     * serialized record cannot contain a raw newline -- a document's source is a JSON string, so any
     * newline inside it is escaped -- so the delimiter cannot collide with the content. It also makes a
     * one-record blob byte-identical to what this class wrote before batching existed, so a log written
     * by an older writer is read by this one with no version field, no migration and no special case.
     *
     * @param term the writing node's term
     * @param records the writes, in the order they must replay
     * @throws IOException if the append fails
     */
    public void append(long term, List<WalRecord> records) throws IOException {
        if (records.isEmpty()) {
            // Not an error, and not a blob. An empty batch is what a bulk request whose items all belong
            // to other shards leaves for this one, and a PUT for it would say nothing.
            return;
        }
        final java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        for (WalRecord record : records) {
            if (buffer.size() > 0) {
                buffer.write('\n');
            }
            buffer.write(org.opensearch.core.common.bytes.BytesReference.toBytes(record.toBytes()));
        }
        final byte[] bytes = buffer.toByteArray();
        // Zero-padded so a lexicographic listing is a chronological one. Within a term there is exactly
        // one writer, so the ordinal needs no coordination. A batch takes one ordinal and its records
        // replay in the order they were written, so the total order over a term is (ordinal, position).
        seed(term);
        final String name = String.format(java.util.Locale.ROOT, "%020d", ordinal.incrementAndGet());
        // Refused on collision, never overwritten. A store instance is rebuilt on every local release of a
        // shard, and a shard reopened at the same term replayed records 1..N and then appended its next
        // write as record 1 -- over the record it had just replayed. With the ordinal seeded from what
        // the container holds this does not happen, and if it somehow did, failing the write (which
        // releases the shard, see ServerlessNode) is the honest outcome rather than a silent overwrite.
        containerFor(term).writeBlob(name, new ByteArrayInputStream(bytes), bytes.length, true);
    }

    /**
     * Starts the ordinal after the highest record already in the term, once per term.
     *
     * <p>One listing, paid the first time a term is written to by this instance. Within a term there is
     * exactly one writer, so after seeding the counter needs no coordination.
     */
    private synchronized void seed(long term) throws IOException {
        if (seededTerm == term) {
            return;
        }
        long highest = 0L;
        for (String name : containerFor(term).listBlobs().keySet()) {
            if (RECORD_NAME.matcher(name).matches()) {
                highest = Math.max(highest, Long.parseLong(name));
            }
        }
        ordinal.set(highest);
        seededTerm = term;
    }

    /**
     * Reads every record still in the log, in the order it was written.
     *
     * <p>Ordered by term and then by ordinal, so a later write to the same document id wins on replay
     * exactly as it did originally.
     *
     * @return the records to replay
     * @throws IOException if listing or reading fails
     */
    public List<WalRecord> replayable() throws IOException {
        return replayable(null);
    }

    /**
     * Takes a snapshot of how far the log had been written, for use as a replay cutoff.
     *
     * <p><b>Why a successor needs one.</b> A writer that has lost its shard-head but not yet noticed keeps
     * appending under <em>its own</em> term, and a successor replays every term it finds, so those records
     * are replayed as though they had been acknowledged before the takeover. They were not. The term-scoped
     * key path this class's documentation calls a fence separates the two writers' <em>blobs</em>; it does
     * not stop a successor from reading the older term, which is exactly what recovery does.
     *
     * <p>The cutoff closes that. Everything acknowledged before the takeover is already in the log, because
     * a write is appended before it is acknowledged, so a snapshot taken at the successor's activation
     * necessarily includes all of it. Anything appearing after that snapshot was written by a node that no
     * longer owns the shard, and is dropped. A write in flight at the instant of the snapshot is dropped
     * too, and correctly: its caller never received an acknowledgement.
     *
     * <p>This is a per-term high-water mark rather than the set of names, because names are zero-padded
     * ordinals assigned by a single writer per term, so "everything at or below this name" is exactly
     * "everything written before this moment" and costs one string per term to carry.
     *
     * @return the highest record name seen in each term, empty string for a term with no records yet
     * @throws IOException if listing fails
     */
    public Map<Long, String> position() throws IOException {
        final Map<Long, String> highest = new java.util.HashMap<>();
        for (Map.Entry<Long, BlobContainer> term : termsInOrder().entrySet()) {
            String max = "";
            for (String name : term.getValue().listBlobs().keySet()) {
                if (RECORD_NAME.matcher(name).matches() && name.compareTo(max) > 0) {
                    max = name;
                }
            }
            highest.put(term.getKey(), max);
        }
        return highest;
    }

    /**
     * Reads the records still in the log that fall at or before a cutoff.
     *
     * <p>A term absent from the cutoff did not exist when the snapshot was taken, so every record under it
     * was written afterwards and none of it replays. That is what drops a zombie that starts a fresh term
     * directory, as well as one continuing an old one.
     *
     * @param cutoff a snapshot from {@link #position()}, or null to read everything
     * @return the records to replay, ordered by term and then by ordinal
     * @throws IOException if listing or reading fails
     */
    public List<WalRecord> replayable(Map<Long, String> cutoff) throws IOException {
        final List<WalRecord> records = new ArrayList<>();
        for (Map.Entry<Long, BlobContainer> entry : termsInOrder().entrySet()) {
            final BlobContainer container = entry.getValue();
            if (cutoff != null && cutoff.containsKey(entry.getKey()) == false) {
                continue;
            }
            final String limit = cutoff == null ? null : cutoff.get(entry.getKey());
            final List<String> names = new ArrayList<>(container.listBlobs().keySet());
            names.removeIf(name -> RECORD_NAME.matcher(name).matches() == false);
            if (limit != null) {
                names.removeIf(name -> name.compareTo(limit) > 0);
            }
            names.sort(Comparator.naturalOrder());
            for (String name : names) {
                try (InputStream in = container.readBlob(name)) {
                    records.addAll(parseBatch(in.readAllBytes()));
                } catch (IOException e) {
                    // A name that matches ours and does not parse is corruption, and it propagates --
                    // naming the blob, because a parse failure that does not say which record failed
                    // leaves an operator with a corrupt log and nowhere to look. Anything not matching
                    // was filtered above: a foreign object sharing the prefix is not ours to interpret,
                    // and must not brick a shard's recovery. Absent, foreign and corrupt are three
                    // different answers.
                    throw new IOException("unreadable WAL record at " + container.path().buildAsString() + name, e);
                }
            }
        }
        return records;
    }

    /**
     * Seals the log against a predecessor that has not yet stopped writing, and returns the cutoff to
     * replay under.
     *
     * <p><b>Why the snapshot has to be durable.</b> A cutoff held only in memory protects the one recovery
     * that took it. If that node then dies before publishing — which is exactly the case where the log
     * still matters — the next successor takes its own snapshot, by which time the zombie's records have
     * been sitting in the log looking like ordinary history for as long as it took. Writing the snapshot
     * down makes it survive the successor that took it, which is the whole point: the first node to take
     * over records where legitimate history ended, and every node after it inherits that answer.
     *
     * <p>Seals are merged by taking the <em>lowest</em> recorded position for each term, because the
     * earliest seal was taken closest to the moment ownership actually moved and is therefore the tightest
     * true bound. Re-sealing writes the merged answer, so one blob carries it and the older ones are then
     * dropped; a crash between the write and the drop leaves both, and merging them gives the same answer.
     *
     * <p>Seals live in their own directory so that {@link #replayable(Map)} — which reads term directories
     * — cannot mistake one for a term, and {@code dropOlderTerms} cannot delete one while pruning terms.
     * {@link #deleteAll} does remove them, which is right: that is the log being destroyed, not truncated.
     *
     * @param term the term this node is taking the shard over at
     * @return the cutoff to replay under
     * @throws IOException if reading or writing the seals fails
     */
    public Map<Long, String> sealAt(long term) throws IOException {
        final Map<Long, String> here = position();
        final Map<Long, String> sealed = sealedPosition();
        final long sealedBelow = highestSealedTerm();
        final Map<Long, String> cutoff = new java.util.HashMap<>();
        for (Map.Entry<Long, String> entry : here.entrySet()) {
            if (entry.getKey() >= sealedBelow) {
                // No seal speaks for this term: it is the last sealer's own term, or later. Its records
                // are the ones that sealer legitimately wrote after taking over, and they must replay.
                cutoff.put(entry.getKey(), entry.getValue());
                continue;
            }
            // A seal does speak for this term, and it is authoritative even when it says nothing. A term
            // that a seal taken above it does not mention had no records when that seal was written, so
            // anything under it now appeared afterwards -- which is the steady state of this system, not
            // an edge case: a publish truncates the log, so the usual state at takeover is an empty one,
            // and falling back to the successor's own listing here would hand a zombie's later appends
            // straight back. The empty string excludes every name.
            final String earlier = sealed.getOrDefault(entry.getKey(), "");
            cutoff.put(entry.getKey(), earlier.compareTo(entry.getValue()) > 0 ? entry.getValue() : earlier);
        }

        final StringBuilder body = new StringBuilder();
        for (Map.Entry<Long, String> entry : new TreeMap<>(cutoff).entrySet()) {
            body.append(entry.getKey()).append(' ').append(entry.getValue()).append('\n');
        }
        final byte[] bytes = body.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        final String name = String.format(java.util.Locale.ROOT, "seal-%020d", term);
        final BlobContainer seals = blobStore.blobContainer(shardBase.add("wal").add(SEALS));
        seals.writeBlob(name, new ByteArrayInputStream(bytes), bytes.length, false);

        final List<String> superseded = new ArrayList<>(seals.listBlobs().keySet());
        superseded.removeIf(other -> SEAL_NAME.matcher(other).matches() == false || other.compareTo(name) >= 0);
        if (superseded.isEmpty() == false) {
            seals.deleteBlobsIgnoringIfNotExists(superseded);
        }
        return cutoff;
    }

    /**
     * Reads the cutoff recorded by every node that has taken this shard over, merged.
     *
     * @return the lowest recorded position for each sealed term, empty if the log has never been sealed
     * @throws IOException if reading fails
     */
    public Map<Long, String> sealedPosition() throws IOException {
        final BlobContainer seals = blobStore.blobContainer(shardBase.add("wal").add(SEALS));
        final Map<Long, String> merged = new java.util.HashMap<>();
        for (Map.Entry<String, org.opensearch.common.blobstore.BlobMetadata> blob : seals.listBlobs().entrySet()) {
            if (SEAL_NAME.matcher(blob.getKey()).matches() == false) {
                continue;
            }
            final byte[] bytes;
            try (InputStream in = seals.readBlob(blob.getKey())) {
                bytes = in.readAllBytes();
            }
            for (String line : new String(bytes, java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
                if (line.isEmpty()) {
                    continue;
                }
                final int space = line.indexOf(' ');
                if (space < 0) {
                    throw new IOException("unreadable WAL seal at " + seals.path().buildAsString() + blob.getKey());
                }
                final Long term = parseTermNumber(line.substring(0, space));
                if (term == null) {
                    throw new IOException("unreadable WAL seal at " + seals.path().buildAsString() + blob.getKey());
                }
                final String position = line.substring(space + 1);
                merged.merge(term, position, (a, b) -> a.compareTo(b) <= 0 ? a : b);
            }
        }
        return merged;
    }

    /**
     * The highest term any seal was written at. Every term below it has been sealed, whether or not that
     * seal mentions it — a seal that does not mention a term is saying the term was empty, which is a
     * stronger statement than saying nothing.
     *
     * @return the highest sealing term, or {@link Long#MIN_VALUE} if the log has never been sealed
     * @throws IOException if listing fails
     */
    private long highestSealedTerm() throws IOException {
        final BlobContainer seals = blobStore.blobContainer(shardBase.add("wal").add(SEALS));
        long highest = Long.MIN_VALUE;
        for (String name : seals.listBlobs().keySet()) {
            if (SEAL_NAME.matcher(name).matches() == false) {
                continue;
            }
            final Long term = parseTermNumber(name.substring("seal-".length()));
            if (term != null && term > highest) {
                highest = term;
            }
        }
        return highest;
    }

    private static Long parseTermNumber(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** The log's term directories, lowest term first, which is replay order. */
    private Map<Long, BlobContainer> termsInOrder() throws IOException {
        final BlobContainer walRoot = blobStore.blobContainer(shardBase.add("wal"));
        final Map<Long, BlobContainer> byTerm = new TreeMap<>();
        for (Map.Entry<String, BlobContainer> child : walRoot.children().entrySet()) {
            final Long term = parseTerm(child.getKey());
            if (term != null) {
                byTerm.put(term, child.getValue());
            }
        }
        return byTerm;
    }

    /**
     * Parses one blob, which holds a single record or a batch of them, one per line.
     *
     * <p>Splitting on the newline byte is safe on UTF-8 without decoding first: {@code 0x0A} cannot
     * appear inside a multi-byte sequence, so a byte-level split cannot land mid-character.
     *
     * @param blob the blob's bytes
     * @return the records it holds, in order
     * @throws IOException if a line does not parse
     */
    private static List<WalRecord> parseBatch(byte[] blob) throws IOException {
        final List<WalRecord> records = new ArrayList<>();
        int start = 0;
        for (int i = 0; i <= blob.length; i++) {
            if (i != blob.length && blob[i] != '\n') {
                continue;
            }
            if (i > start) {
                records.add(WalRecord.fromStream(new java.io.ByteArrayInputStream(blob, start, i - start)));
            }
            start = i + 1;
        }
        return records;
    }

    /**
     * Records that a commit has been published, and drops what the previous publish had seen.
     *
     * <p>Deleting the <em>previous</em> snapshot rather than the current one is the whole safety
     * argument; see this class's documentation.
     *
     * @param term the publishing writer's term
     * @return the number of records dropped
     * @throws IOException if listing or deleting fails
     */
    public int onPublished(long term) throws IOException {
        final BlobContainer container = containerFor(term);
        final List<String> current = new ArrayList<>(container.listBlobs().keySet());
        current.removeIf(name -> RECORD_NAME.matcher(name).matches() == false);

        final List<String> toDelete = new ArrayList<>(previousSnapshot);
        toDelete.retainAll(current);
        if (toDelete.isEmpty() == false) {
            container.deleteBlobsIgnoringIfNotExists(toDelete);
        }
        previousSnapshot = current;
        return toDelete.size() + dropOlderTerms(term);
    }

    /**
     * Drops every record belonging to a term older than the one that just published.
     *
     * <p>Without this the log only ever grows. The conservative rule above trims the publishing writer's
     * own term and nothing else, so a dead predecessor's records are trimmed by nobody and replayed by
     * every successor for the life of the shard.
     *
     * <p><b>Why a full publish at a higher term makes them safe to drop</b>, where the same-term rule has
     * to wait a cycle: a successor replays every older record <em>before</em> it starts, so by the time it
     * publishes at term T those records are in the engine and therefore in the commit. There is no
     * appended-but-not-yet-applied window for another writer's term, because this writer applied all of
     * them at once and then flushed.
     *
     * <p><b>And the one case that is not covered is covered anyway.</b> A zombie still appending under its
     * old term after the successor replayed has records that are <em>not</em> in the commit — and deleting
     * them loses nothing, because they were already destined to lose. Replay is ordered by term and then
     * ordinal, so a record under {@code t=1} is always applied before one under {@code t=2}: the zombie's
     * late write is overwritten by the successor's for any document they share, and for any document they
     * do not, the zombie wrote to a shard it no longer owned. That is what fencing means.
     *
     * @param term the term that just published
     * @return how many records were dropped
     * @throws IOException if listing or deleting fails
     */
    private int dropOlderTerms(long term) throws IOException {
        final BlobContainer walRoot = blobStore.blobContainer(shardBase.add("wal"));
        int dropped = 0;
        for (Map.Entry<String, BlobContainer> child : walRoot.children().entrySet()) {
            final Long childTerm = parseTerm(child.getKey());
            if (childTerm == null || childTerm >= term) {
                continue;
            }
            final List<String> ours = new ArrayList<>(child.getValue().listBlobs().keySet());
            ours.removeIf(name -> RECORD_NAME.matcher(name).matches() == false);
            if (ours.isEmpty() == false) {
                child.getValue().deleteBlobsIgnoringIfNotExists(ours);
                dropped += ours.size();
            }
        }
        return dropped;
    }

    /**
     * Removes every record for a shard, for use when the index itself is deleted.
     *
     * @throws IOException if listing or deleting fails
     */
    public void deleteAll() throws IOException {
        final BlobContainer walRoot = blobStore.blobContainer(shardBase.add("wal"));
        for (BlobContainer child : walRoot.children().values()) {
            final List<String> ours = new ArrayList<>(child.listBlobs().keySet());
            // Seals go too: this is the log being destroyed, not truncated, so the cutoffs recorded
            // against it have nothing left to bound.
            ours.removeIf(name -> RECORD_NAME.matcher(name).matches() == false && SEAL_NAME.matcher(name).matches() == false);
            child.deleteBlobsIgnoringIfNotExists(ours);
        }
    }

    private static Long parseTerm(String containerName) {
        if (containerName.startsWith("t=") == false) {
            return null;
        }
        try {
            return Long.parseLong(containerName.substring(2));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
