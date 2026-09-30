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
 * <p><b>One blob per append, and an append may be a batch.</b> A bulk request landing on one shard costs
 * one PUT for the whole batch rather than one per document. See {@link #append(long, List)} for the
 * container format and why an older writer's log still reads.
 *
 * <p><b>A single write no longer costs a PUT of its own either.</b> This paragraph used to say it did,
 * and {@link WalGroupCommitter} is what changed it: concurrent writes to one shard share the PUT that is
 * already in flight for it, so what a document costs is set by the store's write latency rather than by
 * the write rate. Nothing about the format or the ordering here changes -- a group is one append of a
 * list, which is the call this class already had.
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
    /** Set by any failed or ambiguous append: this instance never appends to that term again. See {@link #append}. */
    private volatile boolean poisoned;
    /** The fences this instance knows of in its own term, which truncation must never delete. */
    private final java.util.Set<String> ownFences = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** How many times a fence is retried against a predecessor still winning the next slot. */
    private static final int FENCE_ATTEMPTS = 1000;
    private volatile List<String> previousSnapshot = List.of();
    /** The ordinal the last publish had seen, so the next publish's snapshot is the records after it. */
    private volatile long publishedUpTo = 0L;
    /** The term whose older-term containers this instance has already emptied. */
    private volatile long olderTermsDroppedFor = -1L;
    /** When that emptying last ran, so it can run again for what appeared afterwards. */
    private volatile long olderTermsDroppedAtNanos = Long.MIN_VALUE;
    private final long olderTermRescanMillis;

    /**
     * How long a publish may go before it looks at the older term containers again.
     *
     * <p><b>Why looking again is needed at all.</b> Emptying them once per term was very nearly right: a
     * successor replays every older record before it starts, so after one pass there is nothing left to
     * find. What it missed is the writer that has lost the shard and not yet noticed. It goes on appending
     * under its own, now-older term for up to a lease, and every one of those records lands <em>after</em>
     * the one drop that would have collected it. Nothing else reclaims them -- the collector walks
     * {@code t=N} directories under the shard container and steps over {@code wal} -- so for a shard that
     * never fails over again they stayed for the life of the index. Not a correctness fault: they are
     * behind the successor's seal and never replay. Just bytes nobody was ever going to read, billed
     * monthly.
     *
     * <p>Ten minutes, and the interval is doing two jobs. A zombie stops within a lease of losing the
     * shard, so nothing accumulates for longer than that however rarely this runs; the interval decides
     * how long the bytes sit, not how many there are. Against that, a re-scan is a listing, and a listing
     * is billed at the write tier -- so running it per publish, or per lease, would have cost far more
     * than the storage it reclaims. Ten minutes is two orders of magnitude below "forever" and cheap
     * enough to disappear into the noise: on an object store an emptied prefix lists nothing, so the usual
     * re-scan is one request that finds one live term and stops.
     */
    public static final long DEFAULT_OLDER_TERM_RESCAN_MILLIS = 600_000L;

    /**
     * Creates a WAL for one shard.
     *
     * @param blobStore the backing store
     * @param shardBase the shard's base path
     */
    public WalStore(BlobStore blobStore, BlobPath shardBase) {
        this(blobStore, shardBase, DEFAULT_OLDER_TERM_RESCAN_MILLIS);
    }

    /**
     * Creates a WAL for one shard with an explicit re-scan interval.
     *
     * <p>Zero re-scans on every publish and {@link Long#MAX_VALUE} never re-scans, which is the behaviour
     * this had before the interval existed. For tests that want one end or the other without waiting.
     *
     * @param blobStore the backing store
     * @param shardBase the shard's base path
     * @param olderTermRescanMillis see {@link #DEFAULT_OLDER_TERM_RESCAN_MILLIS}
     */
    public WalStore(BlobStore blobStore, BlobPath shardBase, long olderTermRescanMillis) {
        this.blobStore = blobStore;
        this.shardBase = shardBase;
        this.olderTermRescanMillis = olderTermRescanMillis;
    }

    /** Where a replay's record reads run concurrently; null reads them one after another. */
    private volatile java.util.concurrent.Executor reads;

    /** How many record reads a replay keeps in flight. */
    static final int READ_WIDTH = 8;

    /**
     * Lets a replay read its records concurrently on the given executor, the calling thread reading too.
     *
     * <p>A replay read one record blob at a time, so a log of a hundred records was a hundred round trips
     * in a row on the path a cold shard's first write waits on.
     *
     * @param executor where the reads run
     * @return this, for chaining
     */
    public WalStore readingWith(java.util.concurrent.Executor executor) {
        this.reads = executor;
        return this;
    }

    /** Every named blob's bytes, in order. The caller takes reads too, so a saturated executor only costs width. */
    private List<byte[]> readAll(BlobContainer container, List<String> names) throws IOException {
        final byte[][] bodies = new byte[names.size()][];
        final java.util.concurrent.Executor executor = reads;
        if (executor == null || names.size() < 2) {
            for (int i = 0; i < names.size(); i++) {
                bodies[i] = readOne(container, names.get(i));
            }
            return java.util.Arrays.asList(bodies);
        }
        final java.util.concurrent.atomic.AtomicInteger next = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicReference<IOException> failed = new java.util.concurrent.atomic.AtomicReference<>();
        final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(names.size());
        final Runnable drain = () -> {
            for (int i = next.getAndIncrement(); i < names.size(); i = next.getAndIncrement()) {
                try {
                    if (failed.get() == null) {
                        bodies[i] = readOne(container, names.get(i));
                    }
                } catch (IOException e) {
                    failed.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            }
        };
        for (int helper = 0; helper < Math.min(READ_WIDTH, names.size()) - 1; helper++) {
            try {
                executor.execute(drain);
            } catch (RuntimeException rejected) {
                break;
            }
        }
        drain.run();
        try {
            if (done.await(5, java.util.concurrent.TimeUnit.MINUTES) == false) {
                throw new IOException("reading the log at " + container.path().buildAsString() + " did not finish");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted reading the log", e);
        }
        if (failed.get() != null) {
            throw failed.get();
        }
        return java.util.Arrays.asList(bodies);
    }

    private static byte[] readOne(BlobContainer container, String name) throws IOException {
        try (InputStream in = container.readBlob(name)) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new IOException("unreadable WAL record at " + container.path().buildAsString() + name, e);
        }
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
        if (poisoned) {
            throw new IOException("this log writer stopped appending to term " + term + " after an earlier append failed or it was fenced");
        }
        seed(term);
        final String name = String.format(java.util.Locale.ROOT, "%020d", ordinal.incrementAndGet());
        // Refused on collision, never overwritten. A store instance is rebuilt on every local release of a
        // shard, and a shard reopened at the same term replayed records 1..N and then appended its next
        // write as record 1 -- over the record it had just replayed. With the ordinal seeded from what
        // the container holds this does not happen, and if it somehow did, failing the write (which
        // releases the shard, see ServerlessNode) is the honest outcome rather than a silent overwrite.
        //
        // <b>And any failure ends this writer's use of the term.</b> A refused write means another writer --
        // a successor's fence, or a newer instance -- holds this slot, and an ambiguous one may or may not
        // have landed; either way writing the next ordinal would step past a fence and land a record behind
        // it. Stopping keeps this writer's successful names contiguous, which is what lets a successor find
        // the one slot to fence. See the class documentation's fencing section.
        try {
            containerFor(term).writeBlob(name, new ByteArrayInputStream(bytes), bytes.length, true);
        } catch (IOException | RuntimeException e) {
            poisoned = true;
            throw e;
        }
    }

    /**
     * Begins writing a term, once per term: fences it, and starts the ordinal after the fence.
     *
     * <p>A writer does not take its position from a listing and carry on from there. Another instance may be
     * writing the same term -- the one a same-term reopen replaced, with a PUT still in flight -- and one
     * that seeded from a listing would write straight past it, and past any fence placed for it. So a writer
     * first occupies the next slot itself with a fence, and writes only after that: whatever held the term
     * before is either ahead of the fence, where replay finds it, or refused by the store at the fence.
     */
    private synchronized void seed(long term) throws IOException {
        if (seededTerm == term) {
            return;
        }
        establish(term, null);
    }

    /**
     * Begins writing a term: fences it against every earlier writer of it, and then, if asked, confirms this
     * node still owns the term.
     *
     * <p>The confirmation is what covers a writer that won a head and was superseded before it had written
     * anything: a successor fences only the terms it finds in the log, and this one had none there yet. It
     * places its own fence, then reads the head; a successor that took the shard before that read is seen by
     * it, and this writer stops. One that takes it after finds this term in the log and fences it.
     *
     * @param term the term to write under
     * @param stillOwner asked after the fence is placed; false stops this writer. Null skips the question.
     * @throws IOException if the fence cannot be placed, or this node no longer owns the term
     */
    public synchronized void establish(long term, java.util.function.BooleanSupplier stillOwner) throws IOException {
        final long fence = fence(term, true);
        ownFences.add(name(fence));
        ordinal.set(fence);
        seededTerm = term;
        poisoned = false;
        // What existed before the fence was replayed before this writer started, so it is in the first
        // commit this instance publishes and is dropped by the publish after that.
        publishedUpTo = 0L;
        previousSnapshot = List.of();
        if (stillOwner != null && stillOwner.getAsBoolean() == false) {
            poisoned = true;
            throw new IOException("term " + term + " has been taken over; this node will not write under it");
        }
    }

    /**
     * Fences a term: occupies the slot after its highest blob, so nothing can be written after it.
     *
     * <p>Put-if-absent decides between the fence and a writer's append aimed at the same slot. If the append
     * wins, it is before the fence and the next attempt sees it; a writer's names are contiguous (see
     * {@link #append}), so its next slot is the one fenced now. A fence is a blob of no bytes, which no record
     * is, so a listing tells one apart by its length.
     *
     * @return the fenced ordinal
     */
    private long fence(long term, boolean own) throws IOException {
        return fence(term, own, null);
    }

    /**
     * Fences a term, starting from a listing the caller already holds, if it holds one: the first attempt is
     * then a single put-if-absent, and only a lost race lists again.
     */
    private long fence(long term, boolean own, Map<String, org.opensearch.common.blobstore.BlobMetadata> listed) throws IOException {
        final BlobContainer container = containerFor(term);
        for (int attempt = 0; attempt < FENCE_ATTEMPTS; attempt++) {
            long highest = 0L;
            final Map<String, org.opensearch.common.blobstore.BlobMetadata> blobs = attempt == 0 && listed != null
                ? listed
                : container.listBlobs();
            for (Map.Entry<String, org.opensearch.common.blobstore.BlobMetadata> blob : blobs.entrySet()) {
                if (RECORD_NAME.matcher(blob.getKey()).matches()) {
                    highest = Math.max(highest, Long.parseLong(blob.getKey()));
                    if (own && blob.getValue().length() == 0L) {
                        ownFences.add(blob.getKey());
                    }
                }
            }
            final long slot = highest + 1;
            try {
                container.writeBlob(name(slot), new ByteArrayInputStream(new byte[0]), 0L, true);
                return slot;
            } catch (java.nio.file.FileAlreadyExistsException e) {
                // A writer took the slot first: its record is before the fence. Look again.
            }
        }
        throw new IOException("could not fence term " + term + ": a writer kept taking the next slot");
    }

    /**
     * Fences every older term whose last blob is not already a fence, at a takeover.
     *
     * <p>This is the seal, made a fence: rather than recording how far the predecessor's log reached, which
     * a listing can only say about the past, it stops the log reaching further. Every older term, not just
     * the predecessor's, because an activation that won a head and failed before fencing leaves the term
     * before it open, and the next takeover has to close it.
     *
     * @param term the term this node has just taken the shard over at
     * @throws IOException if a term cannot be listed or fenced
     */
    public void fenceOlderTerms(long term) throws IOException {
        for (Map.Entry<Long, BlobContainer> older : termsInOrder().entrySet()) {
            if (older.getKey() >= term) {
                continue;
            }
            String last = null;
            long lastLength = -1L;
            final Map<String, org.opensearch.common.blobstore.BlobMetadata> listed = older.getValue().listBlobs();
            for (Map.Entry<String, org.opensearch.common.blobstore.BlobMetadata> blob : listed.entrySet()) {
                if (RECORD_NAME.matcher(blob.getKey()).matches() && (last == null || blob.getKey().compareTo(last) > 0)) {
                    last = blob.getKey();
                    lastLength = blob.getValue().length();
                }
            }
            if (last != null && lastLength == 0L) {
                continue;
            }
            fence(older.getKey(), false, listed);
        }
    }

    /**
     * Reads every record the log holds, for a writer that has fenced every term it replays.
     *
     * <p>Fenced, so nothing is cut off by position: whatever landed is before a fence and belongs in the
     * history. Only the bounds recorded by seals from before fences existed are still applied, to the terms
     * they spoke for.
     *
     * @return the records to replay, ordered by term and then by ordinal
     * @throws IOException if listing or reading fails
     */
    public List<WalRecord> replayableAfterFencing() throws IOException {
        // One listing of the log's root says which terms exist and whether any legacy seal does: the seals
        // directory is listed only when it is there, which on a log begun after fences is never.
        final Map<String, BlobContainer> children = blobStore.blobContainer(shardBase.add("wal")).children();
        if (children.containsKey(SEALS) == false) {
            return replayable(null, termsIn(children));
        }
        final BlobContainer sealsContainer = blobStore.blobContainer(shardBase.add("wal").add(SEALS));
        final List<String> sealNames = sealNamesIn(sealsContainer.listBlobs().keySet());
        if (sealNames.isEmpty()) {
            return replayable(null);
        }
        final Map<Long, String> sealed = mergeSeals(sealsContainer, sealNames);
        final long sealedBelow = highestSealedTerm(sealNames);
        final Map<Long, String> cutoff = new java.util.HashMap<>();
        for (Long term : termsInOrder().keySet()) {
            // Above the last legacy seal, unbounded; at or below it, what that seal recorded.
            cutoff.put(term, term >= sealedBelow ? null : sealed.getOrDefault(term, ""));
        }
        return replayable(cutoff);
    }

    private static String name(long ordinal) {
        return String.format(java.util.Locale.ROOT, "%020d", ordinal);
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
        return replayable(cutoff, termsInOrder());
    }

    private List<WalRecord> replayable(Map<Long, String> cutoff, Map<Long, BlobContainer> terms) throws IOException {
        final List<WalRecord> records = new ArrayList<>();
        for (Map.Entry<Long, BlobContainer> entry : terms.entrySet()) {
            final BlobContainer container = entry.getValue();
            if (cutoff != null && cutoff.containsKey(entry.getKey()) == false) {
                continue;
            }
            final String limit = cutoff == null ? null : cutoff.get(entry.getKey());
            final Map<String, org.opensearch.common.blobstore.BlobMetadata> listed = container.listBlobs();
            final List<String> names = new ArrayList<>(listed.keySet());
            // Fences hold no records: a blob of no bytes is one, and is not read.
            names.removeIf(name -> RECORD_NAME.matcher(name).matches() == false || listed.get(name).length() == 0L);
            if (limit != null) {
                names.removeIf(name -> name.compareTo(limit) > 0);
            }
            names.sort(Comparator.naturalOrder());
            final List<byte[]> bodies = readAll(container, names);
            for (int i = 0; i < names.size(); i++) {
                final String name = names.get(i);
                try {
                    records.addAll(parseBatch(bodies.get(i)));
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
        final BlobContainer sealsContainer = blobStore.blobContainer(shardBase.add("wal").add(SEALS));
        // One listing doing the three jobs that used to cost three: which seals to merge, the highest term
        // any of them was written at, and which of them this one supersedes.
        //
        // <b>Taking the supersede set from this listing rather than from a fresh one after the write is a
        // correctness fix, not only a cheaper way to get the same answer.</b> The delete used to be driven
        // by a listing taken *after* the seal below was written, which could therefore see a seal that a
        // concurrent lower-term sealer landed after the merge above had already read the container -- and
        // delete it, unmerged. Seals merge by taking the lowest recorded position per term, so the seal
        // dropped that way was the tighter bound, and losing it widens the cutoff: a zombie's records
        // between the two bounds replay as though they had been acknowledged. Bounding the delete by what
        // was read is what makes "everything this seal supersedes" mean "everything this seal has already
        // accounted for". A concurrent seal is now neither merged nor deleted; it stays, and the next
        // sealer merges it.
        final List<String> sealNames = sealNamesIn(sealsContainer.listBlobs().keySet());
        final Map<Long, String> sealed = mergeSeals(sealsContainer, sealNames);
        final long sealedBelow = highestSealedTerm(sealNames);
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

        // A seal speaks only for terms strictly below the sealing term. The sealer's own term can appear
        // in `here` -- a shard released locally and reopened at the same term through ensureOpen seals
        // again at that term, and its container already holds records -- and writing that entry down was
        // a latent loss: the records the reopened writer went on to append after this seal, all of them
        // acknowledged, would be bounded by *this* listing if a later sealer's seal-write succeeded but
        // its delete of this one did not, because seals are merged by taking the minimum per term. The
        // sealer's own term is bounded only by a later sealer's listing, which is the one true bound; it
        // still replays in full here, through the cutoff returned below.
        final StringBuilder body = new StringBuilder();
        for (Map.Entry<Long, String> entry : new TreeMap<>(cutoff).entrySet()) {
            if (entry.getKey() >= term) {
                continue;
            }
            body.append(entry.getKey()).append(' ').append(entry.getValue()).append('\n');
        }
        final byte[] bytes = body.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        final String name = String.format(java.util.Locale.ROOT, "seal-%020d", term);
        sealsContainer.writeBlob(name, new ByteArrayInputStream(bytes), bytes.length, false);

        // Only what was read above, so nothing that arrived since is dropped unmerged. One delete call for
        // all of them: the container batches, and a seal per takeover meant a request per takeover.
        final List<String> superseded = new ArrayList<>(sealNames);
        superseded.removeIf(other -> other.compareTo(name) >= 0);
        if (superseded.isEmpty() == false) {
            sealsContainer.deleteBlobsIgnoringIfNotExists(superseded);
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
        return mergeSeals(seals, sealNamesIn(seals.listBlobs().keySet()));
    }

    /** The seal blobs among a listing's names, in the order the listing gave them. */
    private static List<String> sealNamesIn(java.util.Collection<String> names) {
        final List<String> seals = new ArrayList<>();
        for (String name : names) {
            if (SEAL_NAME.matcher(name).matches()) {
                seals.add(name);
            }
        }
        return seals;
    }

    /** Merges the named seals, taking the lowest recorded position per term. Caller supplies the listing. */
    private Map<Long, String> mergeSeals(BlobContainer seals, List<String> sealNames) throws IOException {
        final Map<Long, String> merged = new java.util.HashMap<>();
        for (String sealName : sealNames) {
            final byte[] bytes;
            try (InputStream in = seals.readBlob(sealName)) {
                bytes = in.readAllBytes();
            }
            for (String line : new String(bytes, java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
                if (line.isEmpty()) {
                    continue;
                }
                final int space = line.indexOf(' ');
                if (space < 0) {
                    throw new IOException("unreadable WAL seal at " + seals.path().buildAsString() + sealName);
                }
                final Long term = parseTermNumber(line.substring(0, space));
                if (term == null) {
                    throw new IOException("unreadable WAL seal at " + seals.path().buildAsString() + sealName);
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
     * <p>A pure function of the listing the caller already has, so asking this costs nothing on top of
     * the merge it is asked alongside.
     *
     * @param sealNames the seal blob names, from one listing
     * @return the highest sealing term, or {@link Long#MIN_VALUE} if the log has never been sealed
     */
    private static long highestSealedTerm(List<String> sealNames) {
        long highest = Long.MIN_VALUE;
        for (String name : sealNames) {
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
        return termsIn(blobStore.blobContainer(shardBase.add("wal")).children());
    }

    private static Map<Long, BlobContainer> termsIn(Map<String, BlobContainer> children) {
        final Map<Long, BlobContainer> byTerm = new TreeMap<>();
        for (Map.Entry<String, BlobContainer> child : children.entrySet()) {
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
        // No listing. Within a term there is exactly one writer and this instance seeded its ordinal from
        // the container, so the records that exist are exactly the ordinals up to the counter -- less
        // whatever an earlier publish already dropped, which is what publishedUpTo remembers. A listing
        // per publish used to be the largest fixed cost of a busy shard's pass.
        seed(term);
        final BlobContainer container = containerFor(term);
        final long upTo = ordinal.get();
        // The highest blob of the term stays, whatever else goes: it is how a successor finds the slot to
        // fence, and a term emptied by truncation would hide how far its writer had got. It is replayed again
        // at the next open and skipped there by sequence number, which truncation lagging a publish already
        // relies on. It goes once something newer is written.
        final List<String> toDelete = new ArrayList<>(previousSnapshot);
        final boolean keptHighest = toDelete.remove(name(upTo));
        toDelete.removeAll(ownFences);
        if (toDelete.isEmpty() == false) {
            container.deleteBlobsIgnoringIfNotExists(toDelete);
        }
        final List<String> current = new ArrayList<>();
        if (keptHighest) {
            current.add(name(upTo));
        }
        for (long each = publishedUpTo + 1; each <= upTo; each++) {
            final String each20 = name(each);
            // Never a fence: a deleted fence frees the slot a stopped writer's next PUT is aimed at.
            if (ownFences.contains(each20) == false) {
                current.add(each20);
            }
        }
        previousSnapshot = current;
        publishedUpTo = upTo;
        int dropped = toDelete.size();
        // Once when the term changes, and then on a slow timer -- because a zombie is exactly the thing
        // that adds to an older term after the first drop, and "its records lose on replay anyway" answers
        // whether they are dangerous, not whether anything ever removes them. See
        // DEFAULT_OLDER_TERM_RESCAN_MILLIS.
        if (olderTermsDroppedFor != term || rescanOlderTermsDue()) {
            dropped += dropOlderTerms(term);
            olderTermsDroppedFor = term;
            olderTermsDroppedAtNanos = System.nanoTime();
        }
        return dropped;
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
    /**
     * Whether enough time has passed to look at the older terms again.
     *
     * <p>On {@link System#nanoTime} rather than the plane's clock, deliberately: this paces a reclamation
     * and decides nothing about ownership or expiry. A monotonic source is the right one for an interval
     * that must not be moved by a clock adjustment, and nothing here needs to agree with another node
     * about when it ran.
     */
    private boolean rescanOlderTermsDue() {
        if (olderTermRescanMillis == Long.MAX_VALUE) {
            return false;
        }
        final long last = olderTermsDroppedAtNanos;
        return last == Long.MIN_VALUE
            || System.nanoTime() - last >= java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(olderTermRescanMillis);
    }

    private int dropOlderTerms(long term) throws IOException {
        final BlobContainer walRoot = blobStore.blobContainer(shardBase.add("wal"));
        int dropped = 0;
        for (Map.Entry<String, BlobContainer> child : walRoot.children().entrySet()) {
            final Long childTerm = parseTerm(child.getKey());
            if (childTerm == null || childTerm >= term) {
                continue;
            }
            final Map<String, org.opensearch.common.blobstore.BlobMetadata> listed = child.getValue().listBlobs();
            final List<String> ours = new ArrayList<>(listed.keySet());
            // Fences stay, and so does the term's highest blob -- which after a takeover is its fence, so the
            // records below it all go; a term not yet fenced keeps its last record, which is how its fence
            // will find the slot.
            ours.removeIf(name -> RECORD_NAME.matcher(name).matches() == false);
            final String highest = ours.stream().max(Comparator.naturalOrder()).orElse(null);
            ours.removeIf(name -> listed.get(name).length() == 0L || name.equals(highest));
            if (ours.isEmpty() == false) {
                child.getValue().deleteBlobsIgnoringIfNotExists(ours);
                dropped += ours.size();
            }
            // The emptied container stays. Deleting it would stop every later takeover listing one
            // container per historical term, but a term's directory is also how the log reports that a
            // term existed and holds nothing -- position() and the tests that read the log off disk key
            // on it -- and a store that has no real directories (S3) lists nothing for an empty prefix
            // anyway. The per-term listing cost is bounded by the number of takeovers, not by writes.
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
