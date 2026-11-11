/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.serverless.store.PruningDigest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Every index's pruning digests, gathered into a few registers per name prefix, so a search over thousands of
 * indices can rule most of them out without reading anything per index.
 *
 * <p><b>Layout.</b> Indices are grouped by the first {@value #GROUP_CHARS} characters of their name -- every
 * index {@code logs-*} could match shares a group when the prefix is that long -- and hashed into {@value
 * #BUCKETS} buckets within it, one register each. A bucket holds, per index, the uuid it was written for, the
 * shard count, and per shard the digest of what that shard has published and the term of any writer holding it.
 * A search over a prefix of at least {@value #GROUP_CHARS} characters reads the group's {@value #BUCKETS}
 * registers, whatever the number of indices, and reads nothing further for an index they rule out.
 *
 * <p><b>The invariant that makes it safe: every searchable document of an index is covered by its entry before
 * it becomes searchable.</b> A reader serves a shard's published commit, and a writer serves what it has
 * refreshed. So:
 * <ul>
 *   <li>A publish widens the shard's digest here <em>before</em> it writes its manifest, unless a writer's mark
 *   already keeps the shard from being ruled out. A publish that fails in between leaves the entry wider than the
 *   commit, never narrower.</li>
 *   <li>Clearing a writer's mark widens the digest to the published commit's in the same swap, so what was published
 *   under the mark is covered from the moment the shard can be ruled out.</li>
 *   <li>A writer records its term here <em>before</em> it opens the shard, and a shard with a writer is never
 *   ruled out. Letting go clears the term only if it is still that writer's, so a predecessor's late clear
 *   cannot unmark its successor.</li>
 *   <li>An entry written for another uuid -- a deleted incarnation of the name -- is replaced, not merged, by
 *   the first publish or writer of the new one. Until then the new incarnation has nothing searchable, so the
 *   old entry ruling it out rules out nothing.</li>
 *   <li>A shard with no digest yet is never ruled out: an entry is created by one shard's event, and the others
 *   may hold data published before entries existed.</li>
 * </ul>
 * Digests only ever widen within an incarnation -- a merge takes the union of ranges over the fields both sides
 * digest -- and are coarsened outward (dates to the hour) so a shard appending in time order widens its entry
 * about once an hour rather than on every publish. An entry wider than its shard costs a search a per-index
 * check; one narrower would lose documents, and nothing here can make one.
 *
 * <p><b>The same invariant makes the rollups a group's name list.</b> Every way an incarnation of an index comes
 * to hold a document writes an entry for that incarnation first: a writer's mark before the shard opens, and a
 * restore's empty digest before its manifest lands. So an index with no entry has nothing to find, and a prefix
 * pattern is resolved from the group's entries rather than by listing every name under the prefix -- which a store
 * holding a million names cannot do within a request. Aliases are entered too, before the alias record is written,
 * and are never ruled out. An entry left by a deleted index or alias costs a search one descriptor read that comes
 * back absent; a name missing from the rollups would be a wrong answer, and nothing that holds data can be missing.
 *
 * <p><b>What it does not bound.</b> A group's registers grow with the indices ever written in it: at 100,000 such
 * indices a bucket holds about 1,500 entries. Entries of deleted indices stay until the name is reused.
 */
public final class DigestRollups {

    /** How many leading characters of an index name form its group. */
    public static final int GROUP_CHARS = 5;

    /** How many registers each group is spread over. */
    public static final int BUCKETS = 64;

    private static final int CAS_ATTEMPTS = 64;

    private final BlobStore blobStore;
    private final BlobPath base;
    /** The last value this node read or wrote per register, so a publish that widens nothing costs nothing. */
    private final Map<String, Map<String, RollupEntry>> known = new ConcurrentHashMap<>();

    /**
     * Creates the rollup store.
     *
     * @param blobStore the store
     * @param base where the rollups live
     */
    public DigestRollups(BlobStore blobStore, BlobPath base) {
        this.blobStore = blobStore;
        this.base = base;
    }

    /**
     * The group a name belongs to.
     *
     * @param name an index name or a prefix
     * @return its first {@value #GROUP_CHARS} characters, or the whole of it if shorter
     */
    public static String group(String name) {
        return name.length() <= GROUP_CHARS ? name : name.substring(0, GROUP_CHARS);
    }

    /**
     * Whether a prefix pattern can be answered from one group: only a prefix at least as long as a group.
     *
     * @param prefix the prefix
     * @return true when every name starting with it is in the same group
     */
    public static boolean coversGroup(String prefix) {
        return prefix.length() >= GROUP_CHARS;
    }

    static int bucket(String name) {
        final byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return Math.floorMod(MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128()).h1, BUCKETS);
    }

    private ConditionalBlobContainer container(String group) {
        return ConditionalBlobContainer.of(blobStore.blobContainer(base.add(group)));
    }

    private static String blob(int bucket) {
        return String.format(java.util.Locale.ROOT, "%02x", bucket);
    }

    /**
     * Widens a shard's digest to cover a commit about to be published. Called before the manifest is written.
     *
     * @param name the index
     * @param uuid the incarnation publishing
     * @param shards its shard count
     * @param shard the shard
     * @param digest the commit's digest
     * @throws IOException if the register cannot be updated; the caller must not publish
     */
    public void widen(String name, String uuid, int shards, int shard, PruningDigest digest) throws IOException {
        final PruningDigest coarse = digest.coarsened();
        update(name, entry -> {
            final RollupEntry base = entry == null || entry.uuid().equals(uuid) == false ? fresh(uuid, shards) : entry;
            final RollupShardState was = base.states().get(shard);
            final PruningDigest merged = was.digest() == null ? coarse : was.digest().union(coarse);
            if (entry != null && merged.equals(was.digest()) && entry == base) {
                return null;
            }
            return with(base, shard, new RollupShardState(was.ownerTerm(), merged));
        });
    }

    /**
     * Widens a shard's digest for a publish, unless a writer at {@code term} or later already marks the shard.
     *
     * <p>A marked shard is never ruled out, so widening its digest while the mark stands changes no search: it is a
     * register read and swap per publish, which on a shard whose fields move with every write (a counter, a sequence)
     * is one of the larger parts of what a publish costs. The digest has to be right only from the moment the mark
     * is cleared, and every clear widens it to the published commit's digest in the same swap -- see
     * {@link #clearOwned(String, String, int, long, PruningDigest)}. So this skips the register when this node's copy
     * shows the mark, and widens as {@link #widen} does when it does not.
     *
     * @param name the index
     * @param uuid the incarnation publishing
     * @param shards its shard count
     * @param shard the shard
     * @param term the publishing writer's term
     * @param digest the commit's digest
     * @throws IOException if the register cannot be updated; the caller must not publish
     */
    public void widenUnlessMarked(String name, String uuid, int shards, int shard, long term, PruningDigest digest) throws IOException {
        if (term > 0L) {
            final Map<String, RollupEntry> remembered = known.get(key(name));
            final RollupEntry entry = remembered == null ? null : remembered.get(name);
            // This writer's own mark, exactly: a higher one is a successor's, which says nothing about this writer.
            //
            // Trusted from this node's copy because nothing else clears it while this writer can still publish
            // something new. The writer clears it itself, after its last publish. A survivor clears it only after
            // giving the head back on the writer's behalf and fencing the writer's log, and only when every record
            // that landed is in the published commit -- so a writer that was not as dead as it looked can publish
            // again only what that commit already holds, and the entry the survivor widened to it covers that.
            if (entry != null
                && entry.alias() == false
                && entry.uuid().equals(uuid)
                && shard < entry.states().size()
                && entry.states().get(shard).ownerTerm() == term) {
                return;
            }
        }
        widen(name, uuid, shards, shard, digest);
    }

    /**
     * Records that a writer holds a shard at a term. Called before the shard is opened.
     *
     * @param name the index
     * @param uuid the incarnation
     * @param shards its shard count
     * @param shard the shard
     * @param term the writer's term
     * @throws IOException if the register cannot be updated; the caller must not open the shard
     */
    public void markOwned(String name, String uuid, int shards, int shard, long term) throws IOException {
        final Change change = entry -> {
            final RollupEntry base = entry == null || entry.uuid().equals(uuid) == false ? fresh(uuid, shards) : entry;
            final RollupShardState was = base.states().get(shard);
            if (entry == base && was.ownerTerm() >= term) {
                return null;
            }
            return with(base, shard, new RollupShardState(Math.max(was.ownerTerm(), term), was.digest()));
        };
        final String key = key(name);
        final Map<String, RollupEntry> remembered = known.get(key);
        if (remembered != null && change.apply(remembered.get(name)) == null) {
            return;
        }
        // Marks for one register are written together. A node taking several shards whose names share a bucket --
        // every shard of one index does -- used to swap the register once per shard, concurrently, and most of
        // those swaps lost and read it again: three shards of one index cost six reads and six swaps. The first
        // mark to get the register writes every mark that arrived meanwhile in one swap; the rest wait for it.
        final MarkBatch batch;
        synchronized (gatheringMarks) {
            batch = gatheringMarks.computeIfAbsent(key, k -> new MarkBatch());
            batch.changes.add(Map.entry(name, change));
        }
        synchronized (markingLocks.computeIfAbsent(key, k -> new Object())) {
            synchronized (gatheringMarks) {
                gatheringMarks.remove(key, batch);
            }
            if (batch.done.isDone() == false) {
                try {
                    updateAll(group(name), bucket(name), batch.changes, false);
                    batch.done.complete(null);
                } catch (IOException | RuntimeException e) {
                    batch.done.completeExceptionally(e);
                }
            }
        }
        try {
            batch.done.join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e.getCause() instanceof RuntimeException runtime ? runtime : e;
        }
    }

    /** Marks gathered for one register, written by whichever of their callers gets the register first. */
    private static final class MarkBatch {
        private final List<Map.Entry<String, Change>> changes = new ArrayList<>();
        private final java.util.concurrent.CompletableFuture<Void> done = new java.util.concurrent.CompletableFuture<>();
    }

    /** The batch still taking marks, per register; guarded by itself. */
    private final Map<String, MarkBatch> gatheringMarks = new java.util.HashMap<>();
    /** One writer of marks at a time per register, so a batch closes while its predecessor is being swapped. */
    private final Map<String, Object> markingLocks = new ConcurrentHashMap<>();

    /**
     * Clears a writer's mark, if it is still that writer's, widening the shard's digest to the published commit's in
     * the same swap. Publishes made under the mark skip the rollup ({@link #widenUnlessMarked}), so this is where the
     * entry catches up with them, before the shard can be ruled out.
     *
     * <p>What the entry then covers is everything before the mark and the commit published last, not every commit in
     * between: a document changed or deleted by the last commit is no longer covered at its old value. A reader still
     * serving an earlier commit can hold it for a moment longer, and a wide search then answers as the last commit
     * would.
     *
     * @param name the index
     * @param uuid the incarnation
     * @param shard the shard
     * @param term the term being given up
     * @param published the digest of the shard's published commit, or null if it has none
     * @throws IOException if the register cannot be updated
     */
    public void clearOwned(String name, String uuid, int shard, long term, PruningDigest published) throws IOException {
        clearOwned(name, uuid, shard, term, published, true);
    }

    /**
     * Clears a mark another node left, widening the digest to the published commit's in the same swap, and reading the
     * register rather than trusting this node's copy.
     *
     * <p>An owner clearing its own mark can trust its copy: its own mark is in it. A node cleaning up after a dead one
     * cannot -- its copy may predate the mark, and a clear skipped on that copy left the shard marked for good.
     *
     * @param name the index
     * @param uuid the incarnation
     * @param shard the shard
     * @param term the term the mark records
     * @param published the digest of the shard's published commit, or null if it has none
     * @throws IOException if the register cannot be updated
     */
    public void clearOwnedOf(String name, String uuid, int shard, long term, PruningDigest published) throws IOException {
        clearOwned(name, uuid, shard, term, published, false);
    }

    private void clearOwned(String name, String uuid, int shard, long term, PruningDigest published, boolean trustRemembered)
        throws IOException {
        final PruningDigest coarse = published == null ? null : published.coarsened();
        update(name, trustRemembered, entry -> {
            if (entry == null || entry.alias() || entry.uuid().equals(uuid) == false || shard >= entry.states().size()) {
                return null;
            }
            final RollupShardState was = entry.states().get(shard);
            if (was.ownerTerm() != term) {
                return null;
            }
            final PruningDigest digest = coarse == null ? was.digest() : was.digest() == null ? coarse : was.digest().union(coarse);
            return with(entry, shard, new RollupShardState(0L, digest));
        });
    }

    /**
     * Removes an index's entry if it still records nothing: no shard owned, none published.
     *
     * <p>An entry is created when a writer marks a shard, before it opens. A dormant index opened only to be read --
     * a get, a check -- and given back with nothing written keeps that entry with no digest, and an entry with no
     * digest can never rule its index out: a fleet run found more than half of a group's entries were exactly that,
     * 60,000 indices no wide search could skip. With nothing to find, the index needs no entry at all -- a name
     * missing from the rollups is one with nothing searchable -- and the next writer marks it again before it opens.
     * The caller has checked the shards hold nothing; this only refuses to remove an entry that has since changed.
     *
     * @param name the index
     * @param uuid the incarnation the caller checked
     * @return true if the entry was removed or was already gone
     * @throws IOException if the register cannot be updated
     */
    public boolean removeIfNothing(String name, String uuid) throws IOException {
        final boolean[] removed = { true };
        update(name, false, entry -> {
            if (entry == null) {
                return null;
            }
            if (entry.alias() || entry.uuid().equals(uuid) == false || recordsNothing(entry) == false) {
                removed[0] = false;
                return null;
            }
            removed[0] = true;
            return REMOVED;
        });
        return removed[0];
    }

    /** Whether an entry records no owner and no published digest for any shard. */
    public static boolean recordsNothing(RollupEntry entry) {
        for (RollupShardState state : entry.states()) {
            if (state.ownerTerm() != 0L || state.digest() != null) {
                return false;
            }
        }
        return true;
    }

    /** What a change returns to remove the entry rather than replace it. */
    private static final RollupEntry REMOVED = new RollupEntry("_removed", 0, List.of());

    /** The uuid an alias's entry carries: never an index's, so an index's first event replaces it. */
    static final String ALIAS_UUID = "_alias";

    /**
     * Enters an alias's name, before the alias record is written: a pattern resolved from the rollups must find it,
     * and an entry a deleted index left under the same name must not rule it out.
     *
     * @param name the alias
     * @throws IOException if the register cannot be updated; the caller must not write the alias
     */
    public void markAlias(String name) throws IOException {
        update(name, entry -> entry != null && entry.alias() ? null : new RollupEntry(ALIAS_UUID, 0, List.of(), true));
    }

    /** One group's entries as this node last read them, and when. */
    private record Snapshot(Map<String, RollupEntry> entries, long readAtNanos) {
    }

    private final Map<String, Snapshot> groups = new ConcurrentHashMap<>();

    /**
     * Reads a group, reusing what this node read of it within {@code maxAgeMillis}.
     *
     * <p>A copy that young misses at most what was entered within it: an index activated in that window. Its
     * readers could not have served those documents yet, but its owner could, so the window is a lag a deployment
     * opts into ({@code serverless.search.rollup.cache_millis}); by default every search reads the group.
     *
     * @param group the group
     * @param reads how the register reads are run
     * @param maxAgeMillis how old a copy may be and still be used
     * @return entries by index name
     * @throws IOException if a register cannot be read
     */
    public Map<String, RollupEntry> readGroup(String group, DescriptorStore.Reads reads, long maxAgeMillis) throws IOException {
        final Snapshot cached = groups.get(group);
        final long now = System.nanoTime();
        if (cached != null
            && maxAgeMillis > 0
            && now - cached.readAtNanos() < java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(maxAgeMillis)) {
            return cached.entries();
        }
        final Map<String, RollupEntry> read = java.util.Collections.unmodifiableMap(readGroup(group, reads));
        groups.put(group, new Snapshot(read, now));
        return read;
    }

    /**
     * Reads every entry of a group, all its registers at once.
     *
     * @param group the group
     * @param reads how the register reads are run
     * @return entries by index name
     * @throws IOException if a register cannot be read
     */
    public Map<String, RollupEntry> readGroup(String group, DescriptorStore.Reads reads) throws IOException {
        final ConditionalBlobContainer container = container(group);
        final List<Callable<Map<String, RollupEntry>>> tasks = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKETS; bucket++) {
            final String blob = blob(bucket);
            tasks.add(() -> {
                final Optional<BlobRegister> register = container.readRegister(blob);
                return register.isEmpty() ? Map.of() : parse(register.get().value());
            });
        }
        final List<Map<String, RollupEntry>> read;
        try {
            read = reads.runAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted reading the digest rollups of [" + group + "]", e);
        }
        final Map<String, RollupEntry> all = new LinkedHashMap<>();
        for (int i = 0; i < read.size(); i++) {
            if (read.get(i) == null) {
                throw new IOException("could not read digest rollup " + i + " of [" + group + "]");
            }
            all.putAll(read.get(i));
        }
        return all;
    }

    /**
     * Lists the groups that have rollups.
     *
     * @return the group names
     * @throws IOException if the store cannot be listed
     */
    public List<String> groups() throws IOException {
        return new ArrayList<>(blobStore.blobContainer(base).children().keySet());
    }

    /**
     * Reads one register of a group: the entries of the names that hash to that bucket.
     *
     * @param group the group
     * @param bucket the bucket, below {@value #BUCKETS}
     * @return entries by index name
     * @throws IOException if the register cannot be read
     */
    public Map<String, RollupEntry> readBucket(String group, int bucket) throws IOException {
        final Optional<BlobRegister> register = container(group).readRegister(blob(bucket));
        return register.isEmpty() ? Map.of() : parse(register.get().value());
    }

    @FunctionalInterface
    private interface Change {
        /** The new entry, or null to leave the register as it is. */
        RollupEntry apply(RollupEntry current);
    }

    /** A read-change-swap loop on the name's bucket, skipping the read when this node's last copy says nothing changes. */
    private void update(String name, Change change) throws IOException {
        update(name, true, change);
    }

    /** The key of the register a name lives in, as {@link #known} holds it. */
    private static String key(String name) {
        return group(name) + "/" + blob(bucket(name));
    }

    private void update(String name, boolean trustRemembered, Change change) throws IOException {
        updateAll(group(name), bucket(name), List.of(Map.entry(name, change)), trustRemembered);
    }

    /** A read-change-swap loop applying every change, in order, to one register, in one swap. */
    private void updateAll(String group, int bucket, List<Map.Entry<String, Change>> changes, boolean trustRemembered) throws IOException {
        final String key = group + "/" + blob(bucket);
        final ConditionalBlobContainer container = container(group);
        final Map<String, RollupEntry> remembered = known.get(key);
        if (trustRemembered && remembered != null) {
            // Nothing this node knows of would change. The copy may be stale, but a register only ever widens
            // within an incarnation, so if the stale copy already covers the change the current one does too.
            boolean anyChange = false;
            for (Map.Entry<String, Change> each : changes) {
                if (each.getValue().apply(remembered.get(each.getKey())) != null) {
                    anyChange = true;
                    break;
                }
            }
            if (anyChange == false) {
                return;
            }
        }
        for (int attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
            final Optional<BlobRegister> register = container.readRegister(blob(bucket));
            final Map<String, RollupEntry> current = register.isEmpty()
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(parse(register.get().value()));
            boolean changed = false;
            for (Map.Entry<String, Change> each : changes) {
                final RollupEntry next = each.getValue().apply(current.get(each.getKey()));
                if (next == null) {
                    continue;
                }
                changed = true;
                if (next == REMOVED) {
                    current.remove(each.getKey());
                } else {
                    current.put(each.getKey(), next);
                }
            }
            if (changed == false) {
                known.put(key, current);
                return;
            }
            final BytesReference bytes = encode(current);
            final BlobRegisterCasResult result = register.isEmpty()
                ? container.createRegisterIfAbsent(blob(bucket), bytes)
                : container.compareAndSwapRegister(blob(bucket), register.get().generation(), bytes);
            if (result.applied()) {
                known.put(key, current);
                return;
            }
        }
        throw new IOException(
            "could not update the digest rollup of ["
                + changes.get(0).getKey()
                + "]"
                + (changes.size() > 1 ? " and others in its register" : "")
                + ": the register kept moving"
        );
    }

    private static RollupEntry fresh(String uuid, int shards) {
        final List<RollupShardState> states = new ArrayList<>(shards);
        for (int i = 0; i < shards; i++) {
            states.add(new RollupShardState(0L, null));
        }
        return new RollupEntry(uuid, shards, List.copyOf(states));
    }

    private static RollupEntry with(RollupEntry entry, int shard, RollupShardState state) {
        final List<RollupShardState> states = new ArrayList<>(entry.states());
        states.set(shard, state);
        return new RollupEntry(entry.uuid(), entry.shards(), java.util.Collections.unmodifiableList(states), entry.alias());
    }

    static BytesReference encode(Map<String, RollupEntry> entries) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.startObject("entries");
            for (Map.Entry<String, RollupEntry> e : entries.entrySet()) {
                builder.startObject(e.getKey());
                if (e.getValue().alias()) {
                    builder.field("alias", true);
                }
                builder.field("uuid", e.getValue().uuid());
                builder.field("shards", e.getValue().shards());
                builder.startArray("states");
                for (RollupShardState state : e.getValue().states()) {
                    builder.startObject();
                    builder.field("owner", state.ownerTerm());
                    if (state.digest() != null) {
                        builder.field("digest");
                        state.digest().toXContent(builder);
                    }
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    static Map<String, RollupEntry> parse(BytesReference bytes) throws IOException {
        final Map<String, RollupEntry> entries = new LinkedHashMap<>();
        try (
            InputStream in = bytes.streamInput();
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, in)
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IOException("digest rollup must be an object");
            }
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == null) {
                    throw new IOException("unexpected end of digest rollup");
                }
                if (token == XContentParser.Token.FIELD_NAME && "entries".equals(parser.currentName())) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new IOException("digest rollup entries must be an object");
                    }
                    while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        final String name = parser.currentName();
                        parser.nextToken();
                        entries.put(name, parseEntry(parser));
                    }
                } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                }
            }
        }
        return entries;
    }

    private static RollupEntry parseEntry(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IOException("digest rollup entry must be an object");
        }
        String uuid = null;
        int shards = 0;
        boolean alias = false;
        final List<RollupShardState> states = new ArrayList<>();
        String field = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == null) {
                throw new IOException("unexpected end of digest rollup entry");
            }
            if (token == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY && "states".equals(field)) {
                while (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                    long owner = 0L;
                    PruningDigest digest = null;
                    String key = null;
                    XContentParser.Token inner;
                    while ((inner = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (inner == null) {
                            throw new IOException("unexpected end of digest rollup shard state");
                        }
                        if (inner == XContentParser.Token.FIELD_NAME) {
                            key = parser.currentName();
                        } else if (inner == XContentParser.Token.START_OBJECT && "digest".equals(key)) {
                            digest = PruningDigest.parse(parser);
                        } else if (inner == XContentParser.Token.START_OBJECT || inner == XContentParser.Token.START_ARRAY) {
                            parser.skipChildren();
                        } else if ("owner".equals(key)) {
                            owner = parser.longValue();
                        }
                    }
                    states.add(new RollupShardState(owner, digest));
                }
            } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else if ("uuid".equals(field)) {
                uuid = parser.text();
            } else if ("shards".equals(field)) {
                shards = parser.intValue();
            } else if ("alias".equals(field)) {
                alias = parser.booleanValue();
            }
        }
        return new RollupEntry(uuid, shards, java.util.Collections.unmodifiableList(states), alias);
    }

    /**
     * For tests only: overwrites an index's entry, ignoring the invariant -- so a test can plant an entry
     * narrower than its shard and show that the conservativeness canary catches it.
     *
     * @param name the index
     * @param entry what to write
     * @throws IOException if the register cannot be written
     */
    public void plantForTest(String name, RollupEntry entry) throws IOException {
        update(name, current -> entry);
    }
}
