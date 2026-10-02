/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
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
 *   <li>A publish widens the shard's digest here <em>before</em> it writes its manifest. A publish that fails in
 *   between leaves the entry wider than the commit, never narrower.</li>
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
    private final Map<String, Map<String, Entry>> known = new ConcurrentHashMap<>();

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

    /** One shard's part of an entry: the term of a writer holding it, zero if none; and its digest, null if never published. */
    public record ShardState(long ownerTerm, PruningDigest digest) {
    }

    /**
     * What a bucket knows of one index.
     *
     * @param uuid the incarnation the entry was written for
     * @param shards the incarnation's shard count
     * @param states one per shard
     */
    public record Entry(String uuid, int shards, List<ShardState> states, boolean alias) {

        /**
         * An index's entry.
         *
         * @param uuid the incarnation the entry was written for
         * @param shards the incarnation's shard count
         * @param states one per shard
         */
        public Entry(String uuid, int shards, List<ShardState> states) {
            this(uuid, shards, states, false);
        }

        /**
         * Whether this entry settles that the index may match: every shard unowned and published, and not all of
         * them ruled out. A shard's own digest is never wider than its entry, so reading it could rule out nothing
         * more -- the per-shard check can be skipped.
         *
         * @param query the query
         * @param nowMillis the request's single now
         * @return true when the entry alone has decided the index is to be searched
         */
        public boolean judgedMatchable(QueryBuilder query, long nowMillis) {
            if (alias || states.size() != shards) {
                return false;
            }
            for (ShardState state : states) {
                if (state.ownerTerm() != 0L || state.digest() == null) {
                    return false;
                }
            }
            return rulesOut(query, nowMillis) == false;
        }

        /**
         * Whether this entry proves the query can match nothing the index holds.
         *
         * @param query the query
         * @param nowMillis the request's single now
         * @return true only when every shard is unowned, published and ruled out by its digest
         */
        public boolean rulesOut(QueryBuilder query, long nowMillis) {
            // An alias resolves to indices the pattern may not name; what they hold is not here to judge.
            if (alias || states.size() != shards) {
                return false;
            }
            for (ShardState state : states) {
                if (state.ownerTerm() != 0L || state.digest() == null || state.digest().canMatch(query, nowMillis)) {
                    return false;
                }
            }
            return true;
        }
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

    private BlobContainer container(String group) {
        return blobStore.blobContainer(base.add(group));
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
            final Entry base = entry == null || entry.uuid().equals(uuid) == false ? fresh(uuid, shards) : entry;
            final ShardState was = base.states().get(shard);
            final PruningDigest merged = was.digest() == null ? coarse : was.digest().union(coarse);
            if (entry != null && merged.equals(was.digest()) && entry == base) {
                return null;
            }
            return with(base, shard, new ShardState(was.ownerTerm(), merged));
        });
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
        update(name, entry -> {
            final Entry base = entry == null || entry.uuid().equals(uuid) == false ? fresh(uuid, shards) : entry;
            final ShardState was = base.states().get(shard);
            if (entry == base && was.ownerTerm() >= term) {
                return null;
            }
            return with(base, shard, new ShardState(Math.max(was.ownerTerm(), term), was.digest()));
        });
    }

    /**
     * Clears a writer's mark, if it is still that writer's. Called after the last publish and the head's release;
     * a clear that fails leaves the shard marked, which only stops it being ruled out.
     *
     * @param name the index
     * @param uuid the incarnation
     * @param shard the shard
     * @param term the term being given up
     * @throws IOException if the register cannot be updated
     */
    public void clearOwned(String name, String uuid, int shard, long term) throws IOException {
        clearOwned(name, uuid, shard, term, true);
    }

    /**
     * Clears a mark another node left, reading the register rather than trusting this node's copy.
     *
     * <p>An owner clearing its own mark can trust its copy: its own mark is in it. A node cleaning up after a dead one
     * cannot -- its copy may predate the mark, and a clear skipped on that copy left the shard marked for good.
     *
     * @param name the index
     * @param uuid the incarnation
     * @param shard the shard
     * @param term the term the mark records
     * @throws IOException if the register cannot be updated
     */
    public void clearOwnedOf(String name, String uuid, int shard, long term) throws IOException {
        clearOwned(name, uuid, shard, term, false);
    }

    private void clearOwned(String name, String uuid, int shard, long term, boolean trustRemembered) throws IOException {
        update(name, trustRemembered, entry -> {
            if (entry == null || entry.alias() || entry.uuid().equals(uuid) == false || shard >= entry.states().size()) {
                return null;
            }
            final ShardState was = entry.states().get(shard);
            if (was.ownerTerm() != term) {
                return null;
            }
            return with(entry, shard, new ShardState(0L, was.digest()));
        });
    }

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
        update(name, entry -> entry != null && entry.alias() ? null : new Entry(ALIAS_UUID, 0, List.of(), true));
    }

    /** One group's entries as this node last read them, and when. */
    private record Snapshot(Map<String, Entry> entries, long readAtNanos) {
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
    public Map<String, Entry> readGroup(String group, DescriptorStore.Reads reads, long maxAgeMillis) throws IOException {
        final Snapshot cached = groups.get(group);
        final long now = System.nanoTime();
        if (cached != null
            && maxAgeMillis > 0
            && now - cached.readAtNanos() < java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(maxAgeMillis)) {
            return cached.entries();
        }
        final Map<String, Entry> read = java.util.Collections.unmodifiableMap(readGroup(group, reads));
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
    public Map<String, Entry> readGroup(String group, DescriptorStore.Reads reads) throws IOException {
        final BlobContainer container = container(group);
        final List<Callable<Map<String, Entry>>> tasks = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKETS; bucket++) {
            final String blob = blob(bucket);
            tasks.add(() -> {
                final Optional<BlobRegister> register = container.readRegister(blob);
                return register.isEmpty() ? Map.of() : parse(register.get().value());
            });
        }
        final List<Map<String, Entry>> read;
        try {
            read = reads.runAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted reading the digest rollups of [" + group + "]", e);
        }
        final Map<String, Entry> all = new LinkedHashMap<>();
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
    public Map<String, Entry> readBucket(String group, int bucket) throws IOException {
        final Optional<BlobRegister> register = container(group).readRegister(blob(bucket));
        return register.isEmpty() ? Map.of() : parse(register.get().value());
    }

    @FunctionalInterface
    private interface Change {
        /** The new entry, or null to leave the register as it is. */
        Entry apply(Entry current);
    }

    /** A read-change-swap loop on the name's bucket, skipping the read when this node's last copy says nothing changes. */
    private void update(String name, Change change) throws IOException {
        update(name, true, change);
    }

    private void update(String name, boolean trustRemembered, Change change) throws IOException {
        final String group = group(name);
        final int bucket = bucket(name);
        final String key = group + "/" + blob(bucket);
        final BlobContainer container = container(group);
        final Map<String, Entry> remembered = known.get(key);
        if (trustRemembered && remembered != null && change.apply(remembered.get(name)) == null) {
            // Nothing this node knows of would change. The copy may be stale, but a register only ever widens
            // within an incarnation, so if the stale copy already covers the change the current one does too.
            return;
        }
        for (int attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
            final Optional<BlobRegister> register = container.readRegister(blob(bucket));
            final Map<String, Entry> current = register.isEmpty()
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(parse(register.get().value()));
            final Entry next = change.apply(current.get(name));
            if (next == null) {
                known.put(key, current);
                return;
            }
            current.put(name, next);
            final BytesReference bytes = encode(current);
            final BlobRegisterCasResult result = register.isEmpty()
                ? container.createRegisterIfAbsent(blob(bucket), bytes)
                : container.compareAndSwapRegister(blob(bucket), register.get().generation(), bytes);
            if (result.applied()) {
                known.put(key, current);
                return;
            }
        }
        throw new IOException("could not update the digest rollup of [" + name + "]: the register kept moving");
    }

    private static Entry fresh(String uuid, int shards) {
        final List<ShardState> states = new ArrayList<>(shards);
        for (int i = 0; i < shards; i++) {
            states.add(new ShardState(0L, null));
        }
        return new Entry(uuid, shards, List.copyOf(states));
    }

    private static Entry with(Entry entry, int shard, ShardState state) {
        final List<ShardState> states = new ArrayList<>(entry.states());
        states.set(shard, state);
        return new Entry(entry.uuid(), entry.shards(), java.util.Collections.unmodifiableList(states), entry.alias());
    }

    static BytesReference encode(Map<String, Entry> entries) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.startObject("entries");
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                builder.startObject(e.getKey());
                if (e.getValue().alias()) {
                    builder.field("alias", true);
                }
                builder.field("uuid", e.getValue().uuid());
                builder.field("shards", e.getValue().shards());
                builder.startArray("states");
                for (ShardState state : e.getValue().states()) {
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

    static Map<String, Entry> parse(BytesReference bytes) throws IOException {
        final Map<String, Entry> entries = new LinkedHashMap<>();
        try (
            InputStream in = bytes.streamInput();
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, in)
        ) {
            parser.nextToken();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME && "entries".equals(parser.currentName())) {
                    parser.nextToken();
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

    private static Entry parseEntry(XContentParser parser) throws IOException {
        String uuid = null;
        int shards = 0;
        boolean alias = false;
        final List<ShardState> states = new ArrayList<>();
        String field = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY && "states".equals(field)) {
                while (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                    long owner = 0L;
                    PruningDigest digest = null;
                    String key = null;
                    XContentParser.Token inner;
                    while ((inner = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
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
                    states.add(new ShardState(owner, digest));
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
        return new Entry(uuid, shards, java.util.Collections.unmodifiableList(states), alias);
    }

    /**
     * For tests only: overwrites an index's entry, ignoring the invariant -- so a test can plant an entry
     * narrower than its shard and show that the conservativeness canary catches it.
     *
     * @param name the index
     * @param entry what to write
     * @throws IOException if the register cannot be written
     */
    public void plantForTest(String name, Entry entry) throws IOException {
        update(name, current -> entry);
    }
}
