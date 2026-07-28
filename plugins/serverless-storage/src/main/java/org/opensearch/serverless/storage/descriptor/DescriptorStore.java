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
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The one place descriptors are read and written.
 *
 * <p>Every seam in Area H needs the same three operations and none of them existed: a get by id for
 * exact-name resolution, a put with {@code op_type=create} for uniqueness without a cluster state update,
 * and a prefix search with {@code search_after} paging for wildcards and listings. They live here rather
 * than being open-coded at each seam, which is precisely the mistake C3 made with routing lookups: the
 * resolution path and the write path were hooked separately, drifted apart, and the divergence was possible
 * only because the pairing was written out at every site.
 *
 * <p><b>The three operations have different consistency, and that is measured rather than chosen.</b> H18
 * established that a get by id reads through the translog and so sees a descriptor the instant its write is
 * acknowledged, while a search sees only refreshed segments. So {@link #get} is realtime and {@link
 * #findByPrefix} is refresh-bound, and callers needing immediate visibility must name the index exactly.
 *
 * <p><b>The index is created lazily.</b> A cluster that never creates a gated index never pays for it, and
 * creating at node start would put an index creation on every node's startup path for a feature most
 * clusters will not use. The flag is an optimisation only: creation races are resolved by
 * {@code ResourceAlreadyExistsException} being treated as success, because two nodes creating the
 * descriptor index concurrently is the normal case rather than an error.
 */
public final class DescriptorStore {

    private static final Logger logger = LogManager.getLogger(DescriptorStore.class);

    /** The descriptor index's name. Fixed rather than configurable: it is part of the on-disk contract. */
    public static final String DESCRIPTOR_INDEX = ".opensearch-index-descriptors";

    /**
     * How many descriptors one page of a prefix search returns.
     *
     * <p>S24 measured about 33 ms per thousand names, so a page is a bounded cost rather than a preference.
     * Callers that need everything page through; callers that need a page get one.
     */
    public static final int PAGE_SIZE = 1_000;

    /**
     * How long a descriptor read is served from memory before going to the index again.
     *
     * <p>{@code AbsentIndexDescriptorSuppliers} states that a supplier is expected to answer from a cache,
     * and this one had none: every resolution of an unresolved name issued a real get. P7 then made it
     * per-request for gated indices, because synthesising placement metadata reads the descriptor on every
     * routing resolution.
     *
     * <p>One second is the same window {@code StoreBackedFieldRefresher} uses, and for the same reason: it
     * bounds staleness by an interval rather than leaving it unbounded, and a descriptor changes far less
     * often than it is read. Exact-name resolution stays realtime for a <em>newly created</em> index because
     * a miss is not cached, only a hit.
     */
    static final long CACHE_TTL_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(1);

    /**
     * How many descriptors one node keeps in memory.
     *
     * <p>Retained as a secondary guard rather than as the bound. T4b measured that a descriptor with twenty
     * plain aliases is 5.8 times a typical one and two hundred aliases is 51.6 times, so an entry count
     * admits a fiftyfold range of memory and cannot be the thing defending the residency ceiling.
     */
    static final int CACHE_CAPACITY = 50_000;

    /**
     * How much memory those descriptors may occupy, which is the bound that actually holds.
     *
     * <p>T4b is the reason this exists and the reason it is bytes. The rule was stated before the numbers:
     * a byte budget earns its cost if a plausible heavy descriptor is more than about five times a typical
     * one. Measured at 5.8x for twenty aliases and 51.6x for two hundred, so it does.
     *
     * <p>Thirty-two megabytes is roughly what fifty thousand typical descriptors occupy, so a cluster of
     * ordinary indices sees the same behaviour it saw before, while a cluster of heavily aliased ones is
     * held to a memory figure rather than to an entry count that means nothing about memory.
     */
    static final long CACHE_BYTES = 32L * 1024 * 1024;

    /**
     * Charged per entry on top of the descriptor itself, for the map node holding it.
     *
     * <p>Without it a population of very small descriptors would be bounded by a number that ignores the
     * structure doing the holding, which is the same class of error as bounding by entry count.
     */
    private static final long ENTRY_OVERHEAD_BYTES = 64;

    /**
     * How long a caller waits for another caller's in-flight read before going to the index itself.
     *
     * <p>Generous relative to the 2.7 ms T1 measured for an uncontended read, because the point is to bound
     * a pathological wait rather than to race a healthy one. TiDB sets a 3 s timeout on the equivalent
     * storage read for the same reason, so a slow meta region leader does not stall every reader behind it.
     */
    static final long COLLAPSE_WAIT_MILLIS = 3_000;

    /**
     * A cached descriptor, carrying two independent clocks.
     *
     * <p>{@code readAtNanos} answers "is this still fresh" and {@code admittedAt} answers "is this the
     * stalest thing here". They are separate because a wall clock cannot do the second job: entries admitted
     * within one {@code nanoTime} tick share a value, and an eviction threshold taken from equal stamps
     * evicts nothing, so a bulk warm-up could leave the cache over its bound while re-sorting every entry on
     * every admission and making no progress. A monotonic sequence is unique by construction, which is what
     * {@code GatedShardSuspensionRegistry} stamps for the same reason.
     */
    private record CachedDescriptor(IndexDescriptor descriptor, long readAtNanos, long admittedAt, long bytes) {
    }

    private final java.util.concurrent.ConcurrentHashMap<String, CachedDescriptor> cache = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The name currently being read, so concurrent misses on it wait rather than each issuing a read.
     *
     * <p>An entry lives only for the duration of one read, which is why this needs no capacity bound while
     * {@link #cache} does.
     */
    private final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.CompletableFuture<IndexDescriptor>> inFlight =
        new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.function.LongSupplier clock;
    private final long collapseWaitMillis;
    private final int cacheCapacity;
    private final long cacheBytes;
    private final java.util.concurrent.atomic.AtomicLong reads = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong evictions = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong admissions = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong cachedBytes = new java.util.concurrent.atomic.AtomicLong();

    private final Client client;
    private final int shardCount;
    private final AtomicBoolean indexKnownToExist = new AtomicBoolean();

    public DescriptorStore(Client client, int shardCount) {
        this(client, shardCount, System::nanoTime);
    }

    /** Test seam for the clock, so the cache window can be exercised without sleeping. */
    DescriptorStore(Client client, int shardCount, java.util.function.LongSupplier clock) {
        this(client, shardCount, clock, COLLAPSE_WAIT_MILLIS, CACHE_CAPACITY, CACHE_BYTES);
    }

    /**
     * Test seam for the collapse wait, so the fallback can be exercised without waiting three seconds.
     *
     * <p>Worth a seam rather than being left to a slow-read scenario nothing can arrange: an untested
     * fallback is how this area produced mechanisms that were correct and never reached.
     */
    DescriptorStore(Client client, int shardCount, java.util.function.LongSupplier clock, long collapseWaitMillis) {
        this(client, shardCount, clock, collapseWaitMillis, CACHE_CAPACITY, CACHE_BYTES);
    }

    /**
     * Test seam for the capacity, so behaviour at and past the bound can be measured without creating
     * fifty thousand descriptors to reach it.
     */
    DescriptorStore(Client client, int shardCount, java.util.function.LongSupplier clock, long collapseWaitMillis, int cacheCapacity) {
        this(client, shardCount, clock, collapseWaitMillis, cacheCapacity, CACHE_BYTES);
    }

    /** Test seam for the byte budget, so the bound T4b established can be reached without 32 MB of names. */
    DescriptorStore(
        Client client,
        int shardCount,
        java.util.function.LongSupplier clock,
        long collapseWaitMillis,
        int cacheCapacity,
        long cacheBytes
    ) {
        this.client = client;
        this.shardCount = shardCount;
        this.clock = clock;
        this.collapseWaitMillis = collapseWaitMillis;
        this.cacheCapacity = cacheCapacity;
        this.cacheBytes = cacheBytes;
    }

    /**
     * Pretends a read for this name is already in flight and will never finish, so a test can prove a
     * waiter falls back rather than hanging behind it.
     */
    java.util.concurrent.CompletableFuture<IndexDescriptor> pretendReadIsInFlight(String name) {
        java.util.concurrent.CompletableFuture<IndexDescriptor> never = new java.util.concurrent.CompletableFuture<>();
        inFlight.put(name, never);
        return never;
    }

    /** How many times this store has actually gone to the index, which is what a test counts. */
    public long readCount() {
        return reads.get();
    }

    /** How many cached descriptors have been evicted, which distinguishes a small cache from a broken one. */
    public long evictionCount() {
        return evictions.get();
    }

    /** How many descriptors are cached, which is what the capacity bounds and what a test checks. */
    int cachedCount() {
        return cache.size();
    }

    /** How much memory the cached descriptors occupy, which is the bound T4b established has to hold. */
    long cachedBytes() {
        return cachedBytes.get();
    }

    /**
     * Caches a descriptor, evicting the stalest entries if that puts the cache over its bound.
     *
     * <p><b>The admission is unconditional, which T3 is the reason for.</b> This used to be gated on
     * {@code cache.size() < cacheCapacity}, and since nothing ever removed an entry, that was not an
     * eviction policy but a freeze. The first {@code cacheCapacity} names held every slot permanently, and
     * because an expired entry was re-read and then not re-admitted, every entry went stale after one
     * second and could never be refreshed. Past the bound the cache held its memory and served nothing:
     * T3 measured twenty names costing twenty reads on every pass, forever.
     *
     * <p>Recency is stamped on write and never touched on read, which is the structure P2 established for
     * the suspension registry after P1 measured a read-mutating LRU running backwards under contention.
     * The same reasoning applies here for the same reason: this is read on every resolution.
     */
    private void admit(String name, IndexDescriptor descriptor, long now) {
        long bytes = retainedSizeOf(name, descriptor);
        CachedDescriptor replaced = cache.put(name, new CachedDescriptor(descriptor, now, admissions.incrementAndGet(), bytes));
        // Net, so refreshing an entry does not double count it. A refresh is the common case once the
        // window starts expiring, so getting this wrong would drift the total upward until the cache
        // evicted itself to nothing.
        cachedBytes.addAndGet(bytes - (replaced == null ? 0 : replaced.bytes()));
        evictIfOverCapacity();
    }

    /**
     * What one cached descriptor costs.
     *
     * <p>The alias list is the only unbounded part, which is what T4b measured, so it is the only part
     * walked rather than assumed.
     */
    private static long retainedSizeOf(String name, IndexDescriptor descriptor) {
        long bytes = ENTRY_OVERHEAD_BYTES + org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance(IndexDescriptor.class)
            + org.apache.lucene.util.RamUsageEstimator.sizeOf(name) + org.apache.lucene.util.RamUsageEstimator.sizeOf(descriptor.uuid());
        return bytes + org.apache.lucene.util.RamUsageEstimator.sizeOfCollection(descriptor.aliases());
    }

    /**
     * Drops the stalest tenth when the cache is over its bound.
     *
     * <p>A threshold from one pass over the stamps rather than a sorted eviction list, matching
     * {@code GatedShardSuspensionRegistry}. Evicting a tenth rather than a single entry keeps this from
     * running on every admission once the cache is full.
     */
    private void evictIfOverCapacity() {
        if (cache.size() <= cacheCapacity && cachedBytes.get() <= cacheBytes) {
            return;
        }
        // Repeated because one pass drops a tenth of the entries, which is a tenth of the count but an
        // unknown share of the bytes. A single pass would leave a cache of large descriptors over budget.
        // Bounded so a concurrent writer refilling as fast as this drains cannot spin here forever.
        for (int pass = 0; pass < 10 && (cache.size() > cacheCapacity || cachedBytes.get() > cacheBytes); pass++) {
            if (evictStalestTenth() == false) {
                return;
            }
        }
    }

    /** Drops the stalest tenth, reporting whether there was anything to drop. */
    private boolean evictStalestTenth() {
        int toEvict = Math.max(1, cache.size() / 10);
        long[] stamps = cache.values().stream().mapToLong(CachedDescriptor::admittedAt).sorted().toArray();
        if (stamps.length == 0) {
            return false;
        }
        long threshold = stamps[Math.min(toEvict, stamps.length - 1)];
        boolean[] dropped = new boolean[1];
        cache.entrySet().removeIf(entry -> {
            if (entry.getValue().admittedAt() < threshold) {
                cachedBytes.addAndGet(-entry.getValue().bytes());
                evictions.incrementAndGet();
                dropped[0] = true;
                return true;
            }
            return false;
        });
        return dropped[0];
    }

    /** Drops every cached descriptor, which a write must do so it does not serve its own stale value. */
    public void invalidate(String name) {
        CachedDescriptor removed = cache.remove(name);
        if (removed != null) {
            cachedBytes.addAndGet(-removed.bytes());
        }
    }

    /**
     * The descriptor for this name, or null if there is none.
     *
     * <p>Realtime, per H18: this is a get by id, so it reads through the translog and sees a descriptor as
     * soon as its write is acknowledged. That is what lets a client create an index and immediately write
     * to it.
     */
    public IndexDescriptor get(String name) {
        // The guard precedes the read, which P3 and P5 both had to learn the hard way: a cache consulted
        // after the expensive call prevents nothing.
        CachedDescriptor cached = cache.get(name);
        long now = clock.getAsLong();
        if (cached != null && now - cached.readAtNanos() < CACHE_TTL_NANOS) {
            return cached.descriptor();
        }

        // The window above only helps a caller arriving after some other caller finished reading. Callers
        // arriving together all see the same empty slot, and T1 measured that they all go to the index:
        // sixty-four concurrent resolutions of one name issued sixty-four reads. So one of them reads and
        // the rest wait on it.
        java.util.concurrent.CompletableFuture<IndexDescriptor> mine = new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<IndexDescriptor> reader = inFlight.putIfAbsent(name, mine);
        if (reader != null) {
            return awaitOrReadDirectly(reader, name);
        }
        try {
            IndexDescriptor descriptor = readFromIndex(name, now);
            mine.complete(descriptor);
            return descriptor;
        } finally {
            // Completing an already-completed future is a no-op, so this only fires if readFromIndex threw
            // an Error. Without it a waiter would block until its own timeout for no reason.
            mine.complete(null);
            inFlight.remove(name, mine);
        }
    }

    /**
     * Waits for the caller that is already reading this name, and reads directly if that takes too long.
     *
     * <p>The fallback is the point. Collapsing turns N independent reads into one read with N-1 threads
     * depending on it, which is a new way to fail: before, a caller that hung hung alone. A bounded wait
     * keeps the failure blast radius what it was, at the cost of occasionally issuing the read this exists
     * to avoid.
     */
    private IndexDescriptor awaitOrReadDirectly(java.util.concurrent.CompletableFuture<IndexDescriptor> reader, String name) {
        try {
            return reader.get(collapseWaitMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (java.util.concurrent.TimeoutException e) {
            logger.debug("waited [{}] ms on an in-flight descriptor read for [{}]; reading directly", collapseWaitMillis, name);
            // The clock is read again rather than reused from before the wait. A timestamp taken up to
            // collapseWaitMillis ago would stamp the entry as already expired against a one second window,
            // so the cache would be populated with something nothing can ever read.
            return readFromIndex(name, clock.getAsLong());
        } catch (Exception e) {
            logger.debug("in-flight descriptor read for [{}] failed", name, e);
            return null;
        }
    }

    private IndexDescriptor readFromIndex(String name, long now) {
        try {
            reads.incrementAndGet();
            var response = client.prepareGet(DESCRIPTOR_INDEX, name).get();
            if (response.isExists() == false) {
                // A miss is deliberately not cached. An index created a moment ago must be nameable
                // immediately (H18), and caching absence would delay that by the window.
                return null;
            }
            IndexDescriptor descriptor = DescriptorCodec.fromSource(response.getSourceAsMap());
            admit(name, descriptor, now);
            return descriptor;
        } catch (Exception e) {
            // A missing index is a missing descriptor, not a failure: before the first gated creation the
            // descriptor index does not exist, and every resolution would otherwise throw.
            logger.debug("descriptor lookup for [{}] failed", name, e);
            return null;
        }
    }

    /**
     * Records a descriptor for an index that does not yet exist, returning whether this call created it.
     *
     * <p>{@code op_type=create} is what makes the name unique without a cluster state update, which is the
     * whole of H3: the store, rather than the cluster manager, is the thing that says a name is taken. A
     * version conflict is therefore a lost race and a correct answer, not an error.
     */
    public boolean create(IndexDescriptor descriptor) {
        ensureIndexExists();
        try {
            client.index(new IndexRequest(DESCRIPTOR_INDEX).id(descriptor.name()).source(DescriptorCodec.toSource(descriptor)).create(true))
                .actionGet();
            invalidate(descriptor.name());
            return true;
        } catch (VersionConflictEngineException e) {
            // Someone else created this name first. That is the mechanism working.
            return false;
        }
    }

    /** Overwrites a descriptor, for a mapping generation bump or a tombstone. Blocks until written. */
    public void put(IndexDescriptor descriptor) {
        ensureIndexExists();
        client.index(new IndexRequest(DESCRIPTOR_INDEX).id(descriptor.name()).source(DescriptorCodec.toSource(descriptor))).actionGet();
        invalidate(descriptor.name());
    }

    /**
     * Records a descriptor without waiting for it to be written.
     *
     * <p><b>This exists because the publish hook runs on the cluster state thread.</b>
     * {@code Metadata} calls {@code IndexDescriptorPublisher.publish} while a cluster state is being built,
     * so a blocking write here deadlocks: the cluster state update waits on an index operation that itself
     * needs a cluster state to route. Registering the blocking {@link #put} as the publisher hung the node
     * rather than failing, which is how it was found.
     *
     * <p>The cost of not waiting is that a descriptor write can be lost to a crash between the cluster
     * state commit and the index operation landing. For creation that is tolerable, since the index is in
     * cluster state and the descriptor is redundant there. <b>For a tombstone it is not</b>, because H4b
     * established that a node adopting dangling shard data must be able to read the tombstone, and a lost
     * tombstone is exactly the case that protects against. That gap is real and is tracked rather than
     * papered over here.
     *
     * <p>Failures are logged rather than thrown for the same reason they are swallowed on the read path:
     * this runs inside cluster state construction, and an exception raised there fails an unrelated
     * cluster state update.
     */
    public void putAsync(IndexDescriptor descriptor) {
        try {
            client.index(
                new IndexRequest(DESCRIPTOR_INDEX).id(descriptor.name()).source(DescriptorCodec.toSource(descriptor)),
                new org.opensearch.core.action.ActionListener<>() {
                    @Override
                    public void onResponse(org.opensearch.action.index.IndexResponse response) {}

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("failed to record descriptor for [{}]", descriptor.name(), e);
                    }
                }
            );
        } catch (Exception e) {
            logger.warn("failed to submit descriptor write for [{}]", descriptor.name(), e);
        }
    }

    /**
     * How many times a tombstone write is retried before giving up.
     *
     * <p>A tombstone is the one descriptor write that is not safe to lose (H4b): a node adopting dangling
     * shard data must be able to read it, and for a gated index there is no cluster state entry and no
     * graveyard entry to fall back on. Retrying closes the transient half of that risk, where the write
     * fails because the mapping index is briefly unavailable or the node is busy.
     */
    private static final int TOMBSTONE_ATTEMPTS = 5;

    /**
     * Records a tombstone, retrying on failure.
     *
     * <p>Still asynchronous, because W4 established that this runs on the cluster state thread and a
     * blocking write there deadlocks against the index operation it issues. Retrying is what can be done
     * without blocking.
     *
     * <p><b>What this does not fix.</b> A crash between the cluster state commit and the last attempt
     * landing still loses the tombstone. Closing that needs reconciliation on node join rather than
     * retries: an index whose data is on disk but which appears in neither cluster state nor the descriptor
     * index has been deleted, and a positive check against durable storage is a stronger statement than the
     * local guess {@code IndexGraveyard} was built to avoid. That is a separate mechanism and is tracked
     * rather than half-built here.
     */
    public void putTombstoneAsync(IndexDescriptor tombstone) {
        submitTombstone(tombstone, 1);
    }

    private void submitTombstone(IndexDescriptor tombstone, int attempt) {
        try {
            client.index(
                new IndexRequest(DESCRIPTOR_INDEX).id(tombstone.name()).source(DescriptorCodec.toSource(tombstone)),
                new org.opensearch.core.action.ActionListener<>() {
                    @Override
                    public void onResponse(org.opensearch.action.index.IndexResponse response) {}

                    @Override
                    public void onFailure(Exception e) {
                        if (attempt < TOMBSTONE_ATTEMPTS) {
                            submitTombstone(tombstone, attempt + 1);
                            return;
                        }
                        // Logged at warn rather than swallowed: a lost tombstone means a node may later
                        // adopt this index's dangling shard data, so it is worth an operator seeing.
                        logger.warn(
                            "failed to record tombstone for [{}] after {} attempts; dangling data for this "
                                + "index may be adopted on a node rejoin",
                            tombstone.name(),
                            TOMBSTONE_ATTEMPTS,
                            e
                        );
                    }
                }
            );
        } catch (Exception e) {
            logger.warn("failed to submit tombstone write for [{}]", tombstone.name(), e);
        }
    }

    /**
     * One page of descriptors whose name starts with {@code prefix}, ordered by name.
     *
     * <p>Refresh-bound, per H18, because it is a search. A descriptor written microseconds ago may not
     * appear until the next refresh, which is the documented wildcard contract rather than a defect.
     *
     * @param afterName the last name of the previous page, or null for the first page
     */
    public List<IndexDescriptor> findByPrefix(String prefix, String afterName, int size) {
        try {
            var request = client.prepareSearch(DESCRIPTOR_INDEX)
                .setQuery(QueryBuilders.prefixQuery("name", prefix))
                .addSort("name", SortOrder.ASC)
                .setSize(size);
            if (Strings.isNullOrEmpty(afterName) == false) {
                request.searchAfter(new Object[] { afterName });
            }
            SearchResponse response = request.get();
            List<IndexDescriptor> descriptors = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                descriptors.add(DescriptorCodec.fromSource(hit.getSourceAsMap()));
            }
            return descriptors;
        } catch (Exception e) {
            // Same reasoning as get: no descriptor index means no descriptors, not a failed request.
            logger.debug("descriptor prefix search for [{}] failed", prefix, e);
            return List.of();
        }
    }

    /**
     * One page of names and creation dates, without reading or decoding the rest of the descriptor.
     *
     * <p>Pagination reads exactly these two fields, and T5 measured that building the full record for every
     * hit is roughly a quarter to a third of what a page costs. Both fields are already indexed, so they
     * come back as doc values and {@code _source} is never fetched or parsed.
     *
     * <p>It sits beside {@link #findByPrefix} rather than replacing it because callers that need a whole
     * descriptor still exist. That is a second read path, which is the thing C3 warned about, so the two are
     * kept adjacent and share the paging and failure behaviour rather than being open-coded at the seam that
     * wanted the cheap one.
     *
     * <p>Refresh-bound for the same reason {@link #findByPrefix} is: it is a search.
     *
     * @param afterName the last name of the previous page, or null for the first page
     */
    public List<AbsentIndexDescriptorSuppliers.PagedIndex> findNamesByPrefix(String prefix, String afterName, int size) {
        try {
            var request = client.prepareSearch(DESCRIPTOR_INDEX)
                .setQuery(QueryBuilders.prefixQuery("name", prefix))
                .addSort("name", SortOrder.ASC)
                .setFetchSource(false)
                .addDocValueField("name")
                .addDocValueField("creationDate")
                .setSize(size);
            if (Strings.isNullOrEmpty(afterName) == false) {
                request.searchAfter(new Object[] { afterName });
            }
            SearchResponse response = request.get();
            List<AbsentIndexDescriptorSuppliers.PagedIndex> page = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                page.add(
                    new AbsentIndexDescriptorSuppliers.PagedIndex(
                        hit.field("name").getValue(),
                        ((Number) hit.field("creationDate").getValue()).longValue()
                    )
                );
            }
            return page;
        } catch (Exception e) {
            logger.debug("descriptor name search for [{}] failed", prefix, e);
            return List.of();
        }
    }

    /**
     * Creates the descriptor index if it is not already there.
     *
     * <p>The settings are the contract rather than tuning, and both were established by measurement:
     *
     * <ul>
     *   <li><b>refresh_interval</b> bounds how stale a wildcard may be (H18). It is set explicitly because
     *       leaving it at the default makes an API-visible guarantee depend on an unstated default.</li>
     *   <li><b>merge policy floor and segments per tier</b> bound lookup latency (S29, S30). Segments left
     *       unbounded gave 1.88x per decade; held low, the same decade cost 1.05x. This is the difference
     *       between the descriptor index being a ceiling and not being one.</li>
     * </ul>
     */
    private void ensureIndexExists() {
        if (indexKnownToExist.get()) {
            return;
        }
        try {
            client.admin()
                .indices()
                .create(
                    new CreateIndexRequest(DESCRIPTOR_INDEX).settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            // H18: this is the wildcard staleness bound, stated rather than defaulted.
                            .put("index.refresh_interval", "1s")
                            // S29 and S30: this is the lookup latency bound.
                            .put("index.merge.policy.segments_per_tier", 4)
                            .put("index.merge.policy.max_merge_at_once", 4)
                            .build()
                    ).mapping(DescriptorCodec.MAPPING)
                )
                .actionGet();
            indexKnownToExist.set(true);
        } catch (Exception e) {
            if (e instanceof org.opensearch.ResourceAlreadyExistsException
                || e.getCause() instanceof org.opensearch.ResourceAlreadyExistsException) {
                // The normal case on every node but the first. Concurrent creation is expected, not an error.
                indexKnownToExist.set(true);
                return;
            }
            throw e instanceof RuntimeException runtime ? runtime : new RuntimeException(e);
        }
    }

    /** Whether the descriptor index has been created, which tests assert rather than infer. */
    public boolean indexExists() {
        return client.admin().indices().prepareExists(DESCRIPTOR_INDEX).get().isExists();
    }

    /** The mapping and the document shape, kept beside each other so they cannot drift. */
    static final class DescriptorCodec {

        static final Map<String, Object> MAPPING = Map.of(
            "properties",
            Map.ofEntries(
                Map.entry("name", Map.of("type", "keyword")),
                Map.entry("uuid", Map.of("type", "keyword")),
                Map.entry("shardCount", Map.of("type", "integer")),
                Map.entry("searchOnlyReplicaCount", Map.of("type", "integer")),
                Map.entry("serverless", Map.of("type", "boolean")),
                Map.entry("state", Map.of("type", "keyword")),
                Map.entry("aliases", Map.of("type", "keyword")),
                Map.entry("createdVersion", Map.of("type", "long")),
                Map.entry("system", Map.of("type", "boolean")),
                Map.entry("hidden", Map.of("type", "boolean")),
                Map.entry("remoteSnapshot", Map.of("type", "boolean")),
                Map.entry("warm", Map.of("type", "boolean")),
                Map.entry("mappingGeneration", Map.of("type", "long")),
                // H17 pagination orders by (creationDate, name), so it has to be sortable here.
                Map.entry("creationDate", Map.of("type", "long"))
            )
        );

        static Map<String, Object> toSource(IndexDescriptor d) {
            return Map.ofEntries(
                Map.entry("name", d.name()),
                Map.entry("uuid", d.uuid()),
                Map.entry("shardCount", d.shardCount()),
                Map.entry("searchOnlyReplicaCount", d.searchOnlyReplicaCount()),
                Map.entry("serverless", d.serverless()),
                Map.entry("state", d.state().name()),
                Map.entry("aliases", d.aliases()),
                Map.entry("createdVersion", d.createdVersion()),
                Map.entry("system", d.system()),
                Map.entry("hidden", d.hidden()),
                Map.entry("remoteSnapshot", d.remoteSnapshot()),
                Map.entry("warm", d.warm()),
                Map.entry("mappingGeneration", d.mappingGeneration()),
                Map.entry("creationDate", d.creationDate())
            );
        }

        @SuppressWarnings("unchecked")
        static IndexDescriptor fromSource(Map<String, Object> source) {
            return new IndexDescriptor(
                (String) source.get("name"),
                (String) source.get("uuid"),
                ((Number) source.get("shardCount")).intValue(),
                ((Number) source.get("searchOnlyReplicaCount")).intValue(),
                (Boolean) source.get("serverless"),
                IndexDescriptor.State.valueOf((String) source.get("state")),
                (List<String>) source.getOrDefault("aliases", List.of()),
                ((Number) source.get("createdVersion")).longValue(),
                (Boolean) source.get("system"),
                (Boolean) source.get("hidden"),
                (Boolean) source.get("remoteSnapshot"),
                (Boolean) source.get("warm"),
                ((Number) source.get("mappingGeneration")).longValue(),
                ((Number) source.get("creationDate")).longValue()
            );
        }

        private DescriptorCodec() {}
    }
}
