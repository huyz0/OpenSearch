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
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.cluster.metadata.DescriptorUnavailableException;
import org.opensearch.cluster.metadata.IndexDescriptor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * The read path in front of a {@link DescriptorBackend}: a bounded cache with a freshness window and
 * in-flight request collapsing.
 *
 * <h2>Why this is its own class</h2>
 *
 * It was inside {@code DescriptorStore}, which was fine while there was one backend. There are about to be
 * two, and every property below was paid for by a measurement that a second copy would have to re-derive.
 * T3's audit put it plainly: duplicating this per backend is two chances to get it subtly different, on
 * the path where being subtly different is invisible.
 *
 * <p>It also matters much more against an object store than it did against a system index. A miss went
 * from about half a millisecond to a network round trip, so collapsing stopped being an optimisation:
 * without it, M concurrent readers of one cold descriptor are M round trips, and T1 measured sixty-four
 * concurrent resolutions of a single name issuing sixty-four reads before collapsing existed.
 *
 * <h2>What was kept exactly as it was, and why</h2>
 *
 * Every comment below is load-bearing history rather than description, so the behaviour is carried over
 * unchanged rather than tidied on the way past:
 *
 * <ul>
 *   <li>Admission is unconditional. Gating it on {@code size() < capacity} is not an eviction policy but a
 *       freeze, and T3 measured what that did: the first {@code capacity} names held every slot forever,
 *       expired entries were re-read and never re-admitted, and twenty names cost twenty reads on every
 *       pass.</li>
 *   <li>Recency is stamped on write and never touched on read, the structure P2 established after P1
 *       measured a read-mutating LRU running backwards under contention.</li>
 *   <li>Two clocks per entry. A wall clock cannot pick the stalest entry, because entries admitted within
 *       one {@code nanoTime} tick share a value and a threshold taken from equal stamps evicts nothing.</li>
 *   <li>An entry larger than the whole budget is not cached at all, because it could never be evicted down
 *       to fit and would leave the cache permanently over budget, scanning on every admission forever.</li>
 *   <li>A miss is never cached. An index created a moment ago has to be nameable immediately (H18), and
 *       caching absence would delay that by the whole window.</li>
 * </ul>
 *
 * <h2>The constant that does not survive the move to an object store</h2>
 *
 * {@link #DEFAULT_TTL_NANOS} is one second, chosen against a sub-millisecond local read. Against a round
 * trip it means a descriptor read more than once a second re-reads every second, per active tenant per
 * node, whatever the request rate above it. The answer is a long window plus explicit invalidation through
 * {@link #invalidate}, driven by the descriptor change log, rather than a larger number picked by feel. It
 * is a constructor parameter for that reason.
 */
public final class DescriptorCache {

    private static final Logger logger = LogManager.getLogger(DescriptorCache.class);

    /** See the class javadoc: right for a local index, wrong by two orders of magnitude for an object store. */
    public static final long DEFAULT_TTL_NANOS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Entry count, retained as a secondary guard rather than as the bound.
     *
     * <p>T4b measured a descriptor with twenty plain aliases at 5.8 times a typical one and two hundred
     * aliases at 51.6 times, so an entry count admits a fiftyfold range of memory and cannot be the thing
     * defending the residency ceiling.
     */
    public static final int DEFAULT_CAPACITY = 50_000;

    /** The bound that actually holds. Roughly what fifty thousand typical descriptors occupy. */
    public static final long DEFAULT_BYTES = 32L * 1024 * 1024;

    /**
     * How long a caller waits for another caller's in-flight read before going to the backend itself.
     *
     * <p>Bounds a pathological wait rather than racing a healthy one, so that collapsing does not turn one
     * hung reader into every reader behind it hanging too. Worth re-deriving for an object store: it was
     * chosen against T1's 2.7 ms uncontended read, and against a fifty millisecond one the fallback
     * essentially never fires, so the blast-radius protection it exists for is no longer there.
     */
    public static final long DEFAULT_COLLAPSE_WAIT_MILLIS = 3_000;

    /** Charged per entry for the map node holding it, so a population of tiny descriptors is still bounded. */
    private static final long ENTRY_OVERHEAD_BYTES = 64;

    /**
     * A cached descriptor, carrying two independent clocks: {@code readAtNanos} answers "is this still
     * fresh" and {@code admittedAt} answers "is this the stalest thing here".
     */
    private record CachedDescriptor(IndexDescriptor descriptor, long readAtNanos, long admittedAt, long bytes) {
    }

    private final ConcurrentHashMap<String, CachedDescriptor> cache = new ConcurrentHashMap<>();

    /**
     * The name currently being read, so concurrent misses on it wait rather than each issuing a read.
     *
     * <p>An entry lives only for the duration of one read, which is why this needs no capacity bound while
     * {@link #cache} does.
     */
    private final ConcurrentHashMap<String, CompletableFuture<IndexDescriptor>> inFlight = new ConcurrentHashMap<>();

    private final LongSupplier clock;
    private final long ttlNanos;
    private final long collapseWaitMillis;
    private final int capacity;
    private final long maxBytes;

    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private final AtomicLong admissions = new AtomicLong();
    private final AtomicLong cachedBytes = new AtomicLong();

    /**
     * Lookups served without touching the backend, split by how.
     *
     * <p>{@code reads} alone cannot give a hit rate, because it counts backend trips rather than lookups,
     * and it cannot distinguish the two ways a lookup avoids one. That distinction is the point: a fresh
     * hit costs nothing, and a collapsed wait costs a full round trip that somebody else is paying. A
     * cache reporting 99% "hits" where most of them are threads blocked behind one cold read is not a
     * cache that is working, and against a system index the difference was half a millisecond so nobody
     * had to care. Against an object store it is the difference between a served request and a stalled
     * one.
     */
    private final AtomicLong freshHits = new AtomicLong();
    private final AtomicLong collapsedWaits = new AtomicLong();
    private final AtomicLong collapseFallbacks = new AtomicLong();

    public DescriptorCache() {
        this(System::nanoTime, DEFAULT_TTL_NANOS, DEFAULT_COLLAPSE_WAIT_MILLIS, DEFAULT_CAPACITY, DEFAULT_BYTES);
    }

    public DescriptorCache(LongSupplier clock, long ttlNanos, long collapseWaitMillis, int capacity, long maxBytes) {
        this.clock = clock;
        this.ttlNanos = ttlNanos;
        this.collapseWaitMillis = collapseWaitMillis;
        this.capacity = capacity;
        this.maxBytes = maxBytes;
    }

    /**
     * The descriptor for a name, from memory if it is fresh and from {@code loader} otherwise.
     *
     * <p>The loader is handed the name and returns the descriptor or null, and does not admit anything
     * itself: admission, eviction and the read counter all belong here, so a backend cannot forget one.
     *
     * @throws DescriptorUnavailableException if the loader could not tell whether the descriptor exists
     */
    public IndexDescriptor get(String name, Function<String, IndexDescriptor> loader) {
        // The guard precedes the read, which P3 and P5 both had to learn the hard way: a cache consulted
        // after the expensive call prevents nothing.
        CachedDescriptor cached = cache.get(name);
        long now = clock.getAsLong();
        if (cached != null && now - cached.readAtNanos() < ttlNanos) {
            freshHits.incrementAndGet();
            return cached.descriptor();
        }

        // The window above only helps a caller arriving after some other caller finished reading. Callers
        // arriving together all see the same empty slot, and T1 measured that they all go to the backend:
        // sixty-four concurrent resolutions of one name issued sixty-four reads. So one of them reads and
        // the rest wait on it.
        CompletableFuture<IndexDescriptor> mine = new CompletableFuture<>();
        CompletableFuture<IndexDescriptor> reader = inFlight.putIfAbsent(name, mine);
        if (reader != null) {
            return awaitOrLoadDirectly(reader, name, loader);
        }
        try {
            IndexDescriptor descriptor = load(name, loader, now);
            mine.complete(descriptor);
            return descriptor;
        } catch (RuntimeException e) {
            // Waiters must inherit the failure rather than the null it would otherwise become, or
            // collapsing would convert one node's unavailability back into "absent" for every caller
            // behind it, which is the whole point of DescriptorUnavailableException.
            mine.completeExceptionally(e);
            throw e;
        } finally {
            // Completing an already-completed future is a no-op, so this only fires if the load threw an
            // Error. Without it a waiter would block until its own timeout for no reason.
            mine.complete(null);
            inFlight.remove(name, mine);
        }
    }

    /**
     * Waits for the caller already reading this name, and reads directly if that takes too long.
     *
     * <p>The fallback is the point. Collapsing turns N independent reads into one read with N-1 threads
     * depending on it, which is a new way to fail: before, a caller that hung hung alone. A bounded wait
     * keeps the failure blast radius what it was, at the cost of occasionally issuing the read this exists
     * to avoid.
     */
    private IndexDescriptor awaitOrLoadDirectly(
        CompletableFuture<IndexDescriptor> reader,
        String name,
        Function<String, IndexDescriptor> loader
    ) {
        try {
            IndexDescriptor shared = reader.get(collapseWaitMillis, TimeUnit.MILLISECONDS);
            if (shared != null) {
                collapsedWaits.incrementAndGet();
                return shared;
            }
            return load(name, loader, clock.getAsLong());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (TimeoutException e) {
            collapseFallbacks.incrementAndGet();
            logger.debug("waited [{}] ms on an in-flight descriptor read for [{}]; reading directly", collapseWaitMillis, name);
            // The clock is read again rather than reused from before the wait. A timestamp taken up to
            // collapseWaitMillis ago would stamp the entry as already expired against the window, so the
            // cache would be populated with something nothing can ever read.
            return load(name, loader, clock.getAsLong());
        } catch (ExecutionException e) {
            // The reader failed. Unavailability propagates, since a waiter is no better placed to claim
            // the index is absent than the reader was.
            if (e.getCause() instanceof DescriptorUnavailableException unavailable) {
                throw unavailable;
            }
            logger.debug("in-flight descriptor read for [{}] failed", name, e);
            return null;
        }
    }

    /**
     * Admits a descriptor that was read somewhere else, counting the read.
     *
     * <p>For the prefetch path, which resolves a whole request's names in one round trip and so cannot go
     * through {@link #get}: that takes a loader and calls it synchronously, which is exactly what the
     * prefetcher must not do. The read is counted here because it happened, and leaving it uncounted would
     * make the hit rate this cache reports flattering rather than true -- warmed names would look like hits
     * with no read behind them.
     */
    public void warm(String name, IndexDescriptor descriptor) {
        if (descriptor == null) {
            // Same rule as load: absence is never cached, so a name created a moment ago stays resolvable
            // immediately rather than after the window.
            return;
        }
        reads.incrementAndGet();
        admit(name, descriptor, clock.getAsLong());
    }

    /** Runs the loader, counts the read, and admits a hit. A miss is deliberately not cached. */
    private IndexDescriptor load(String name, Function<String, IndexDescriptor> loader, long now) {
        reads.incrementAndGet();
        IndexDescriptor descriptor = loader.apply(name);
        if (descriptor != null) {
            admit(name, descriptor, now);
        }
        return descriptor;
    }

    /**
     * Caches a descriptor, evicting the stalest entries if that puts the cache over its bound.
     *
     * <p><b>The admission is unconditional, which T3 is the reason for.</b> This used to be gated on
     * {@code cache.size() < capacity}, and since nothing ever removed an entry, that was not an eviction
     * policy but a freeze. The first {@code capacity} names held every slot permanently, and because an
     * expired entry was re-read and then not re-admitted, every entry went stale after one window and
     * could never be refreshed. Past the bound the cache held its memory and served nothing: T3 measured
     * twenty names costing twenty reads on every pass, forever.
     *
     * <p>Recency is stamped on write and never touched on read, which is the structure P2 established for
     * the suspension registry after P1 measured a read-mutating LRU running backwards under contention.
     */
    private void admit(String name, IndexDescriptor descriptor, long now) {
        long bytes = retainedSizeOf(name, descriptor);
        if (bytes > maxBytes) {
            // An entry that alone exceeds the budget can never be evicted down to fit, so admitting it
            // would leave the cache permanently over budget and run the eviction scan on every subsequent
            // admission forever. That is the shape T4 caught once already.
            logger.debug("descriptor for [{}] is {} bytes, above the whole cache budget of {}; not cached", name, bytes, maxBytes);
            return;
        }
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
        long bytes = ENTRY_OVERHEAD_BYTES + RamUsageEstimator.shallowSizeOfInstance(IndexDescriptor.class) + RamUsageEstimator.sizeOf(name)
            + RamUsageEstimator.sizeOf(descriptor.uuid());
        return bytes + RamUsageEstimator.sizeOfCollection(descriptor.aliases());
    }

    /**
     * Drops the stalest tenth when the cache is over its bound.
     *
     * <p>A threshold from one pass over the stamps rather than a sorted eviction list, matching
     * {@code GatedShardSuspensionRegistry}. Evicting a tenth rather than a single entry keeps this from
     * running on every admission once the cache is full.
     */
    private void evictIfOverCapacity() {
        if (cache.size() <= capacity && cachedBytes.get() <= maxBytes) {
            return;
        }
        // Repeated because one pass drops a tenth of the entries, which is a tenth of the count but an
        // unknown share of the bytes. A single pass would leave a cache of large descriptors over budget.
        // Bounded so a concurrent writer refilling as fast as this drains cannot spin here forever.
        for (int pass = 0; pass < 10 && (cache.size() > capacity || cachedBytes.get() > maxBytes); pass++) {
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

    /** Drops a cached descriptor, which a write must do so it does not serve its own stale value. */
    public void invalidate(String name) {
        CachedDescriptor removed = cache.remove(name);
        if (removed != null) {
            cachedBytes.addAndGet(-removed.bytes());
        }
    }

    /** How many times this cache has actually gone to the backend, which is what a test counts. */
    public long readCount() {
        return reads.get();
    }

    /** How many cached descriptors have been evicted, which distinguishes a small cache from a broken one. */
    public long evictionCount() {
        return evictions.get();
    }

    /** Lookups answered from memory with no wait and no backend trip. */
    public long freshHitCount() {
        return freshHits.get();
    }

    /**
     * Lookups answered by waiting on somebody else's in-flight read.
     *
     * <p>Counted apart from {@link #freshHitCount} because it is not the same outcome. This is a saved
     * round trip rather than an avoided one: the caller still waited for the network, it just did not add
     * traffic. Folding the two together would report a cache as healthy while every request stalls.
     */
    public long collapsedWaitCount() {
        return collapsedWaits.get();
    }

    /**
     * Times a waiter gave up on the in-flight read and went to the backend itself.
     *
     * <p>The one number here that should stay near zero. It rising means the collapse wait is shorter than
     * a real read takes, which turns collapsing into duplicated work: every waiter times out and then
     * issues the read it was waiting to avoid. That is the specific way the 3-second default becomes wrong
     * against an object store, and it is invisible without this counter.
     */
    public long collapseFallbackCount() {
        return collapseFallbacks.get();
    }

    /**
     * Lookups served without a backend trip, as a fraction of all lookups, or NaN before any lookup.
     *
     * <p>The number the blob-backed design rests on, and it is stated as a derived value rather than a
     * stored one so it cannot drift from the counters it comes from.
     */
    public double hitRate() {
        long hits = freshHits.get() + collapsedWaits.get();
        long total = hits + reads.get();
        return total == 0 ? Double.NaN : (double) hits / total;
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
     * Pretends a read for this name is already in flight and will never finish, so a test can prove a
     * waiter falls back rather than hanging behind it.
     */
    CompletableFuture<IndexDescriptor> pretendReadIsInFlight(String name) {
        CompletableFuture<IndexDescriptor> never = new CompletableFuture<>();
        inFlight.put(name, never);
        return never;
    }
}
