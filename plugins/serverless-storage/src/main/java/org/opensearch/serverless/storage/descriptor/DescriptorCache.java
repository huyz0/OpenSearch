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
 * It lived inside the index-backed descriptor store, which was fine while that was the only backend. It
 * was extracted when a second one arrived, and every property below was paid for by a measurement a second
 * copy would have had to re-derive. T3's audit put it plainly: duplicating this per backend is two chances
 * to get it subtly different, on the path where being subtly different is invisible. The index-backed store
 * was removed on 2026-08-05 and this outlived it, which is the outcome the extraction was for.
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

    /**
     * How many invalidations have landed on a name while a read of it was outstanding.
     *
     * <p><b>Without this, an invalidation that arrives during a load is erased by that load.</b> {@link
     * #invalidate} removed the cached entry and nothing else, and {@link #load} admitted its result
     * unconditionally, so:
     *
     * <pre>
     * t0  node B misses on "logs-a", takes the in-flight slot, and issues a readRegister
     * t1  node A deletes "logs-a": tombstone durable, live key deleted, change-log entry appended
     * t2  node B's tailer reads the DELETED entry and calls invalidate("logs-a") -- which removes nothing,
     *     because the load has not admitted yet
     * t3  node B's t0 read, issued before the delete, returns the *live* descriptor and admits it
     * t4  for the whole freshness window -- a minute by default -- node B resolves "logs-a" as OPEN and
     *     routes acknowledged writes to a shard whose index is deleted
     * </pre>
     *
     * Every cross-node freshness mechanism in this design ends in {@link #invalidate}: the change tailer,
     * and each of the backend's own write paths. All of them were subject to this, and the sequential
     * version of it is the one case that <em>is</em> covered by a test, which is what made the concurrent
     * version look handled.
     *
     * <p><b>Why a counter per outstanding read rather than a counter per name.</b> A permanent per-name
     * epoch map is a leak: {@link #invalidate} is called for every name that changes anywhere in the
     * cluster, most of which this node has never read and never will. An entry here exists only while at
     * least one load of that name is in flight, so the map's size is bounded by concurrent misses rather
     * than by cluster-wide change volume. {@code readers} is only ever mutated inside a {@link
     * ConcurrentHashMap#compute} on its own key, which is what makes it safe without being volatile.
     */
    private final ConcurrentHashMap<String, LoadGuard> loads = new ConcurrentHashMap<>();

    /** The invalidation counter for one name, alive only while a read of that name is outstanding. */
    private static final class LoadGuard {
        private final AtomicLong invalidations = new AtomicLong();
        private int readers;
    }

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

    /**
     * Reads whose result was thrown away because the name was invalidated while they were in flight.
     *
     * <p>Counted rather than silent because it is the one thing that says the guard is doing something. A
     * zero here across a cluster that deletes indices means either the race genuinely never happens or the
     * guard is not wired to the invalidations that matter, and those are indistinguishable without a
     * number. A large one means invalidation and reads are contending, which is a change-rate problem
     * rather than a cache problem.
     */
    private final AtomicLong staleAdmissionsDropped = new AtomicLong();

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
    /**
     * A per-name jittered freshness window, spreading re-reads that would otherwise land together (T104).
     *
     * <p>Derived from the name's hash rather than from a random number so it is stable for a name across
     * calls and across nodes: a window that moved on every lookup would not spread anything, it would
     * merely make freshness unpredictable.
     *
     * <p><b>The jitter only ever shortens the window; it used to run in both directions.</b> A &plusmn;10%
     * spread makes {@code ttlNanos} neither an upper nor a lower bound on staleness, so a caller that has
     * been told "at most a minute old" can be handed something older, and the invalidation-driven design
     * this cache sits in reasons in exactly those terms. One-sided jitter spreads the herd just as well --
     * what matters is that T names do not expire in the same tick, not which side of the nominal window
     * they land on -- while keeping the configured TTL a real ceiling.
     */
    private long jitteredTtl(String name) {
        if (ttlNanos <= 0) {
            return ttlNanos;
        }
        int hash = name.hashCode();
        double factor = 0.9 + ((hash & Integer.MAX_VALUE) % 100 / 1000.0);
        return (long) (ttlNanos * factor);
    }

    public IndexDescriptor getIfFresh(String name) {
        CachedDescriptor cached = cache.get(name);
        long now = clock.getAsLong();
        if (cached != null && now - cached.readAtNanos() < jitteredTtl(name)) {
            freshHits.incrementAndGet();
            return cached.descriptor();
        }
        return null;
    }

    public IndexDescriptor get(String name, Function<String, IndexDescriptor> loader) {
        // The guard precedes the read, which P3 and P5 both had to learn the hard way: a cache consulted
        // after the expensive call prevents nothing.
        CachedDescriptor cached = cache.get(name);
        long now = clock.getAsLong();
        // Jittered, like getIfFresh, and this is the path where the jitter was actually needed. It was
        // computed for the thundering herd and then applied only to the non-blocking peek, which issues no
        // read: this is the method that goes to the backend, so the mitigation was absent from the only
        // path it could mitigate. A node that warms T tenants in one burst -- a mass reactivation, a
        // prefetch over a wide bulk -- re-read all T of them in the same tick a window later, once per node
        // per window, forever.
        //
        // The second effect matters as much: with the raw TTL here and a jittered one there, the two
        // methods disagreed about whether the same entry was fresh, by up to ten percent in either
        // direction. A cache whose two read paths answer differently for one entry is a cache whose hit
        // rate is not a measurement of anything.
        if (cached != null && now - cached.readAtNanos() < jitteredTtl(name)) {
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
            //
            // Completed exceptionally rather than with null, and that changed when waiters stopped
            // re-reading a shared null (see awaitOrLoadDirectly). While they re-read, a null here cost a
            // wasted round trip; now that they trust it, a null here would report "there is no such index"
            // on the strength of an OutOfMemoryError. Unavailability is the honest answer and the one this
            // package is careful to keep separate from absence.
            mine.completeExceptionally(
                new DescriptorUnavailableException(
                    name,
                    new IllegalStateException("the descriptor read for [" + name + "] did not complete")
                )
            );
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
            // Including a null one, which this used to re-read instead of sharing.
            //
            // Absence is deliberately never cached, so for a name that does not exist collapsing was
            // switched off entirely: every waiter behind the reader issued its own read, and each of those
            // reads costs *two* GETs at the backend (the live prefix, then the tombstone prefix). A request
            // storm against a name that does not exist -- a client retrying against a deleted index, a
            // typo'd name in a hot loop -- was therefore 2 x concurrency GETs with no cache and no
            // collapse, which is the metastable cache-miss cliff this whole design is shaped to avoid.
            //
            // Sharing the null is not caching it: the answer came from a read issued moments ago by a
            // caller that is still here, so a name created since is at most one collapse-window stale,
            // where caching it would have made it a whole freshness window stale. Read-your-writes is
            // preserved because nothing is stored -- the next caller to arrive after this reader is gone
            // reads the store again.
            if (shared != null) {
                collapsedWaits.incrementAndGet();
                return shared;
            }
            collapsedWaits.incrementAndGet();
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Not null. Null means "there is no such descriptor" to every caller of this class, and that is
            // the one answer this whole package is careful to keep separate from "I could not find out" --
            // the distinction DescriptorUnavailableException exists for, because a client told an index is
            // absent may go on to create it. An interrupt says nothing whatsoever about whether the index
            // exists; it says this thread was asked to stop waiting.
            throw new DescriptorUnavailableException(name, e);
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
            // Anything else is still a failure, and it used to be answered with null.
            //
            // Null means "there is no such index" to every caller of this class, and a client acting on
            // that may go on to create an index that already exists -- the one conflation this package's
            // own javadoc says must never happen. Any RuntimeException the loader raises that is not a
            // DescriptorUnavailableException (an NPE on a malformed descriptor, a rejected execution, an
            // IllegalStateException from admission) was being laundered into "no such index" for every
            // waiter behind the reader, and only for waiters: the reader itself already rethrows.
            logger.debug("in-flight descriptor read for [{}] failed: {}", name, e);
            throw new DescriptorUnavailableException(name, e.getCause() == null ? e : e.getCause());
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
     *
     * <p><b>Not covered by {@link #loads}' invalidation guard, and it has no production caller today.</b>
     * The guard has to be taken before the read is issued, and this method is handed a value that was read
     * somewhere else, so there is nothing here to take it around. That is currently harmless because the
     * only prefetcher, {@code BlobDescriptorBackend.warmAsync}, warms by calling {@link #get} per name and
     * is therefore guarded. A future prefetcher that reads in bulk and admits through here would
     * reintroduce the race, and would need to capture the guard around its own read instead.
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

    /**
     * Runs the loader, counts the read, and admits a hit unless the name was invalidated while the loader
     * was running. A miss is deliberately not cached.
     *
     * <p>Every load path in this class goes through here -- the collapsing reader, the waiter that timed
     * out, and the waiter that was handed a null -- so the guard is taken in one place rather than at three
     * call sites, one of which would eventually be added without it. The guard is taken <em>before</em> the
     * loader is called and released after admission, which is the whole of the window {@link #loads}
     * describes.
     *
     * <p>The value is still returned to this caller when its admission is dropped. It is what the store
     * answered, and the caller asked for an answer, not for a cache entry; what must not happen is that
     * answer outliving the invalidation for every later caller.
     */
    private IndexDescriptor load(String name, Function<String, IndexDescriptor> loader, long now) {
        LoadGuard guard = beginLoad(name);
        long invalidationsBefore = guard.invalidations.get();
        try {
            reads.incrementAndGet();
            IndexDescriptor descriptor = loader.apply(name);
            if (descriptor != null) {
                if (guard.invalidations.get() == invalidationsBefore) {
                    admit(name, descriptor, now);
                } else {
                    staleAdmissionsDropped.incrementAndGet();
                    logger.debug("descriptor for [{}] was invalidated while it was being read; not admitting the result", name);
                }
            }
            return descriptor;
        } finally {
            endLoad(name, guard);
        }
    }

    /** Registers this thread as a reader of {@code name}, creating the guard if it is the first. */
    private LoadGuard beginLoad(String name) {
        return loads.compute(name, (key, guard) -> {
            LoadGuard existing = guard == null ? new LoadGuard() : guard;
            existing.readers++;
            return existing;
        });
    }

    /**
     * Releases this thread's claim, removing the guard once the last reader of the name is done.
     *
     * <p>Identity-checked against the guard the caller began with, so a guard already removed and replaced
     * by a later load is not decremented on this load's behalf.
     */
    private void endLoad(String name, LoadGuard guard) {
        loads.computeIfPresent(name, (key, current) -> {
            if (current != guard) {
                return current;
            }
            current.readers--;
            return current.readers <= 0 ? null : current;
        });
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

    /**
     * Drops a cached descriptor, which a write must do so it does not serve its own stale value.
     *
     * <p>The counter is bumped before the entry is removed, and the order is load-bearing: between the two
     * statements an in-flight load may admit, and a load that admits after the bump is dropped by {@link
     * #load}'s own check. The other order leaves that admission both un-dropped and un-removed. See {@link
     * #loads} for the interleaving this closes.
     */
    public void invalidate(String name) {
        loads.computeIfPresent(name, (key, guard) -> {
            guard.invalidations.incrementAndGet();
            return guard;
        });
        CachedDescriptor removed = cache.remove(name);
        if (removed != null) {
            cachedBytes.addAndGet(-removed.bytes());
        }
    }

    /** Drops a batch of cached descriptors during bulk mutations or deletions. */
    public void invalidateAll(Iterable<String> names) {
        if (names == null) {
            return;
        }
        for (String name : names) {
            invalidate(name);
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

    /** Reads whose result was dropped rather than admitted because an invalidation overtook them. */
    public long staleAdmissionsDroppedCount() {
        return staleAdmissionsDropped.get();
    }

    /** How many descriptors are cached, which is what the capacity bounds and what a test checks. */
    public int cachedCount() {
        return cache.size();
    }

    /** How much memory the cached descriptors occupy, which is the bound T4b established has to hold. */
    public long cachedBytes() {
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
