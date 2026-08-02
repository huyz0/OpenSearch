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
import org.opensearch.cluster.metadata.DescriptorUnavailableException;
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
public final class DescriptorStore implements DescriptorBackend, DescriptorPrefixBackend {

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
    /**
     * The read path, extracted in T7 so the blob backend reuses it rather than growing a second copy.
     * Every property in it was paid for by a measurement; see {@link DescriptorCache}.
     */
    private final DescriptorCache descriptorCache;

    private final Client client;
    private final int shardCount;
    private final AtomicBoolean indexKnownToExist = new AtomicBoolean();

    /** One-shot latch for the asynchronous bootstrap, cleared on failure so a transient outage can retry. */
    private final java.util.concurrent.atomic.AtomicReference<java.util.concurrent.CompletableFuture<Void>> bootstrap =
        new java.util.concurrent.atomic.AtomicReference<>();

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
        this.descriptorCache = new DescriptorCache(clock, CACHE_TTL_NANOS, collapseWaitMillis, cacheCapacity, cacheBytes);
    }

    /**
     * Pretends a read for this name is already in flight and will never finish, so a test can prove a
     * waiter falls back rather than hanging behind it.
     */
    java.util.concurrent.CompletableFuture<IndexDescriptor> pretendReadIsInFlight(String name) {
        return descriptorCache.pretendReadIsInFlight(name);
    }

    /** How many times this store has actually gone to the index, which is what a test counts. */
    public long readCount() {
        return descriptorCache.readCount();
    }

    /** How many cached descriptors have been evicted, which distinguishes a small cache from a broken one. */
    public long evictionCount() {
        return descriptorCache.evictionCount();
    }

    /** How many descriptors are cached, which is what the capacity bounds and what a test checks. */
    int cachedCount() {
        return descriptorCache.cachedCount();
    }

    /** How much memory the cached descriptors occupy, which is the bound T4b established has to hold. */
    long cachedBytes() {
        return descriptorCache.cachedBytes();
    }

    /** Drops every cached descriptor, which a write must do so it does not serve its own stale value. */
    public void invalidate(String name) {
        descriptorCache.invalidate(name);
    }

    /**
     * The descriptor for this name, or null if there is none.
     *
     * <p>Realtime, per H18: this is a get by id, so it reads through the translog and sees a descriptor as
     * soon as its write is acknowledged. That is what lets a client create an index and immediately write
     * to it.
     */
    @Override
    public IndexDescriptor get(String name) {
        return descriptorCache.get(name, this::readFromIndex);
    }

    /**
     * Warms the cache for a whole request's names in one round trip, without blocking the caller.
     *
     * <p><b>Asynchronous because the caller is a request thread, and that is not a preference.</b> The first
     * version of C1's prefetcher looped over {@link #get}, which blocks, and it ran inside
     * {@code TransportBulkAction.doExecute}. On a transport worker or the cluster applier thread that trips
     * OpenSearch's "Blocking operation" assertion, and the failure surfaced somewhere else entirely: an
     * {@code AssertionError} captured into the gated creation future and re-thrown at the acknowledgement,
     * so four suites reported a create or delete failing with a thread assertion naming code nowhere near
     * them.
     *
     * <p>One multi-get rather than N gets, which is also what C1 said this was for: a bulk touching M
     * tenants resolves M descriptors together instead of one at a time from inside the document loop.
     *
     * <p>Best effort by contract. Any failure completes the listener successfully, because a request must
     * not fail for want of a warm cache -- whatever did not warm is resolved inline later, slowly.
     */
    @Override
    public void warmAsync(java.util.Collection<String> names, org.opensearch.core.action.ActionListener<Void> listener) {
        if (names.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        try {
            org.opensearch.action.get.MultiGetRequest request = new org.opensearch.action.get.MultiGetRequest();
            for (String name : names) {
                request.add(DESCRIPTOR_INDEX, name);
            }
            client.multiGet(request, new org.opensearch.core.action.ActionListener<>() {
                @Override
                public void onResponse(org.opensearch.action.get.MultiGetResponse response) {
                    for (org.opensearch.action.get.MultiGetItemResponse item : response.getResponses()) {
                        if (item.isFailed() || item.getResponse() == null || item.getResponse().isExists() == false) {
                            continue;
                        }
                        try {
                            descriptorCache.warm(item.getId(), DescriptorCodec.fromSource(item.getResponse().getSourceAsMap()));
                        } catch (Exception e) {
                            logger.debug("could not warm the descriptor for [{}]", item.getId(), e);
                        }
                    }
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("could not prefetch descriptors", e);
                    listener.onResponse(null);
                }
            });
        } catch (Exception e) {
            logger.debug("could not submit the descriptor prefetch", e);
            listener.onResponse(null);
        }
    }

    /**
     * One read of one descriptor from the index, with no caching of its own.
     *
     * <p>T7 moved the freshness window, the eviction and the in-flight collapsing into
     * {@link DescriptorCache}, so what is left here is the part that is specific to reading from an
     * OpenSearch index and nothing else. A backend that forgets to count a read or to admit a hit is now
     * not expressible, because neither is its job.
     */
    private IndexDescriptor readFromIndex(String name) {
        try {
            var response = client.prepareGet(DESCRIPTOR_INDEX, name).get();
            if (response.isExists() == false) {
                // A miss is deliberately not cached, which DescriptorCache enforces by never admitting
                // null. An index created a moment ago must be nameable immediately (H18), and caching
                // absence would delay that by the window.
                return null;
            }
            return DescriptorCodec.fromSource(response.getSourceAsMap());
        } catch (org.opensearch.index.IndexNotFoundException e) {
            // The descriptor index itself does not exist, which means no gated index has ever been created.
            // Absent is the true answer here, and this is the case the original blanket catch was written
            // for: before the first gated creation every resolution would otherwise throw.
            logger.debug("descriptor index absent while resolving [{}]", name, e);
            return null;
        } catch (Exception e) {
            // Anything else means the descriptor index exists and could not be read: a cluster block, no
            // shard available, a timeout. Returning null here would report every gated index in the
            // cluster as non-existent, and a client acting on that could create an index that already
            // exists. "I cannot tell" and "it is not there" have opposite safe responses, so they get
            // different answers.
            logger.warn("could not read the descriptor for [{}]; reporting unavailable rather than absent", name, e);
            throw new org.opensearch.cluster.metadata.DescriptorUnavailableException(name, e);
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

    /**
     * The descriptor index, created once per node, without blocking the caller.
     *
     * <p>{@link #ensureIndexExists} cannot be used from the creation path: it blocks, and W4 established
     * that blocking on the cluster state thread deadlocks, since the index operation it issues needs a
     * cluster state to route and the thread that would supply one is the blocked one.
     *
     * <p><b>This also closes a gap T18 found.</b> Only {@code create} and {@code put} called
     * {@code ensureIndexExists}, and neither has a production caller: the wired path is {@code putAsync},
     * which does not. So in production the descriptor index was auto-created by its first write, without
     * any of W8's contract settings. That silently dropped the merge policy S29 and S30 measured as the
     * difference between 1.05x and 1.88x lookup growth per decade, and the refresh interval H18 made the
     * wildcard staleness bound. The settings were correct, explicit, and never applied.
     *
     * <p>A failed bootstrap clears the latch rather than caching the failure, so a transient outage during
     * the first gated creation in a cluster does not permanently poison every later one.
     */
    private java.util.concurrent.CompletableFuture<Void> ensureIndexExistsAsync() {
        java.util.concurrent.CompletableFuture<Void> existing = bootstrap.get();
        if (existing != null) {
            return existing;
        }
        java.util.concurrent.CompletableFuture<Void> mine = new java.util.concurrent.CompletableFuture<>();
        if (bootstrap.compareAndSet(null, mine) == false) {
            return bootstrap.get();
        }
        try {
            client.admin().indices().create(descriptorIndexRequest(), new org.opensearch.core.action.ActionListener<>() {
                @Override
                public void onResponse(org.opensearch.action.admin.indices.create.CreateIndexResponse response) {
                    indexKnownToExist.set(true);
                    mine.complete(null);
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof org.opensearch.ResourceAlreadyExistsException
                        || e.getCause() instanceof org.opensearch.ResourceAlreadyExistsException) {
                        // The normal case on every node but the first, and on every node after the first
                        // gated creation. Concurrent bootstrap is expected rather than an error.
                        indexKnownToExist.set(true);
                        repairMapping();
                        mine.complete(null);
                        return;
                    }
                    bootstrap.set(null);
                    mine.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            bootstrap.set(null);
            mine.completeExceptionally(e);
        }
        return mine;
    }

    /**
     * Records a descriptor for an index that does not yet exist, without blocking, reporting whether this
     * call created it.
     *
     * <p>This is H3's uniqueness gate on the path H3 designed it for. {@code op_type=create} makes the
     * store rather than the cluster manager decide that a name is taken, so a version conflict completes
     * {@code false} and is a lost race rather than a failure.
     *
     * <p>T23 is why this exists. The creation path routed through {@code publish} to a plain put, so eight
     * concurrent creations of one gated name were all acknowledged: the gate was implemented, tested, and
     * never called. T17 is the other half, since a plain put was also fire and forget, so an acknowledged
     * creation did not mean the descriptor had landed.
     *
     * <p>Non-blocking throughout, so the cluster state thread can issue this and return. The waiting is
     * done by the acknowledgement, which is a different thread and is the one that should wait.
     */
    public java.util.concurrent.CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor) {
        return ensureIndexExistsAsync().thenCompose(ignored -> {
            java.util.concurrent.CompletableFuture<Boolean> created = new java.util.concurrent.CompletableFuture<>();
            try {
                client.index(
                    new IndexRequest(DESCRIPTOR_INDEX).id(descriptor.name()).source(DescriptorCodec.toSource(descriptor)).create(true),
                    new org.opensearch.core.action.ActionListener<>() {
                        @Override
                        public void onResponse(org.opensearch.action.index.IndexResponse response) {
                            invalidate(descriptor.name());
                            created.complete(true);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof VersionConflictEngineException || e.getCause() instanceof VersionConflictEngineException) {
                                // Someone else created this name first. That is the mechanism working.
                                created.complete(false);
                                return;
                            }
                            created.completeExceptionally(e);
                        }
                    }
                );
            } catch (Exception e) {
                created.completeExceptionally(e);
            }
            return created;
        });
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
        // Bootstrapped first, for the same reason createAsync is. A bare index request against a missing
        // descriptor index does not fail, it is auto-created -- with dynamic mappings, because auto-creation
        // knows nothing about DescriptorCodec.MAPPING. The name field then is not a keyword, so every prefix
        // query fails with "No mapping found for [name] in order to sort on" while point reads by id keep
        // working. That is the worst shape a failure can take here: creation, exact-name resolution and
        // writes all succeed, only wildcards break, and they break for the life of the process.
        //
        // This was reachable in production, not only in tests. Whichever write path touched the store first
        // after the index was absent decided its mapping, and putAsync is a first-touch path: it is what the
        // publisher calls, so an ordinary index entering cluster state before any gated index existed was
        // enough to create the store wrong.
        ensureIndexExistsAsync().whenComplete((ignored, bootstrapFailure) -> {
            if (bootstrapFailure != null) {
                logger.warn("could not bootstrap the descriptor index before recording [{}]", descriptor.name(), bootstrapFailure);
                return;
            }
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
        });
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
        // Same bootstrap as putAsync, and for the same reason: a tombstone is also a first-touch path, since
        // deleting an index is a perfectly ordinary first thing for a cluster to do to the descriptor store.
        ensureIndexExistsAsync().whenComplete((ignored, bootstrapFailure) -> submitTombstoneNow(tombstone, attempt));
    }

    private void submitTombstoneNow(IndexDescriptor tombstone, int attempt) {
        try {
            logger.debug("recording tombstone for [{}], attempt [{}]", tombstone.name(), attempt);
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
     * <p><b>Ordered by {@code (creationDate, name)} because that is the order pagination pages in.</b> T27
     * found this sorted by name alone while {@code IndexPaginationStrategy} merged the result by creation
     * date, so a page was selected in one order and returned in another, and the {@code afterCreationDate}
     * the seam passes was accepted and discarded. Sorting here by anything other than the caller's order is
     * not a near miss, it silently drops indices from the walk.
     *
     * <p><b>Both directions are real searches.</b> Descending used to be served by fetching ascending and
     * reversing the page, which reverses the <em>first</em> page rather than producing the last one, so a
     * descending walk returned the same names as an ascending one.
     *
     * <p><b>Tombstones are excluded.</b> H4 records a deletion as a document rather than removing one, so
     * without this filter a deleted index is still listed as an index. That is a state question rather than
     * a paging one, which is why it belongs in the query rather than in the caller: every future reader of
     * this page would otherwise have to remember it, and T27 measured that the first one did not.
     *
     * @param afterName the last name of the previous page, or null for the first page
     * @param afterCreationDate the last creation date of the previous page, ignored when afterName is null
     */
    public List<AbsentIndexDescriptorSuppliers.PagedIndex> findNamesForPage(
        String afterName,
        long afterCreationDate,
        boolean ascending,
        int size
    ) {
        try {
            SortOrder order = ascending ? SortOrder.ASC : SortOrder.DESC;
            var request = client.prepareSearch(DESCRIPTOR_INDEX)
                .setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("state", IndexDescriptor.State.DELETED.name())))
                .addSort("creationDate", order)
                .addSort("name", order)
                .setFetchSource(false)
                .addDocValueField("name")
                .addDocValueField("creationDate")
                // An exact hit total would make the collector visit every descriptor in the population to
                // produce one page, which S41 measured as the difference between a flat cost and a linear
                // one. Nothing here reports a total.
                .setTrackTotalHits(false)
                .setSize(size);
            if (Strings.isNullOrEmpty(afterName) == false) {
                request.searchAfter(new Object[] { afterCreationDate, afterName });
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
            logger.debug("descriptor page search after [{}] failed", afterName, e);
            return List.of();
        }
    }

    /**
     * The gated indices whose name starts with {@code prefix}, or the fact that more than {@code limit} do.
     *
     * <p>This is T28's wildcard expansion, and the {@code limit + 1} it asks for is the whole mechanism.
     * Asking for one more than the cap is how an over-cap pattern is detected without paying for it: the
     * query stops as soon as it has that many, so refusing costs a page rather than a scan. That only holds
     * because {@code descriptorIndexRequest} sorts the index by name and this asks for no hit total, and
     * S41 measured that both are required and neither is sufficient.
     *
     * <p><b>Failure propagates rather than answering empty</b>, unlike every other read here. The others
     * degrade one request; this one decides which indices a request touches, so a swallowed failure turns
     * "the catalogue was unreadable" into "there are no such indices" and the caller acts on the second.
     * T12 created {@link DescriptorUnavailableException} for exactly this distinction and T25 measured what
     * the confident empty answer looks like from a tenant's side.
     *
     * <p>Open and hidden come back with the name because {@code IndicesOptions} filters on them per request
     * while this result is shared. Fetching them costs two doc values inside a page that is already capped.
     *
     * <p>Refresh-bound, per H18, because it is a search. An index created moments ago may not match a
     * wildcard yet, though it resolves immediately by exact name.
     */
    public AbsentIndexDescriptorSuppliers.PrefixExpansion expandPrefix(String prefix, int limit) {
        try {
            var request = client.prepareSearch(DESCRIPTOR_INDEX)
                .setQuery(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.prefixQuery("name", prefix))
                        .mustNot(QueryBuilders.termQuery("state", IndexDescriptor.State.DELETED.name()))
                )
                .addSort("name", SortOrder.ASC)
                .setFetchSource(false)
                .addDocValueField("name")
                .addDocValueField("state")
                .addDocValueField("hidden")
                .setTrackTotalHits(false)
                .setSize(limit + 1);
            SearchResponse response = request.get();
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length > limit) {
                return AbsentIndexDescriptorSuppliers.PrefixExpansion.tooMany(limit);
            }
            List<AbsentIndexDescriptorSuppliers.PrefixMatch> matches = new ArrayList<>(hits.length);
            for (SearchHit hit : hits) {
                // Read into Object rather than inlining. SearchHitField#getValue is generic, so an inlined
                // String.valueOf(...) infers char[] and binds the wrong overload, which compiles and then
                // throws ClassCastException on the first hit.
                Object state = hit.field("state").getValue();
                Object hidden = hit.field("hidden").getValue();
                matches.add(
                    new AbsentIndexDescriptorSuppliers.PrefixMatch(
                        hit.field("name").getValue(),
                        IndexDescriptor.State.OPEN.name().equals(state),
                        Boolean.TRUE.equals(hidden)
                    )
                );
            }
            return AbsentIndexDescriptorSuppliers.PrefixExpansion.of(matches);
        } catch (org.opensearch.index.IndexNotFoundException e) {
            // No descriptor index means no gated indices, which is a real and complete answer rather than
            // an unreadable one. Every cluster is in this state until its first gated index exists.
            forgetIndex();
            return AbsentIndexDescriptorSuppliers.PrefixExpansion.of(List.of());
        } catch (Exception e) {
            // A search that fails for any other reason may be a search against an index that exists without
            // its mapping. Forgetting is not enough on its own: nothing else would run until the next write,
            // and a cluster whose wildcards are broken may not take another write for a long time, so the
            // repair has to be kicked from here. The bootstrap finds the index present and re-applies the
            // mapping, and the caller's retry then succeeds.
            forgetIndex();
            ensureIndexExistsAsync();
            throw new DescriptorUnavailableException(prefix + "*", e);
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
    /**
     * The descriptor index as it must be created, wherever it is created from.
     *
     * <p>Shared by the blocking and asynchronous bootstraps so the two cannot drift. T18 found the
     * settings below had never actually been applied in production, because the only callers of the
     * blocking path have no production caller, so drift here would be invisible in exactly the way that
     * was.
     */
    private CreateIndexRequest descriptorIndexRequest() {
        return new CreateIndexRequest(DESCRIPTOR_INDEX).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                // H18: this is the wildcard staleness bound, stated rather than defaulted.
                .put("index.refresh_interval", "1s")
                // S29 and S30: this is the lookup latency bound.
                .put("index.merge.policy.segments_per_tier", 4)
                .put("index.merge.policy.max_merge_at_once", 4)
                // T28's wildcard bound, and the one setting here that cannot be changed later: index
                // sorting is fixed at creation. S41 measured why it is a contract rather than tuning. A
                // prefix query asking for one page costs 49.7 ms at 800k without it and stays at the 4 ms
                // transport floor with it, because segments stored in name order let the collector stop as
                // soon as it has a page instead of ranking every match. Extrapolated, that is the
                // difference between about six seconds at a hundred million and a flat cost.
                //
                // S41b measured what it costs the write path with arrival order shuffled, which is how
                // descriptors actually arrive: 0.94x, meaning no penalty. That measurement used bulk
                // requests and no reads, which is not this store's workload, so T28b re-measured it under
                // the shape that matters here: 0.87x on single-document writes and 0.95x on realtime GETs
                // by id. No penalty on either, and faster on both.
                .build()
        ).mapping(DescriptorCodec.MAPPING);
    }

    private void ensureIndexExists() {
        if (indexKnownToExist.get()) {
            return;
        }
        try {
            client.admin().indices().create(descriptorIndexRequest()).actionGet();
            indexKnownToExist.set(true);
        } catch (Exception e) {
            if (e instanceof org.opensearch.ResourceAlreadyExistsException
                || e.getCause() instanceof org.opensearch.ResourceAlreadyExistsException) {
                // The normal case on every node but the first. Concurrent creation is expected, not an error.
                indexKnownToExist.set(true);
                repairMapping();
                return;
            }
            throw e instanceof RuntimeException runtime ? runtime : new RuntimeException(e);
        }
    }

    /**
     * Re-applies the mapping to an index that already exists.
     *
     * <p>Guards against the descriptor index existing <em>without</em> its mapping, which is not
     * hypothetical: a descriptor write to a missing index is auto-created by the bulk path, and auto-creation
     * knows nothing about {@link DescriptorCodec#MAPPING}. The index comes back with dynamic mappings, the
     * {@code name} field is not a keyword, and every prefix query then fails with "No mapping found for
     * [name] in order to sort on" while point reads by id keep working.
     *
     * <p>That combination is the bad part. Creation, resolution by exact name and writes all still succeed,
     * so the cluster looks healthy; only wildcards fail, and they fail for as long as the process lives
     * because {@link #indexKnownToExist} is a latch that is never re-examined once set.
     *
     * <p>Idempotent, best effort, and off the critical path: a put-mapping that adds nothing is cheap, and
     * one that cannot be applied must not fail the write that noticed. Found from two ITs that could resolve
     * a gated index by name and could not match it with a wildcard.
     */
    private void repairMapping() {
        try {
            client.admin().indices().preparePutMapping(DESCRIPTOR_INDEX).setSource(DescriptorCodec.MAPPING).execute();
        } catch (Exception e) {
            logger.debug("could not re-apply the descriptor mapping", e);
        }
    }

    /**
     * Forgets that the descriptor index exists, so the next write bootstraps it again.
     *
     * <p>Called when an operation observes the index missing or unusable. Without it the two latches --
     * {@link #indexKnownToExist} and {@link #bootstrap} -- are one-way: once set, an index that is later
     * deleted is never recreated properly, and every subsequent write relies on auto-creation, which is
     * what produces the mapping-less index {@link #repairMapping} then has to fix.
     */
    void forgetIndex() {
        indexKnownToExist.set(false);
        bootstrap.set(null);
    }

    /** Whether the descriptor index has been created, which tests assert rather than infer. */
    public boolean indexExists() {
        return client.admin().indices().prepareExists(DESCRIPTOR_INDEX).get().isExists();
    }

    /**
     * {@inheritDoc}
     *
     * <p>For this backend readiness is the descriptor index existing, so this is {@link #indexExists}
     * under the name the interface uses. The two are kept separate rather than renamed because tests
     * assert on the mechanism deliberately, and a backend with no index to create still has to answer
     * the question.
     */
    @Override
    public boolean available() {
        return indexExists();
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
                Map.entry("creationDate", d.creationDate()),
                Map.entry("routingNumShards", d.routingNumShards()),
                Map.entry("routingPartitionSize", d.routingPartitionSize())
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
                ((Number) source.get("creationDate")).longValue(),
                // Tolerant, because descriptors written before these fields existed are still in the index
                // and must keep reading as "never resharded, not partitioned" rather than failing.
                ((Number) source.getOrDefault("routingNumShards", 0)).intValue(),
                ((Number) source.getOrDefault("routingPartitionSize", 0)).intValue()
            );
        }

        private DescriptorCodec() {}
    }
}
