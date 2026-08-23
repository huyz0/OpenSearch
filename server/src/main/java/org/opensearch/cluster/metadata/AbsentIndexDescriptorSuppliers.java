/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Node-level hook for resolving an index that has no entry in cluster state metadata.
 *
 * <p>The gated-index design's premise is that cluster state should hold no entry per index, which means
 * resolution has to answer from somewhere else. This is that seam, and it is deliberately the same shape as
 * its routing-plane counterpart {@code AbsentIndexRoutingSuppliers}: try the published structure first,
 * fall back to a supplier, and behave exactly as before when nothing is installed.
 *
 * <p><b>Why copy that shape rather than design a new one.</b> The routing seam was applied across nine call
 * sites and the failures it produced are known and documented: the same absence has to mean the same thing
 * everywhere, the fast path has to be identical rather than merely equivalent, and a caller that silently
 * resolves nothing is worse than one that throws. Reusing the shape inherits those lessons instead of
 * rediscovering them.
 *
 * <p><b>What this does not do.</b> It does not make resolution asynchronous. A descriptor lookup is a
 * realtime GET against one shard, measured flat at about 0.8 ms up to fifty thousand descriptors,
 * but it is still a remote call where a map read was local. Every caller of this seam is on a path that
 * already does remote work, and a supplier that blocks a transport thread would be a worse problem than
 * the one this solves. The supplier is therefore expected to answer from a cache and to decline rather
 * than block when it cannot -- and, since expectation is not enforcement, {@link
 * #THREADS_WHERE_BLOCKING_IS_UNSAFE} now names the event-loop threads as well, so a supplier that would
 * have blocked one is never asked to.
 *
 * <p>Unset by default, so an ordinary cluster resolves exactly as it always has.
 *
 * <p><b>Settled architecture: one authority per plane.</b> This static registry is the node-level
 * <em>authority</em> for descriptor-backed metadata -- the single place a plugin's answers live, and the
 * seam the few core internals that ask node-level or descriptor-vocabulary questions consult directly.
 * The {@code ClusterPlugin} SPI ({@link IndexCatalog}, implemented for this registry by {@link
 * SupplierBackedIndexCatalog}) is the registration <em>front door</em>: how a plugin installs
 * into this authority, not a second authority. The catalog registered on the node is the
 * <em>read path</em> ({@code Metadata#indexOrResolved}, {@code
 * Metadata#existsOrResolved}), and it answers by forwarding here. The direct static call sites that
 * remain in core -- each carrying its own comment -- are permanent by design, not a pending migration:
 * they need descriptor-level vocabulary the SPI deliberately does not expose (uuid, the three-way
 * live/tombstoned/unknown distinction, prefix expansion). The other reason such call sites used to exist --
 * "is the feature currently active on this node" -- is gone: {@link IndexCatalog#isActive()} now models it,
 * and the four call sites that asked it here have migrated onto that.
 * Its routing-plane counterpart, {@code org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers},
 * follows exactly the same shape.
 */
public final class AbsentIndexDescriptorSuppliers {

    private static final Logger logger = LogManager.getLogger(AbsentIndexDescriptorSuppliers.class);

    private static final AtomicReference<Function<String, IndexDescriptor>> SUPPLIER = new AtomicReference<>();
    private static final AtomicReference<Function<String, IndexDescriptor>> CACHED_SUPPLIER = new AtomicReference<>();

    /**
     * Threads on which a supplier must not be asked to do I/O.
     *
     * <p>The deadlock is established by measurement: a remote lookup made from the cluster state applier thread needs
     * that thread to make progress before it can complete, so it waits on itself. The cluster manager's
     * update thread is the same shape and is additionally the one serialised thread this whole design
     * exists to keep out of the request path.
     *
     * <p>An audit enumerated six call sites that reach a supplier from one of these: {@code DanglingIndicesState},
     * {@code IndicesClusterStateService} twice, {@code RoutingNodes} through
     * {@code ClusterState.getRoutingNodes}, {@code ClusterStateHealth} by way of {@code AllocationService},
     * and {@code ActiveShardCount}. Six is few enough to fix one at a time and too many to keep correct by
     * discipline, because every future core change that reads metadata on one of these threads reopens the
     * hole and the failure only appears on a cache miss, which is the case testing does not produce.
     *
     * <p>So this makes it impossible rather than forbidden. A warm descriptor still answers, because that
     * costs no I/O and never reaches a supplier that would block. A cold one degrades -- but to what
     * depends on which thread refused, and the difference matters: on a cluster-state thread it degrades
     * to the absence the seam already models everywhere (that trade is long-standing and those call sites
     * have no second tier), while on an event-loop thread it raises {@link DescriptorUnavailableException}.
     * Answering "absent" there would report a live index as missing to a client and invite it to create
     * over the top; "unavailable" says what actually happened and is retryable. See {@link #supply(String)}.
     *
     * <p><b>The event-loop threads are here for a different reason than the cluster-state threads, and it
     * is not deadlock.</b> This class's own javadoc already states the rule -- "a supplier that blocks a
     * transport thread would be a worse problem than the one this solves" -- but nothing enforced it. The
     * resolver's descriptor branch and {@link Metadata#indexOrResolved} are reached synchronously from the
     * coordinating thread of a bulk or search request, which is ordinarily an {@code http_server_worker} or
     * {@code transport_worker} event loop. A cold descriptor there is one or two object-store round trips
     * with retries and sleep-backoff, or up to a three-second wait to collapse onto an in-flight read. An
     * event-loop thread is shared by every connection multiplexed onto it, so one such stall stalls all of
     * them -- and there are only a handful of these threads per node, so a few concurrent cold lookups take
     * the node's whole network layer down for the duration. Core already treats these two names as
     * "never block here" ({@code Transports#isTransportThread}, used by assertions across the codebase);
     * this makes the descriptor seam honour that rather than assert about it.
     *
     * <p>Matched by the same substring rule as the rest of this list; the two strings are the values of
     * {@code HttpServerTransport#HTTP_SERVER_WORKER_THREAD_NAME_PREFIX} and
     * {@code TcpTransport#TRANSPORT_WORKER_THREAD_NAME_PREFIX}, written out rather than imported to keep
     * this static seam free of a dependency on the transport packages -- the same reason the cluster-state
     * thread names above are literals rather than references to the services that own them.
     */
    private static final String[] THREADS_WHERE_BLOCKING_IS_UNSAFE = {
        "clusterApplierService#updateTask",
        "clusterManagerService#updateTask",
        "masterService#updateTask",
        "http_server_worker",
        "transport_worker" };

    /**
     * Whether the calling thread is one a supplier must not block.
     *
     * <p>Matched on thread name, which is how {@code ClusterService} already asserts the same property. It
     * is a weaker check than holding a reference to the executor, and it is the one available to a static
     * seam that has no services injected into it.
     */
    static boolean blockingIsUnsafeHere() {
        String threadName = Thread.currentThread().getName();
        for (String unsafe : THREADS_WHERE_BLOCKING_IS_UNSAFE) {
            if (threadName.contains(unsafe)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the calling thread is one of the two network event loops, as opposed to a cluster-state
     * thread. Both are unsafe to block, but they differ in what a refusal may answer: see
     * {@link #supply(String)}.
     */
    static boolean onEventLoopThread() {
        String threadName = Thread.currentThread().getName();
        return threadName.contains("http_server_worker") || threadName.contains("transport_worker");
    }

    private AbsentIndexDescriptorSuppliers() {}

    /**
     * Installs the supplier. Registering null clears it, which is what lets a test restore the default
     * rather than leaking a supplier into unrelated cases.
     */
    public static void register(Function<String, IndexDescriptor> supplier) {
        SUPPLIER.set(supplier);
    }

    public static void registerCached(Function<String, IndexDescriptor> cachedSupplier) {
        CACHED_SUPPLIER.set(cachedSupplier);
    }

    /** Whether anything is installed, so callers can skip work that would be discarded. */
    public static boolean isRegistered() {
        return SUPPLIER.get() != null;
    }

    /**
     * The descriptor for a name with no metadata entry, or null to fall back to the existing behaviour.
     *
     * <p>A supplier that throws is treated as having no answer rather than being allowed to fail the
     * request, for the reason the routing seam settled: this is a degradation path already, and turning a
     * plugin bug into a request failure makes the absence worse than it was.
     */
    public static IndexDescriptor supply(String indexName) {
        if (indexName == null) {
            return null;
        }
        if (blockingIsUnsafeHere()) {
            Function<String, IndexDescriptor> cached = CACHED_SUPPLIER.get();
            IndexDescriptor warm = cached == null ? null : cached.apply(indexName);
            if (warm != null) {
                return warm;
            }
            logger.debug("refusing to resolve the descriptor for [{}] on {}", indexName, Thread.currentThread().getName());
            if (isRegistered() && onEventLoopThread()) {
                // Refusing to block is right; answering "absent" is not. On an event-loop thread a cold
                // descriptor means we declined to find out, and null is this seam's word for "does not
                // exist" -- so returning it here would turn a busy network thread into a spurious
                // IndexNotFoundException for an index that is perfectly alive, and invite the caller to
                // create over the top of it. That is the exact confusion DescriptorUnavailableException
                // exists to prevent, and it already reports as SERVICE_UNAVAILABLE, which is the correct
                // client response: retry, and by then the descriptor is likely warm.
                //
                // The cluster-state threads above keep answering null deliberately. Their null is
                // load-bearing and long-standing: those two consultation points have no second tier to
                // fall back on, so throwing there would fail every gated index on every state update
                // rather than degrade one lookup.
                throw new DescriptorUnavailableException(indexName, null);
            }
            return null;
        }
        Function<String, IndexDescriptor> supplier = SUPPLIER.get();
        if (supplier == null) {
            return null;
        }
        try {
            return supplier.apply(indexName);
        } catch (DescriptorUnavailableException e) {
            logger.warn("Descriptor unavailable for [{}] on Object Storage: {}", indexName, e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.debug("supplier threw for [{}]: {}", indexName, e);
            return null;
        }
    }

    /**
     * Whether a name resolves to something that exists, consulting metadata first and the supplier only
     * on a miss.
     *
     * <p>The order is what keeps an ordinary cluster unchanged: a name present in metadata never reaches
     * the supplier, so no cluster that has not opted in pays a remote lookup. A tombstoned descriptor
     * answers false, which is what makes deletion durable without an {@code IndexGraveyard}.
     */
    public static boolean exists(Metadata metadata, String indexName) {
        if (metadata.getIndicesLookup().containsKey(indexName)) {
            return true;
        }
        if (isRegistered() == false) {
            return false;
        }
        IndexDescriptor descriptor = supply(indexName);
        return descriptor != null && descriptor.exists();
    }

    /**
     * One gated index that matched a prefix, carrying only what wildcard expansion filters on.
     *
     * <p>Three fields rather than an {@link IndexDescriptor}, because measurement showed that building the full
     * fourteen field record for every hit is a quarter to a third of what an expansion costs and only three
     * are read. Three rather than one because expansion is not just a name lookup. {@code IndicesOptions}
     * decides
     * whether closed indices and hidden indices are in the answer, so an expander returning bare names
     * forces the caller either to ignore those options or to fetch each descriptor to honour them. The first
     * is wrong and the second reintroduces the cost the cap exists to avoid.
     */
    public record PrefixMatch(String name, boolean open, boolean hidden) {
    }

    /**
     * The result of expanding one prefix: the matches, or the fact that there are too many.
     *
     * <p>The two are a single type because they are one answer with two shapes, and collapsing them into an
     * empty list or a truncated one is precisely the failure this area keeps shipping. A wildcard that
     * matched five thousand tenants and returned the first hundred reads as a complete answer, and a tenant
     * has no way to tell it was not.
     *
     * @param matches the matching gated indices, empty when the limit was exceeded
     * @param exceeded whether more than {@code limit} indices matched, so no answer is being given
     * @param limit the cap that was applied, carried so an error can name it
     */
    public record PrefixExpansion(List<PrefixMatch> matches, boolean exceeded, int limit, String limitSettingName) {

        public static PrefixExpansion of(List<PrefixMatch> matches) {
            return new PrefixExpansion(matches, false, -1, null);
        }

        /**
         * {@code limitSettingName} lets the
         * expander name its own setting in the refusal a user reads. Core used to interpolate one specific
         * plugin's setting key ({@code serverless_storage.wildcard.max_expanded_indices}) into {@code
         * UnsupportedWildcardException}'s message, which is core naming a plugin's vocabulary. Nullable:
         * an expander that would rather not name a setting gets a message that simply omits the sentence.
         */
        public static PrefixExpansion tooMany(int limit, String limitSettingName) {
            return new PrefixExpansion(List.of(), true, limit, limitSettingName);
        }
    }

    /**
     * Expands a prefix over gated indices.
     *
     * <p>Prefix rather than pattern, and that is the contract rather than an implementation detail. The
     * descriptor store is keyed by name and lists in that order, so a prefix is a bounded listing and
     * anything else is a scan of the whole population. An expander is never asked to answer {@code *-logs}, because at a hundred million
     * indices there is no answer to give.
     *
     * <p>The cap lives in the implementation rather than here, so the policy stays with the component that
     * knows the cluster's settings and core carries no constant it cannot justify.
     */
    @FunctionalInterface
    public interface DescriptorPrefixExpander {
        /**
         * The gated indices whose name starts with {@code prefix}, or the fact that too many do.
         *
         * @param prefix the pattern with its trailing star removed, which may be empty for match-all
         */
        PrefixExpansion expand(String prefix);
    }

    private static final AtomicReference<DescriptorPrefixExpander> EXPANDER = new AtomicReference<>();

    /** Installs the expander. Registering null clears it, which is how a test restores the default. */
    public static void registerExpander(DescriptorPrefixExpander expander) {
        EXPANDER.set(expander);
    }

    /**
     * Whether wildcards are answered over gated indices at all.
     *
     * <p>Callers must ask this rather than infer it from an empty expansion, because the two mean opposite
     * things: nothing installed is an ordinary cluster where wildcards behave as they always have, and an
     * empty expansion is a gated cluster where the pattern genuinely matched nothing.
     */
    public static boolean isExpanderRegistered() {
        return EXPANDER.get() != null;
    }

    /**
     * Expands {@code prefix}, or returns null when nothing is installed.
     *
     * <p><b>A failing expander propagates rather than answering empty</b>, which is the opposite of how
     * {@link #supply} treats failure and is deliberate. That one degrades a request; this
     * one decides which indices a request touches, so a swallowed failure turns "I could not read the
     * catalogue" into "there are no such indices" and the caller acts on the second. That distinction is
     * what {@link DescriptorUnavailableException} was created for.
     */
    public static PrefixExpansion expandPrefix(String prefix) {
        DescriptorPrefixExpander expander = EXPANDER.get();
        if (expander == null || prefix == null) {
            return null;
        }
        return expander.expand(prefix);
    }

    /**
     * The {@link IndexMetadata} synthesised from a gated index's descriptor, or null if there is none.
     *
     * <p><b>Cached, and the cache is for stability rather than for speed.</b> {@code
     * AbsentIndexRoutingSuppliers} memoises placement on metadata <em>identity</em>, so a fresh instance per
     * call makes that memo miss every time, which was measured as an 18x regression rather than a slow path.
     * Keyed on descriptor identity, so a descriptor that changes invalidates it.
     *
     * <p>One cache rather than one per caller, which is the reason this lives here and not beside each
     * user. The routing seam and the write path both synthesise metadata for the same index on the same
     * request, and two caches would hand them two instances that compare equal and are not the same object.
     * The routing memo would then miss on every write, and worse, {@code IndexShard} identity checks
     * comparing metadata across the two paths would disagree about an index nothing had changed.
     */
    public static IndexMetadata synthesisedMetadata(String indexName) {
        IndexDescriptor descriptor = supply(indexName);
        if (descriptor == null || descriptor.exists() == false) {
            return null;
        }
        SynthesisedMetadata cached = SYNTHESISED.get(indexName);
        if (cached != null && cached.descriptor == descriptor) {
            return cached.metadata;
        }
        IndexMetadata metadata = descriptor.toIndexMetadata();
        SYNTHESISED.put(indexName, new SynthesisedMetadata(descriptor, metadata));
        return metadata;
    }

    private record SynthesisedMetadata(IndexDescriptor descriptor, IndexMetadata metadata) {
    }

    /**
     * The byte ceiling for {@link #SYNTHESISED}.
     *
     * <p><b>Bytes, not entries, and that distinction is the whole point.</b> This cache used to be built
     * with {@code setMaximumWeight(50_000)} and no weigher, which -- since {@link CacheBuilder}'s default
     * weigher is one per entry -- meant "fifty thousand synthesised {@link IndexMetadata} instances,
     * whatever they weigh". A synthesised instance carries the index's whole mapping, so a tenant with a
     * few thousand fields is hundreds of kilobytes on its own and fifty thousand of them are hundreds of
     * megabytes of heap held by a cache whose stated purpose is instance <em>stability</em>, not capacity.
     * This registry's sibling, the plugin's descriptor cache, was deliberately converted to a byte bound
     * after exactly this failure shape was measured there; this is the same correction applied here.
     *
     * <p>Two hundred and fifty-six megabytes is deliberately generous rather than tuned: the cost of
     * evicting too eagerly is a re-synthesis plus, worse, a fresh instance that breaks the identity memo in
     * {@code AbsentIndexRoutingSuppliers} which this cache exists to keep hitting (an 18x regression when
     * it misses -- see {@link #synthesisedMetadata}). The point of the change is that the bound is now
     * expressed in the resource that actually runs out.
     */
    private static final long SYNTHESISED_MAX_BYTES = 256L * 1024 * 1024;

    /**
     * Roughly what one cached entry retains: the mapping source dominates by orders of magnitude, and it is
     * held compressed (see {@code MappingMetadata}, which keeps only a {@code CompressedXContent}), so the
     * compressed length is the honest figure rather than the expanded one. The settings and the fixed
     * per-entry overhead are approximated by a flat constant -- this is a bound, not an accounting.
     */
    private static long synthesisedWeight(String indexName, SynthesisedMetadata value) {
        long weight = 1024L + 2L * (indexName == null ? 0 : indexName.length());
        IndexMetadata metadata = value.metadata();
        if (metadata != null) {
            MappingMetadata mapping = metadata.mapping();
            if (mapping != null) {
                weight += mapping.source().compressed().length;
            }
            weight += 64L * metadata.getSettings().size();
        }
        return weight;
    }

    private static final Cache<String, SynthesisedMetadata> SYNTHESISED = CacheBuilder.<String, SynthesisedMetadata>builder()
        .setMaximumWeight(SYNTHESISED_MAX_BYTES)
        .weigher(AbsentIndexDescriptorSuppliers::synthesisedWeight)
        .build();

    /** Drops every synthesised instance, which a test must do because this registry is static. */
    public static void clearSynthesised() {
        SYNTHESISED.invalidateAll();
    }

    /**
     * The metadata for an index, from cluster state when it is there and from the descriptor when it is
     * not.
     *
     * <p>This is the write path's whole repair, and its sites were found one stack trace at a
     * time: eleven places on the path from a bulk request to a shard read cluster state for something a
     * gated index keeps in its descriptor. Each of them wants an {@link IndexMetadata} and each of them
     * gets a null or a throw instead.
     *
     * <p><b>Deliberately not folded into {@link Metadata#getIndexSafe}.</b> That is the obvious place and it
     * is the wrong one: widening a hot core accessor to do a remote lookup was measured to deadlock,
     * because the accessor is called from the cluster state applier thread and the lookup needs that thread
     * to make progress. A separate helper means every caller of it is one somebody chose, and the cluster
     * state thread is not among them.
     *
     * <p>Returns null rather than throwing, matching {@link Metadata#index(Index)}, because the callers
     * disagree about what absence means: a bulk request fails the one document, resolution reports no such
     * index, and routing reports no shard available.
     */
    public static IndexMetadata metadataOrDescriptor(Metadata metadata, String indexName) {
        IndexMetadata published = metadata.index(indexName);
        if (published != null || isRegistered() == false) {
            return published;
        }
        return synthesisedMetadata(indexName);
    }

    /**
     * The same, for a caller that already holds a concrete {@link org.opensearch.core.index.Index}.
     *
     * <p>The uuid is checked rather than ignored. A caller with an {@code Index} has already resolved a
     * name to a uuid, and answering with a descriptor for a different uuid would serve a request against a
     * deleted index's successor, which is the one thing the durable tombstone exists to prevent.
     */
    public static IndexMetadata metadataOrDescriptor(Metadata metadata, org.opensearch.core.index.Index index) {
        IndexMetadata published = metadata.index(index);
        if (published != null || isRegistered() == false) {
            return published;
        }
        IndexMetadata synthesised = synthesisedMetadata(index.getName());
        if (synthesised == null || synthesised.getIndexUUID().equals(index.getUUID()) == false) {
            return null;
        }
        return synthesised;
    }

    /**
     * The names among these that are gated, meaning cluster state has no entry and a descriptor does.
     *
     * <p>For the operations that cannot be expressed on a descriptor at all, so they can refuse clearly
     * instead of failing on {@code Metadata.getIndexSafe} with "no such index". That message is actively
     * misleading for a gated index: the index exists, it is simply not where the caller looked, and an
     * operator reading it would go looking for a deleted index rather than an unsupported operation.
     *
     * <p><b>Must be called off the cluster state thread.</b> Resolving a descriptor is a remote read and
     * {@link #supply} refuses to answer on that thread, so a caller that checks from inside a cluster state
     * update task gets an empty answer and learns nothing. Every current caller resolves on the transport
     * thread that received the request and passes the result in, which is what {@code MetadataDeleteIndexService}
     * established.
     */
    public static java.util.List<org.opensearch.core.index.Index> gatedAmong(Metadata metadata, org.opensearch.core.index.Index[] indices) {
        if (isRegistered() == false || indices == null) {
            return java.util.List.of();
        }
        java.util.List<org.opensearch.core.index.Index> gated = new java.util.ArrayList<>();
        for (org.opensearch.core.index.Index index : indices) {
            if (metadata.index(index) == null && metadataOrDescriptor(metadata, index) != null) {
                gated.add(index);
            }
        }
        return gated;
    }
}
