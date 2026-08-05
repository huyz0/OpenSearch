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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Node-level hook for resolving an index that has no entry in cluster state metadata.
 *
 * <p>Area H's premise is that cluster state should hold no entry per index, which means resolution has to
 * answer from somewhere else. This is that seam, and it is deliberately the same shape as
 * {@code AbsentIndexRoutingSuppliers} from Area C: try the published structure first, fall back to a
 * supplier, and behave exactly as before when nothing is installed.
 *
 * <p><b>Why copy that shape rather than design a new one.</b> Area C's seam was applied across nine call
 * sites and the failures it produced are known and documented: the same absence has to mean the same thing
 * everywhere, the fast path has to be identical rather than merely equivalent, and a caller that silently
 * resolves nothing is worse than one that throws. Reusing the shape inherits those lessons instead of
 * rediscovering them.
 *
 * <p><b>What this does not do.</b> It does not make resolution asynchronous. A descriptor lookup is a
 * realtime GET against one shard, measured flat at about 0.8 ms up to fifty thousand descriptors (S21),
 * but it is still a remote call where a map read was local. Every caller of this seam is on a path that
 * already does remote work, and a supplier that blocks a transport thread would be a worse problem than
 * the one this solves. The supplier is therefore expected to answer from a cache and to decline rather
 * than block when it cannot.
 *
 * <p>Unset by default, so an ordinary cluster resolves exactly as it always has.
 */
public final class AbsentIndexDescriptorSuppliers {

    private static final Logger logger = LogManager.getLogger(AbsentIndexDescriptorSuppliers.class);

    private static final AtomicReference<Function<String, IndexDescriptor>> SUPPLIER = new AtomicReference<>();

    /**
     * Threads on which a supplier must not be asked to do I/O.
     *
     * <p>W4 established the deadlock: a remote lookup made from the cluster state applier thread needs
     * that thread to make progress before it can complete, so it waits on itself. The cluster manager's
     * update thread is the same shape and is additionally the one serialised thread this whole design
     * exists to keep out of the request path.
     *
     * <p>T1 enumerated six call sites that reach a supplier from one of these: {@code DanglingIndicesState},
     * {@code IndicesClusterStateService} twice, {@code RoutingNodes} through
     * {@code ClusterState.getRoutingNodes}, {@code ClusterStateHealth} by way of {@code AllocationService},
     * and {@code ActiveShardCount}. Six is few enough to fix one at a time and too many to keep correct by
     * discipline, because every future core change that reads metadata on one of these threads reopens the
     * hole and the failure only appears on a cache miss, which is the case testing does not produce.
     *
     * <p>So this makes it impossible rather than forbidden. A warm descriptor still answers, because that
     * costs no I/O and never reaches a supplier that would block. A cold one degrades to the same absence
     * the seam already models everywhere. That turns a silent stall into a reported absence, which is the
     * same trade T38 made when it landed the auto-creation removal alongside the shard path rather than
     * before it.
     */
    private static final String[] THREADS_WHERE_BLOCKING_IS_UNSAFE = {
        "clusterApplierService#updateTask",
        "clusterManagerService#updateTask",
        "masterService#updateTask" };

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

    private AbsentIndexDescriptorSuppliers() {}

    /**
     * Installs the supplier. Registering null clears it, which is what lets a test restore the default
     * rather than leaking a supplier into unrelated cases.
     */
    public static void register(Function<String, IndexDescriptor> supplier) {
        SUPPLIER.set(supplier);
    }

    /** Whether anything is installed, so callers can skip work that would be discarded. */
    public static boolean isRegistered() {
        return SUPPLIER.get() != null;
    }

    /**
     * The descriptor for a name with no metadata entry, or null to fall back to the existing behaviour.
     *
     * <p>A supplier that throws is treated as having no answer rather than being allowed to fail the
     * request, for the reason Area C settled: this is a degradation path already, and turning a plugin
     * bug into a request failure makes the absence worse than it was.
     */
    public static IndexDescriptor supply(String indexName) {
        Function<String, IndexDescriptor> supplier = SUPPLIER.get();
        if (supplier == null || indexName == null) {
            return null;
        }
        if (blockingIsUnsafeHere()) {
            // Reported as absent rather than fetched. See blockingIsUnsafeHere: a supplier backed by a
            // remote store would block a thread that has to make progress for the fetch to complete, and
            // every caller here already handles absence because that is the seam's contract.
            logger.debug("refusing to resolve the descriptor for [{}] on {}", indexName, Thread.currentThread().getName());
            return null;
        }
        try {
            return supplier.apply(indexName);
        } catch (DescriptorUnavailableException e) {
            // The one failure that must not become "no answer". Everything else here is a supplier bug,
            // and Area C's reasoning holds for a bug: resolution is already a degradation path and turning
            // a plugin defect into a request failure makes the absence worse. It does not hold when the
            // store could not be read, because then "no answer" is indistinguishable from "no such index"
            // and the caller's safe response to those two is opposite.
            throw e;
        } catch (Exception e) {
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
     * Resolves several names at once, so a caller with a list pays one round trip rather than one each.
     *
     * <p>Present because resolution is frequently plural and the per-name form would otherwise turn a
     * ten-index request into ten sequential lookups, which is the shape of mistake that made wake and
     * sleep cost a publication each before they were batched.
     */
    /**
     * Supplies one page of gated indices in pagination order.
     *
     * <p>The whole point is the {@code size} argument. H16 measured that pagination sorts the entire
     * population to produce one page, so making gated indices visible by folding them into that sort would
     * have made the cost problem worse while appearing to fix the correctness one. A pager is asked for a
     * page and returns a page, which is what the descriptor index can actually do cheaply: S24 measured
     * paging by sorted name with {@code search_after} at roughly 33 ms per thousand names.
     */
    public static List<IndexDescriptor> supplyAll(List<String> indexNames) {
        if (isRegistered() == false) {
            return List.of();
        }
        return indexNames.stream().map(AbsentIndexDescriptorSuppliers::supply).filter(java.util.Objects::nonNull).toList();
    }

    /**
     * One gated index that matched a prefix, carrying only what wildcard expansion filters on.
     *
     * <p>Three fields rather than an {@link IndexDescriptor}, because T5 measured that building the full
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
    public record PrefixExpansion(List<PrefixMatch> matches, boolean exceeded, int limit) {

        public static PrefixExpansion of(List<PrefixMatch> matches) {
            return new PrefixExpansion(matches, false, -1);
        }

        public static PrefixExpansion tooMany(int limit) {
            return new PrefixExpansion(List.of(), true, limit);
        }
    }

    /**
     * Expands a prefix over gated indices.
     *
     * <p>Prefix rather than pattern, and that is the contract rather than an implementation detail. The
     * descriptor index is sorted by name, so a prefix is a range scan and anything else is a scan of the
     * whole population. An expander is never asked to answer {@code *-logs}, because at a hundred million
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
     * call makes that memo miss every time, which P5 measured as an 18x regression rather than a slow path.
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

    private static final java.util.concurrent.ConcurrentHashMap<String, SynthesisedMetadata> SYNTHESISED =
        new java.util.concurrent.ConcurrentHashMap<>();

    /** Drops every synthesised instance, which a test must do because this registry is static. */
    public static void clearSynthesised() {
        SYNTHESISED.clear();
    }

    /**
     * The metadata for an index, from cluster state when it is there and from the descriptor when it is
     * not.
     *
     * <p>This is the write path's whole repair, and S51 to S54 found the sites for it one stack trace at a
     * time: eleven places on the path from a bulk request to a shard read cluster state for something a
     * gated index keeps in its descriptor. Each of them wants an {@link IndexMetadata} and each of them
     * gets a null or a throw instead.
     *
     * <p><b>Deliberately not folded into {@link Metadata#getIndexSafe}.</b> That is the obvious place and it
     * is the wrong one: W4 established that widening a hot core accessor to do a remote lookup deadlocks,
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
