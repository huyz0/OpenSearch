/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Randomness;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndex;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices;
import org.opensearch.indices.cluster.IndicesClusterStateService.Shard;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;

/**
 * The residency bookkeeping for gated indices a node opened on demand: which ones this node holds, the
 * ceiling that bounds how many, the idle and deleted-descriptor sweeps that close them again, and the
 * shared close path both go through.
 *
 * <p>Extracted from {@link IndicesClusterStateService}, which grew this subsystem around its on-demand
 * shard opener and keeps the parts that genuinely belong to it: the opener itself (which needs that
 * class's fifteen shard-construction collaborators and its monitor), the sweep timer scheduling, and the
 * cluster-state appliers that consult {@link #heldOnDemand}. Everything here is state and policy
 * mechanics; that class delegates to it and adds nothing.
 *
 * <p><b>The lock-ordering rules documented on the methods below are about that service's monitor</b> --
 * the one its {@code applyClusterState} takes -- and they moved here with the code they describe. Nothing
 * in this class synchronizes on that monitor; the contract is the reverse, that closing an index must
 * never happen while holding it.
 */
class GatedIndexResidency {

    private static final Logger logger = LogManager.getLogger(GatedIndexResidency.class);

    /** How many resident indices to sample when choosing one to evict. See {@link #evictColdestOfASample}. */
    static final int EVICTION_SAMPLE_SIZE = 32;

    /**
     * How many descriptor reads one {@link #sweepDeletedGatedIndices} cycle may make.
     *
     * <p><b>The sweep cannot afford to be exhaustive.</b> Each held index whose descriptor is not currently
     * cached costs one sequential remote read of about 30 ms, on the single MANAGEMENT thread this runs on.
     * The idle descriptor of a held-but-quiet index is precisely the one that will not be cached -- the
     * descriptor time-to-live and this sweep's interval are both a minute, so an index nobody has touched
     * since the last cycle is guaranteed to be a cold read on this one. The resident-set ceiling
     * ({@link #resolveGatedMaxOpen()}) allows tens of thousands of open indices per node, and at five
     * thousand held that is roughly two and a half minutes of work inside a sixty-second interval: the
     * sweep could never finish a cycle, and a scheduleWithFixedDelay sweep that overruns simply runs
     * continuously.
     *
     * <p>So a cycle spends a bounded budget and the next resumes where it stopped ({@link #sweepCursor}).
     * Two hundred reads is about six seconds of a sixty-second interval -- roughly a tenth of one
     * MANAGEMENT thread rather than all of it and more.
     *
     * <p><b>What this costs: detection latency.</b> A deleted gated index is now noticed within
     * {@code ceil(held / 200)} sweep intervals rather than one -- minutes rather than a minute for a node
     * holding thousands. That is the same kind of bound the sweep already was (see
     * {@link #sweepDeletedGatedIndices}'s own note that it trades latency for needing nothing that does not
     * already exist), just a larger constant, and it is strictly better than the alternative it replaces:
     * a cycle that never completes gives a detection latency that is not bounded at all.
     */
    static final int SWEEP_BUDGET_PER_CYCLE = 200;

    /**
     * Where the next {@link #sweepDeletedGatedIndices} cycle resumes, as an offset into that cycle's own
     * snapshot of the held set. Only ever read and written from the single sweep thread, so a plain field
     * rather than an atomic; a stale or wrapped value costs a re-check, never a missed index (the offset is
     * taken modulo the snapshot's size, and the set is walked round-robin).
     */
    private int sweepCursor;

    /**
     * How long a first request waits for the shard it triggered.
     *
     * <p>Shorter than the request timeouts above it, so that a shard which cannot be opened surfaces as
     * this node declining rather than as the client's own deadline expiring. Ten seconds against a
     * measured 41.5 ms is two orders of margin, which is the right shape for a bound that should never be
     * reached.
     */
    private static final TimeValue ON_DEMAND_RECOVERY_WAIT = TimeValue.timeValueSeconds(10);

    private final AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService;
    private final ClusterService clusterService;

    private volatile int gatedMaxOpen;

    /**
     * Indices this node built from a descriptor rather than from an applied cluster state.
     *
     * <p>Node-local and deliberately not a static registry, which is what every other seam in this area is.
     * A registry keyed on nothing would be shared by every node in a test JVM, and the question this answers
     * -- "did <em>I</em> open this" -- is one only a node can answer about itself.
     *
     * <p><b>A map rather than a set, and the token is the reason.</b> A release closes an index and then
     * forgets it, and those cannot be one atomic step: closing is slow and the owning service's monitor is
     * the one its {@code applyClusterState} takes, so holding it across a close stalls cluster state
     * application behind a request. Both orderings then leave a window, and this is the second one -- a
     * request that reopens the index between the close and the forget has its entry erased by the release
     * that was finishing, leaving the index live on this node and absent from the bookkeeping that says so.
     *
     * <p>Removing by key <em>and</em> value closes it. A reopen stamps a new token, so a release whose token
     * no longer matches removes nothing and the index stays correctly recorded as held.
     *
     * <p>An earlier attempt guarded {@code IndicesClusterStateService#removeIndices} by asking the index's
     * own metadata whether it skips cluster state. That works only while a descriptor supplier is
     * registered, so it made the symptom rare rather than absent: on teardown, or on any node whose gate has
     * been released, the guard answers false and the stale entry surfaces as exactly the assertion it was
     * meant to prevent.
     */
    private final Map<Index, Long> openedOnDemand = ConcurrentCollections.newConcurrentMap();

    /** Stamps each on-demand open, so a release can tell its own entry from a later one for the same index. */
    private final AtomicLong openedOnDemandTokens = new AtomicLong();

    GatedIndexResidency(
        final AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService,
        final ClusterService clusterService
    ) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    /** Resolves the ceiling once at node start, since it depends on nothing that changes. */
    void start() {
        gatedMaxOpen = resolveGatedMaxOpen();
        logger.debug("gated indices capped at {} open on this node", gatedMaxOpen);
    }

    /**
     * Whether this index is one this node opened from a descriptor and should still be holding.
     *
     * <p>The second half is what stops the cluster-state-applier guards from being permanent. An on-demand
     * index is here because a descriptor vouched for it, so with no descriptor supplier installed nothing
     * vouches for it any more and cluster state's answer -- that it does not exist -- is the only one left.
     * Without this the index and its shard lock outlive the mechanism that created them, which a test
     * notices as a shard still locked long after everything that could unlock it is gone.
     *
     * <p>Deliberately a registry check rather than a descriptor read. This runs on the cluster state
     * applier thread, where a blocking descriptor lookup was measured to deadlock, and it runs once
     * per open index per applied state.
     *
     * <p><b>What this does not cover, stated rather than implied:</b> deleting a live gated index does not
     * close the shard on the node holding it. Deletion tombstones the descriptor and changes no cluster
     * state, so no node is told, and this check cannot see it without the read it must not make. The shard
     * stays open until the node stops. That is a real gap in the on-demand opener's design, recorded in
     * the design notes rather than left to be discovered.
     */
    boolean heldOnDemand(Index index) {
        return openedOnDemand.containsKey(index) && AbsentIndexDescriptorSuppliers.isRegistered();
    }

    /**
     * Records an index the on-demand opener just built, stamping a fresh token so a concurrent release of
     * an earlier open of the same name cannot erase this entry. Called from inside the owning service's
     * monitor, immediately after the index is created -- the bookkeeping and the creation must not be
     * separated by a window in which {@code removeIndices} could observe one without the other.
     */
    void recordOpened(Index index) {
        openedOnDemand.put(index, openedOnDemandTokens.incrementAndGet());
    }

    /**
     * Drops an index from the bookkeeping without closing it, for the caller that is about to close it
     * itself: {@code IndicesClusterStateService#removeIndices}, when {@link #heldOnDemand} has just decided
     * the index is no longer vouched for.
     *
     * @return whether the index was recorded as held on demand
     */
    boolean forget(Index index) {
        return openedOnDemand.remove(index) != null;
    }

    /** The names of every gated index this node currently holds on-demand-opened shards for. */
    List<String> onDemandOpenIndexNames() {
        return openedOnDemand.keySet().stream().map(Index::getName).toList();
    }

    /**
     * Every gated index this node currently holds shards for, as uuid plus real, currently-open shard
     * count. A snapshot, not a live view -- see {@code IndicesClusterStateService#onDemandOpenIndices()},
     * the public face of this method, for the full contract.
     */
    List<IndicesClusterStateService.OnDemandOpenIndex> onDemandOpenIndices() {
        List<IndicesClusterStateService.OnDemandOpenIndex> result = new ArrayList<>();
        for (Index index : openedOnDemand.keySet()) {
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
            if (indexService != null) {
                result.add(
                    new IndicesClusterStateService.OnDemandOpenIndex(index.getUUID(), indexService.getIndexSettings().getNumberOfShards())
                );
            }
        }
        return result;
    }

    /**
     * Evicts toward the ceiling before an on-demand open grows the resident set.
     *
     * <p>Must be called before the owning service's lock is taken, because closing an index is slow and
     * that monitor is the one {@code applyClusterState} takes. Holding it across an eviction would stall
     * cluster state application behind a request, which is the mistake the first version of the deletion
     * sweep made and paid five suites for.
     *
     * <p>Only when this node does not already hold the index: reopening something already resident is not
     * growth and must not evict anything to make room for what is already there.
     */
    void makeRoomFor(Index index) {
        int cap = gatedMaxOpen;
        if (cap > 0 && indicesService.indexService(index) == null && openedOnDemand.size() >= cap) {
            evictColdestOfASample(cap);
        }
    }

    /**
     * Closes indices this node opened on demand whose descriptor is no longer there.
     *
     * <p><b>Why a sweep rather than an event.</b> Every other index closure on this node is driven by a
     * cluster state diff, and a gated index never appears in one -- that is the point of gating. So a gated
     * delete tells nobody: the cluster manager tombstones the descriptor and returns, and the node holding
     * the shard has no way to hear about it. Left alone the shard stays open forever, holding its
     * {@code NodeEnvironment} lock, its memory and its files, and still able to serve reads for an index the
     * cluster says is gone. That is how this was found -- as a lock still held at test teardown, three
     * layers downstream of the delete that should have released it.
     *
     * <p>The push version belongs on the change feed, which would let a node learn about a deletion the same
     * way it learns about any other descriptor change. That is not built, and a sweep is the honest stand-in
     * rather than a design: it trades latency, bounded by the interval times the number of cycles a full
     * pass takes (see {@link #SWEEP_BUDGET_PER_CYCLE}), for needing nothing that does not already exist.
     *
     * <p><b>Closes rather than deletes, and that is what makes an ambiguous answer safe.</b> A descriptor
     * that will not resolve may mean deleted or may mean the store is briefly unreadable, and no caller can
     * tell the two apart -- the same ambiguity that stops {@code DanglingIndicesState} inferring deletion
     * from absence. Deleting on that basis would discard live data. Closing does not: {@code
     * NO_LONGER_ASSIGNED} leaves the contents on disk, and the on-demand opener rebuilds the shard on the
     * next request that needs it. So the worst case of guessing wrong is one cold start, not data loss.
     */
    void sweepDeletedGatedIndices() {
        if (openedOnDemand.isEmpty() || AbsentIndexDescriptorSuppliers.isRegistered() == false) {
            return;
        }
        // Runs on GENERIC, which is what lets it resolve a descriptor at all: the read is remote, and
        // AbsentIndexDescriptorSuppliers refuses to answer on a cluster state thread because that was
        // measured to deadlock. That is also why this cannot simply be folded into applyClusterState.
        ClusterState state = clusterService.state();
        List<Index> held = List.copyOf(openedOnDemand.keySet());
        // Resumed where the previous cycle stopped, so the set is covered round-robin across cycles rather
        // than exhaustively within one. See SWEEP_BUDGET_PER_CYCLE for why.
        int start = held.isEmpty() ? 0 : Math.floorMod(sweepCursor, held.size());
        int examined = 0;
        int checkedRemotely = 0;
        while (examined < held.size() && checkedRemotely < SWEEP_BUDGET_PER_CYCLE) {
            Index index = held.get((start + examined) % held.size());
            examined++;
            if (state.metadata().index(index) != null) {
                // Published after all, so the ordinary path owns its lifecycle now. Costs no remote read,
                // so it does not count against this cycle's budget.
                continue;
            }
            IndexMetadata descriptorMetadata;
            try {
                checkedRemotely++;
                descriptorMetadata = state.metadata().indexOrResolved(index);
            } catch (Exception e) {
                logger.debug(() -> new ParameterizedMessage("[{}] could not be checked for deletion", index), e);
                continue;
            }
            if (descriptorMetadata != null) {
                continue;
            }
            // Closed without holding the owning service's monitor, and without removing the entry first. See
            // releaseGatedIndex for both reasons: the monitor is the one applyClusterState takes, and
            // claiming the index before the close opens a window where removeIndices sees a live index that
            // is absent from cluster state and unclaimed, which trips its assertion.
            //
            // A racing on-demand open that rebuilds the shard costs a cold start, not an index.
            releaseGatedIndex(index, "gated index no longer has a descriptor");
        }
        sweepCursor = held.isEmpty() ? 0 : start + examined;
    }

    /**
     * Closes gated indices this node opened on demand that nothing has touched for long enough.
     *
     * <p>The other half of the on-demand lifecycle, and the half that decides whether this design works at
     * scale. {@link #sweepDeletedGatedIndices} answers "is this index gone", which bounds nothing: a node
     * serving live tenants keeps every one of them open forever, so its resident set is the number of
     * distinct tenants it has ever seen. See {@link IndexResidencyPolicy#idleEvictionAfter()} for what that
     * costs.
     *
     * <p><b>Every shard of the index, not any.</b> An index is evicted only when all of its shards are cold,
     * because closing the index closes all of them, and one busy shard is reason enough to keep the whole
     * thing. Reading it the other way round would evict an index that is actively serving on one shard and
     * quiet on the rest, which is the ordinary shape of a skewed tenant rather than an unusual one.
     *
     * <p><b>An index with no shards on this node is left alone rather than treated as maximally idle.</b>
     * That state means the on-demand opener is mid-flight, or every shard has already gone, and evicting on
     * it would race the opener that is building the shard this very moment.
     *
     * <p>Closing flushes, so this does not lose writes: {@code IndexService.closeShard} passes
     * {@code flushEngine = deleted == false && closed}, and an eviction is a close without a delete.
     */
    void evictIdleGatedIndices(TimeValue idleAfter) {
        if (openedOnDemand.isEmpty()) {
            return;
        }
        final long idleAfterMillis = idleAfter.millis();
        for (Index index : List.copyOf(openedOnDemand.keySet())) {
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
            if (indexService == null) {
                continue;
            }
            boolean anyShard = false;
            boolean allCold = true;
            // The index has been idle for as long as its *most recently used* shard, which is the minimum
            // of the per-shard figures rather than the maximum. Reporting the maximum would claim an index
            // had been quiet for as long as its stalest shard while another was still being written.
            long indexIdleMillis = Long.MAX_VALUE;
            for (Shard shard : indexService) {
                anyShard = true;
                long idle = shard.idleMillis();
                indexIdleMillis = Math.min(indexIdleMillis, idle);
                if (idle < idleAfterMillis) {
                    allCold = false;
                    break;
                }
            }
            if (anyShard && allCold) {
                logger.debug("{} evicting gated index idle for {} ms", index, indexIdleMillis);
                releaseGatedIndex(index, "gated index idle for " + indexIdleMillis + " ms");
            }
        }
    }

    /**
     * The ceiling this node enforces, resolving zero to a figure derived from the heap.
     *
     * <p>Computed once at start rather than per open, since it depends on nothing that changes.
     *
     * <p>The per-index heap cost the derivation divides by comes from {@link
     * IndexResidencyPolicy#bytesPerOpenIndex()}: a measured figure when a plugin that knows its own shard
     * implementation is registered, and that interface's documented conservative placeholder when nothing
     * is -- in which case the value is never consulted anyway, because only the on-demand open path reads
     * the ceiling and no index reaches it without a plugin's resolvers installed.
     */
    private int resolveGatedMaxOpen() {
        int configured = IndexResidencyPolicyRegistry.maxOpen();
        if (configured > 0) {
            return configured;
        }
        long derived = (Runtime.getRuntime().maxMemory() / 2) / IndexResidencyPolicyRegistry.bytesPerOpenIndex();
        // Clamped so a tiny heap does not produce a cap of nought or one, which would evict an index the
        // request that opened it is about to use, and so a very large heap does not overflow an int.
        return (int) Math.max(16, Math.min(Integer.MAX_VALUE, derived));
    }

    /**
     * Closes gated indices until this node is below its ceiling, or gives up trying.
     *
     * <p>Called on the path that opens an index, which is the point of it. The idle sweep closes what has
     * gone cold and depends on getting CPU to do so; this closes what has to go for the ceiling to hold, on
     * the thread of the request that would otherwise breach it. The cost lands on the request that caused
     * the growth rather than on an unrelated one, and it lands whether or not anything else is scheduled.
     *
     * <p><b>Sampled rather than exhaustive.</b> Choosing the genuinely coldest index means comparing every
     * resident one, which is a scan of up to a hundred thousand entries on a request path. Sampling a few
     * and evicting the coldest of those is the trade Redis makes for the same reason: the coldest of 32
     * random entries is close enough to the coldest overall for a policy whose job is bounding a total, and
     * it costs a fixed amount however large the population.
     *
     * <p>Bounded attempts, because the alternative is a request that spins. If sampling keeps choosing
     * indices that another thread closes first, or every sampled index is busy, this gives up and lets the
     * open proceed over the ceiling. A cap that is occasionally exceeded is a bound; a request that never
     * returns is an outage.
     */
    private void evictColdestOfASample(int cap) {
        int attempts = 0;
        while (openedOnDemand.size() >= cap && attempts < EVICTION_SAMPLE_SIZE) {
            attempts++;
            Index coldest = null;
            long coldestIdle = -1;
            int sampled = 0;
            // Started at a random offset, which is what makes this a sample rather than a fixed window.
            // Iterating openedOnDemand from the beginning always visits the same entries in the same order
            // -- a ConcurrentHashMap's iteration order is a function of the keys' hashes, not of anything
            // that varies -- so the "first 32" were one fixed region of the map. Indices that landed in it
            // were re-considered on every single eviction and repeatedly closed while genuinely colder
            // indices elsewhere in the map were never even looked at.
            int population = openedOnDemand.size();
            int skip = population > EVICTION_SAMPLE_SIZE ? Randomness.get().nextInt(population) : 0;
            Iterator<Index> candidates = openedOnDemand.keySet().iterator();
            for (int skipped = 0; skipped < skip && candidates.hasNext(); skipped++) {
                candidates.next();
            }
            // Wraps once, so a random offset near the end of the map still yields a full sample rather than
            // a truncated one.
            boolean wrapped = false;
            while (sampled < EVICTION_SAMPLE_SIZE) {
                if (candidates.hasNext() == false) {
                    if (wrapped || skip == 0) {
                        break;
                    }
                    wrapped = true;
                    candidates = openedOnDemand.keySet().iterator();
                    continue;
                }
                Index candidate = candidates.next();
                AllocatedIndex<? extends Shard> indexService = indicesService.indexService(candidate);
                if (indexService == null) {
                    continue;
                }
                long idle = Long.MAX_VALUE;
                for (Shard shard : indexService) {
                    idle = Math.min(idle, shard.idleMillis());
                }
                if (idle != Long.MAX_VALUE && idle > coldestIdle) {
                    coldestIdle = idle;
                    coldest = candidate;
                }
                sampled++;
            }
            if (coldest == null) {
                return;
            }
            logger.debug("{} evicting to stay under the gated ceiling of {}, idle for {} ms", coldest, cap, coldestIdle);
            releaseGatedIndex(coldest, "gated index evicted to stay under the ceiling of " + cap);
        }
    }

    /**
     * Closes a gated index this node opened on demand, if it still holds it.
     *
     * <p>Shared by the sweep and by the change feed, which is what keeps the two from drifting: the feed
     * makes this fast in the common case and the sweep makes it certain in the uncommon one, and both have
     * to close a shard the same way or a shard closed by one route would differ from the other.
     *
     * <p>Closed without holding the owning service's monitor. The first version of the sweep wrapped the
     * whole thing in {@code synchronized} on that service and it cost five previously-passing suites: that
     * monitor is the one its {@code applyClusterState} takes, closing a shard is slow, and this runs on
     * GENERIC, so it would stall cluster state application on its node until it finished. The symptom was an
     * unrelated index delete returning "not acked", nowhere near this code, only under whole-suite load, and
     * passing in isolation. It also doubled the suite's wall clock.
     *
     * <p><b>The index stays in {@code openedOnDemand} until the close has finished, and the order is
     * load-bearing.</b> An earlier version claimed the index by removing it from the set first, on the
     * reasoning that an atomic claim prevents a double close. It does, and {@code removeIndex} is idempotent
     * anyway, so it was buying nothing -- while opening a window in which {@link #heldOnDemand} answered
     * false for an index this node still held. A cluster state application landing in that window reaches
     * {@code IndicesClusterStateService#removeIndices}, finds a live index that is absent from the metadata
     * and unclaimed, and trips its assertion that such an index must have been deleted or the cluster must
     * be new. Neither is true of one that was never published.
     *
     * <p>That was latent for as long as deletion was the only thing that closed a gated index, because a
     * deleted gated index is rare and the window is short. Idle eviction closes them constantly, and the
     * assertion fired within a hundred and twenty creations.
     *
     * <p>Keeping the entry until afterwards closes the window from both sides: while the close runs
     * {@link #heldOnDemand} is true so {@code removeIndices} skips the index, and once it returns the index
     * is no longer in {@code indicesService} for the loop to reach at all.
     */
    void releaseGatedIndex(Index index, String reason) {
        // Captured before the close, so the removal afterwards can tell whether this is still the same open.
        // A request that reopens the index while this close runs stamps a new token, and the removal then
        // matches nothing rather than erasing an entry that describes a live index.
        Long token = openedOnDemand.get(index);
        if (token == null) {
            return;
        }
        logger.debug("{} closing gated index opened on demand: {}", index, reason);
        try {
            indicesService.removeIndex(index, NO_LONGER_ASSIGNED, reason);
        } catch (Exception e) {
            // Left in openedOnDemand deliberately, so the next sweep tries again rather than abandoning an
            // index this node is still holding but no longer tracking.
            logger.warn(() -> new ParameterizedMessage("[{}] could not be closed", index), e);
            return;
        }
        openedOnDemand.remove(index, token);
    }

    /**
     * Waits for shards the on-demand opener just opened to finish recovering, outside the lock.
     *
     * <p>Without this the opener returns a shard that exists and is INITIALIZING, the write fails with
     * "shard is not in primary mode", and the coordinator retries. That retry is the expensive part and it
     * is expensive for a reason particular to this design: {@code ReroutePhase} retries by waiting for the
     * <em>next cluster state change</em>, and a gated index produces none, so the retry sits until the
     * request times out and then succeeds on the timeout path. Measured at 57 seconds per tenant against a
     * one minute default, which reads as a hang rather than as a retry.
     *
     * <p>So the wait happens here, where it can end the moment the shard is ready. A wake was measured at
     * 41.5 ms and this is the same order: it is a bounded pause on a first write, not a poll loop with a
     * long tail.
     *
     * <p>Called outside the owning service's synchronized block deliberately. Its
     * {@code handleRecoveryFailure} is synchronized on that instance, so waiting while holding the lock
     * would stop the very recovery being waited on from ever reporting failure -- a deadlock that only
     * appears when recovery fails, which is the case least likely to be exercised.
     *
     * <p>Gives up quietly at the deadline rather than throwing. The caller's next step re-reads the shard
     * and reports the absence, and a slow recovery should look to the client like the retry it already
     * knows how to handle.
     */
    void awaitStarted(Index index, List<ShardId> opening) {
        if (opening.isEmpty()) {
            return;
        }
        long deadline = System.nanoTime() + ON_DEMAND_RECOVERY_WAIT.nanos();
        for (ShardId shardId : opening) {
            while (System.nanoTime() < deadline) {
                AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
                Shard shard = indexService == null ? null : indexService.getShardOrNull(shardId.id());
                if (shard == null) {
                    // Failed and was removed while recovering. Nothing to wait for, and the caller's
                    // lookup will report it.
                    break;
                }
                if (shard.state() == IndexShardState.STARTED) {
                    break;
                }
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
