/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.cluster;

import org.opensearch.common.unit.TimeValue;

/**
 * An SPI a plugin implements to own the tuning of
 * {@link IndicesClusterStateService}'s on-demand-opened-index residency bookkeeping -- the sweep that closes
 * an index this node opened on demand once nothing references it any more, and the ceiling that bounds how
 * many such indices this node holds open at once.
 *
 * <p><b>What stayed in core, and why, after a dedicated scoping investigation.</b> The original
 * sketch described moving the whole eviction subsystem -- settings, sweep, and the on-demand shard-opening
 * mechanism itself -- bodily into the plugin. Reading {@link IndicesClusterStateService#openComputedShardsOnDemand}
 * in full found that {@code IndicesService.createShard} takes fifteen collaborators this class alone
 * assembles (the checkpoint publisher, the peer recovery target service, the repositories service, the
 * failure and global-checkpoint consumers, the retention lease syncer, and more), and the eviction methods
 * ({@code evictColdestOfASample}, {@code releaseGatedIndex}) are ordered around that same method's locking to
 * avoid a measured cluster-state-thread deadlock -- a second, plugin-resident lifecycle that had to agree with this one on that
 * ordering would be a worse problem than the one this phase exists to solve. That mechanism -- the map of
 * what this node opened on demand, the sweep and eviction loops that walk it, and the shard-open/close calls
 * themselves -- stays in core, unchanged: it now lives in {@link GatedIndexResidency}, extracted from and
 * still constructed and driven by {@code IndicesClusterStateService}.
 *
 * <p>What genuinely was core-owned only by accident of the class it grew inside of is the <em>tuning</em>:
 * three settings ({@code indices.gated.deleted_shard_sweep_interval}, {@code indices.gated.idle_eviction_after},
 * {@code indices.gated.max_open}) hardcoded into {@code ClusterSettings.BUILT_IN_CLUSTER_SETTINGS}, and one
 * empirically-measured constant ({@code MEASURED_BYTES_PER_OPEN_GATED_INDEX}, a specific plugin's own shard
 * implementation's per-index heap cost) baked into the derivation of the third. This interface is exactly
 * that surface, no more: a plugin decides the numbers, core keeps running the mechanism with whatever numbers
 * it is given.
 *
 * <p>Registered via a new {@code ClusterPlugin.getIndexResidencyPolicy()} default-empty hook, the same
 * registration point {@link org.opensearch.cluster.metadata.IndexCreationStrategy} uses, and
 * collected into {@link IndexResidencyPolicyRegistry} once at node startup the same way.
 *
 * <p><b>Unregistered means today's defaults, not "disabled."</b> Unlike {@code IndexCreationStrategyRegistry},
 * where "nothing registered" answers every predicate {@code false} (because an ordinary cluster has no
 * gated/computed indices for the predicate to ever be asked about), this policy's methods can be asked
 * regardless -- the sweep timer in {@link IndicesClusterStateService#doStart} runs on every node, plugin or
 * not, and always has. So each method here defaults to the exact numeric default {@code
 * IndicesClusterStateService} hardcoded before this phase, keeping a plugin-free node's behavior bit-for-bit
 * identical rather than merely equivalent.
 *
 * @opensearch.experimental
 */
public interface IndexResidencyPolicy {

    /**
     * How often a node checks whether the on-demand-opened indices it holds still exist and still deserve
     * to stay open. Zero disables the sweep.
     *
     * <p>Defaults to sixty seconds, the value {@code IndicesClusterStateService} used before this interface
     * existed.
     */
    default TimeValue sweepInterval() {
        return TimeValue.timeValueSeconds(60);
    }

    /**
     * How long an on-demand-opened index may sit untouched before this node closes it again. Zero disables
     * idle eviction.
     *
     * <p>Defaults to thirty minutes, the value {@code IndicesClusterStateService} used before this
     * interface existed.
     */
    default TimeValue idleEvictionAfter() {
        return TimeValue.timeValueMinutes(30);
    }

    /**
     * The most on-demand-opened indices this node will hold at once, or zero to let the caller derive one
     * from the heap and {@link #bytesPerOpenIndex()}.
     *
     * <p>Defaults to zero -- derive it -- matching {@code IndicesClusterStateService}'s previous behavior.
     */
    default int maxOpen() {
        return 0;
    }

    /**
     * The heap cost of one on-demand-opened index, in bytes, which the derived default for {@link
     * #maxOpen()} divides half the heap by (see {@code GatedIndexResidency#resolveGatedMaxOpen}). This is a
     * property of a plugin's own shard implementation, so a plugin that has measured its footprint (the way
     * one plugin's {@code GatedResidencySoakIT} measured 150,888 bytes per index for its implementation)
     * must override this with its own number rather than rely on the default.
     *
     * <p><b>The default is a deliberately conservative placeholder, not a measurement.</b> One mebibyte per
     * open index under-caps rather than over-caps: too small a ceiling evicts and re-opens, which costs a
     * cold start, while too large a ceiling exhausts the heap, which costs the node. It is also, in
     * practice, never consulted on a node without a registered policy -- the ceiling only gates the
     * on-demand open path, and no index reaches that path unless a plugin has installed the resolvers that
     * make an index resolvable outside cluster state -- so the default exists to keep the mechanism
     * well-defined, not tuned.
     */
    default long bytesPerOpenIndex() {
        return 1_048_576L;
    }
}
