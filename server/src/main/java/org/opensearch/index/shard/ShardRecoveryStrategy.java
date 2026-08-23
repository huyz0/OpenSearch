/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.Optional;

/**
 * How a shard's local Lucene store gets populated, and what is authoritative once it is.
 *
 * <p>Everything on this interface runs in {@code StoreRecovery} / {@link IndexShard} <em>before</em>
 * this shard's engine exists, which is precisely why it is not on {@code EngineFactory}: these are
 * store-population and durability-ownership decisions, not engine construction. They were briefly
 * carried as {@code default} methods on {@code EngineFactory} and that shape had two visible
 * symptoms -- three of the methods had byte-identical bodies differing only by {@link
 * RecoverySource.Type}, and the one operation that has no shard at all ({@link
 * EngineNativeSnapshots#release}) had to be implemented on an {@code EngineFactory} whose
 * {@code newReadWriteEngine} threw, purely to satisfy the type of the registry it lived in.
 *
 * <p>Selected per index by {@link org.opensearch.index.IndexModule#INDEX_RECOVERY_STRATEGY_SETTING},
 * resolved against the node-wide map that {@link
 * org.opensearch.plugins.IndexStorePlugin#getShardRecoveryStrategies()} contributes to and into
 * which core registers its own {@link #LOCAL_LUCENE} implementation -- the same
 * built-in-plus-plugins-in-one-map shape {@code RepositoriesModule}, {@code
 * IndexModule#createBuiltInDirectoryFactories} and {@code ClusterModule}'s allocators already use.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ShardRecoveryStrategy {

    /**
     * The name core's own strategy is registered under, and the default value of {@link
     * org.opensearch.index.IndexModule#INDEX_RECOVERY_STRATEGY_SETTING}: an ordinary shard whose
     * local Lucene files on this node's disk <em>are</em> its own authoritative copy.
     */
    String LOCAL_LUCENE = "local-lucene";

    /**
     * Called by {@code StoreRecovery#internalRecoverFromStore} at most once per recovery, when core
     * would otherwise have nothing usable on disk to open an engine against, to give this strategy
     * the chance to materialize {@code store}'s local Lucene commit -- <em>and</em>, since none of
     * these recovery sources create a local translog of their own, a matching fresh local translog
     * (mirroring what {@code StoreRecovery#recoverEmptyStore} already does for its own,
     * unconditional case) -- from wherever this strategy's actual durable copy lives.
     *
     * <p>Called before this shard's local translog is created or its engine is opened. That
     * ordering is load-bearing, not incidental: an early attempt at the in-place-split case
     * attached the child to the parent's data only in the durable store, <em>after</em> the engine
     * had already opened against an empty local Lucene index, leaving the two permanently out of
     * sync behind a stale translog UUID.
     *
     * <p>{@code recoverySourceType} is the only thing that distinguishes the cases core asks about,
     * which is why it is a parameter rather than three separate methods:
     *
     * <ul>
     *   <li>{@link RecoverySource.Type#EXISTING_STORE} -- local recovery expected an existing commit
     *       and found none, the point where core would otherwise fail the shard outright with
     *       {@code "shard allocated for local recovery, should exist, but doesn't"}. Also the type
     *       passed on the second, post-clean call {@link #localStoreIsStale} triggers. A strategy
     *       whose durability doesn't depend on this node's own local disk survival (e.g. one backed
     *       by a remote object store, addressed by its own manifest rather than by any node's local
     *       allocation-id history) materializes this shard's last durable state here.
     *   <li>{@link RecoverySource.Type#IN_PLACE_SPLIT_SHARD} -- a child shard of an in-place split,
     *       which has never had ANY prior durable state of its own. This is its first-ever
     *       activation, attaching it to its share of the parent shard's data by whatever mechanism
     *       the strategy uses (e.g. writing its own first manifest generation referencing the
     *       parent's data without copying bytes), not "recovering" pre-existing state.
     *   <li>{@link RecoverySource.Type#IN_PLACE_MERGE_SHARD} -- the reverse: a parent shard revived
     *       by an in-place merge, whose data is folded back together from <em>both</em> retired
     *       children's current, authoritative document sets. An early spike proposed reviving the
     *       parent for free by taking one surviving child's own local state and dropping its range
     *       filter, since both children were cloned from the same parent bundle; that holds only at
     *       the instant a split commits, with zero post-split writes. Once the children serve
     *       traffic each accepts its own disjoint writes into its own separate bundle/manifest
     *       (routing sends each document to exactly one child by hash), so neither child's local
     *       store is a full copy of the union any longer. The resolved approach folds both children
     *       together with a single {@code IndexWriter#addIndexes} over each child's own
     *       <em>range-filtered</em> reader: because hash routing partitions documents into disjoint
     *       ranges, each filtered reader contributes exactly that child's authoritative slice
     *       (respecting that child's own deletes via {@code liveDocs}), so the union has no
     *       double-counting and no cross-child version conflict to reconcile. Since the metadata is
     *       already de-committed by the time the parent recovers, the retired children's ranges are
     *       carried on the recovery source ({@code
     *       RecoverySource.InPlaceMergeShardRecoverySource}) rather than read from {@code
     *       SplitShardsMetadata}.
     * </ul>
     *
     * <p>Returning {@code true} without actually leaving {@code store} in a state {@link
     * Store#readLastCommittedSegmentsInfo()} can read is a contract violation core cannot detect
     * for you -- the read that follows a {@code true} return will simply fail with whatever
     * exception that leaves.
     *
     * @param indexShard the shard being recovered
     * @param store the (empty, or just-cleaned) local store to materialize into
     * @param recoverySourceType which of the cases above core is asking about
     * @return {@code true} if {@code store} now holds a readable commit and a matching local
     *         translog exists; {@code false} if this strategy has nothing to try, in which case
     *         core proceeds exactly as it would have without this seam (fail the shard for {@code
     *         EXISTING_STORE}, treat it as a plain new empty index otherwise).
     * @throws IOException if materialization was attempted but failed -- surfaced as this shard's
     *                      own recovery failure, not silently downgraded to the "nothing to try"
     *                      outcome.
     */
    boolean recoverLocalStore(IndexShard indexShard, Store store, RecoverySource.Type recoverySourceType) throws IOException;

    /**
     * Called by {@code StoreRecovery#internalRecoverFromStore} when a local Lucene commit <em>was</em>
     * found and read successfully, to ask whether it is still the shard's authoritative commit. Default
     * {@code false}: the local commit is authoritative, because for an ordinary shard it is.
     *
     * <p>The counterpart to {@link #recoverLocalStore}'s {@link RecoverySource.Type#EXISTING_STORE}
     * case, and needed for the same reason. That case only runs when there is no readable local commit
     * at all, so a strategy whose durable copy lives elsewhere is consulted only when this node happens
     * to have nothing on disk. When the node <em>does</em> have something on disk, core recovers from it
     * without ever asking -- and a local commit that is merely <em>stale</em> is indistinguishable, from
     * core's side, from a current one. The motivating case is an index closed, restored to an earlier
     * point from its durable copy, and reopened on the same node: the local files still describe the
     * pre-restore commit, so recovery silently undoes the restore. Core already handles the structurally
     * identical case for a revived in-place-merge parent, a few lines below where this is called; this
     * generalizes it to any strategy that can say its own local copy is out of date.
     *
     * <p>A strategy returning {@code true} is asserting that {@link #recoverLocalStore} will then be
     * able to materialize the authoritative commit: core cleans the local Lucene index before calling
     * it, so a {@code true} here followed by a {@code false} there leaves the shard with nothing and
     * fails its recovery loudly rather than opening an engine on a store that was just emptied.
     *
     * @param indexShard the shard being recovered
     * @param localSegmentsFileName the name of the segments file of the local commit core just read, which
     *                              identifies the commit -- a strategy compares it against whatever its own
     *                              durable record says this shard's commit should be.
     * @return {@code true} if the local commit is not the authoritative one and must be replaced.
     * @throws IOException if the strategy's own durable record could not be consulted -- surfaced as this
     *                      shard's recovery failure rather than silently treated as "not stale", because
     *                      guessing "not stale" here is what silently undoes a restore.
     */
    default boolean localStoreIsStale(IndexShard indexShard, String localSegmentsFileName) throws IOException {
        return false;
    }

    /**
     * Whether this strategy already provides its own durable, remote copy of every segment the shard
     * writes, independent of core's own remote-store upload path ({@code
     * RemoteStoreRefreshListener}, engaged whenever {@code index.remote_store.enabled} is {@code
     * true}). Default {@code false}: core has no reason to believe anything but its own remote-store
     * upload path is keeping this shard's segments durable remotely, so that path stays wired in
     * exactly as it always has.
     *
     * <p>A strategy that overrides this to return {@code true} is asserting that it has
     * <em>already</em> made every segment durable somewhere remote by some mechanism of its own
     * (e.g. publishing an object-store manifest referencing this shard's own segment files directly,
     * rather than delegating to core's remote-store directory/upload machinery) -- wiring core's
     * remote-store upload path in on top of that would not be a correctness problem (nothing about
     * search or recovery depends on it), but would be pure wasted upload bandwidth and remote storage
     * cost for bytes nothing ever reads back through that path. This exists so such a strategy can opt
     * the shard out of that wasted work explicitly, rather than silently accepting it as an
     * unavoidable cost of using {@code index.remote_store.enabled} for an unrelated reason (e.g.
     * {@code index.number_of_search_replicas}'s own prerequisite chain, which requires {@code
     * remote_store.enabled} regardless of whether anything durability-relevant should use it).
     *
     * <p>This is a durability claim, not merely a performance hint -- returning {@code true} when
     * segments are not, in fact, durably reachable by some other means is a correctness regression
     * for anything that assumes {@code index.remote_store.enabled: true} implies core's own
     * remote-store durability guarantee, e.g. a future recovery path that trusts the remote-store
     * directory without checking whether anything was ever actually uploaded to it.
     */
    default boolean ownsRemoteSegmentDurability() {
        return false;
    }

    /**
     * This strategy's engine-native snapshot support, or empty (the default) if the shard's engine
     * never produces engine-native snapshots at all -- i.e. if {@link
     * Engine#attemptEngineNativeSnapshot} can never return non-empty for a shard of this index.
     *
     * <p>Presence is itself the capability answer, deliberately: {@code
     * StoreRecovery#recoverFromEngineNativeSnapshot} checks it <em>before</em> calling {@link
     * org.opensearch.repositories.Repository#getEngineNativeShardSnapshotMetadata}, which is a real
     * remote blob-existence check -- without this cheap local gate, every classic-shaped restore
     * across every {@code BlobStoreRepository}-backed deployment would pay that round trip on every
     * shard, even though the overwhelming majority of shards never produce an engine-native snapshot
     * at all. Because the gate and the restore call are the same object rather than two independently
     * overridable methods, they cannot drift out of lockstep: a strategy that answers "supported" but
     * then declines to restore is not expressible.
     *
     * <p>A strategy whose engine overrides {@link Engine#attemptEngineNativeSnapshot} to ever return
     * non-empty must return a non-empty value here, or its own snapshots will fail to restore: {@code
     * StoreRecovery} would skip the probe that finds them and fall straight to the classic,
     * copy-based restore path instead.
     */
    default Optional<EngineNativeSnapshots> engineNativeSnapshots() {
        return Optional.empty();
    }

    /**
     * The two halves of an engine-native snapshot's lifecycle that are <em>not</em> the snapshot's
     * creation ({@link Engine#attemptEngineNativeSnapshot}, which needs a live engine and so stays on
     * {@link Engine}): turning a previously-written pointer back into a shard, and letting go of
     * whatever that pointer retained.
     *
     * <p>These two live together on their own type, reachable both through a shard's {@link
     * ShardRecoveryStrategy} and, for {@link #release}, through the node-level {@link
     * org.opensearch.index.engine.EngineNativeSnapshotReleasers} registry, because {@link #release}
     * may be called with no live {@link IndexShard} anywhere in the cluster -- see its own javadoc.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    interface EngineNativeSnapshots {

        /**
         * Called by {@code StoreRecovery#recoverFromEngineNativeSnapshot} exactly once, only when the
         * shard is recovering from a snapshot this same engine originally produced via {@link
         * Engine#attemptEngineNativeSnapshot} -- the point where core has an opaque pointer the engine
         * itself wrote at snapshot-creation time and needs it turned back into this shard's local
         * Lucene commit. Called before this shard's local translog is created or its engine is opened,
         * the same ordering {@link #recoverLocalStore} already requires and for the identical reason.
         *
         * <p>Given the exact bytes that call previously returned, materialize {@code store}'s local
         * Lucene commit and a matching local translog, then return {@code true}. Returning {@code
         * true} without leaving {@code store} in a state {@link Store#readLastCommittedSegmentsInfo()}
         * can read is a contract violation core cannot detect for you, exactly as {@link
         * #recoverLocalStore} warns. Returning {@code false} fails this shard's recovery: core has
         * already committed to the engine-native path by the time this is called, having found a
         * pointer written under this shard's own index.
         *
         * @param snapshotPointer the exact bytes this engine's own {@link
         *                        Engine#attemptEngineNativeSnapshot} returned at snapshot-creation
         *                        time -- opaque to core, round-tripped verbatim through repository
         *                        storage.
         * @throws IOException if materialization was attempted but failed -- surfaced as this shard's
         *                      own recovery failure, not silently downgraded to plain-empty.
         */
        boolean restore(IndexShard indexShard, Store store, byte[] snapshotPointer) throws IOException;

        /**
         * Called when a snapshot whose engine-native pointer bytes ({@link
         * Engine#attemptEngineNativeSnapshot}) were tagged with this implementation's own {@code
         * engineId} is deleted, so whatever was retained/pinned on that snapshot's behalf can be
         * released.
         *
         * <p><b>Unlike everything else on this seam, this may be called with no live {@link
         * IndexShard} anywhere in the cluster</b> -- a snapshot routinely outlives the index, or even
         * the node, that originally produced it. This is why the call carries only the opaque pointer
         * bytes, resolved through a node-level {@code engineId} registry ({@link
         * org.opensearch.index.engine.EngineNativeSnapshotReleasers}) rather than dispatched through a
         * specific shard's {@link ShardRecoveryStrategy} the way {@link #restore} is: nothing else on
         * this seam can assume a live shard exists at delete time. An implementation must be able to
         * release purely from the pointer bytes themselves (e.g. an index UUID/shard id/generation
         * embedded in the pointer, resolved directly against durable remote state), not from any
         * node-local or in-memory shard reference.
         *
         * <p>This is deliberately best-effort from core's side: if nothing is currently registered
         * under the pointer's {@code engineId} (the producing plugin has since been uninstalled, for
         * example), the snapshot's own blobs are still deleted -- core does not block a delete on
         * finding a releaser, since that would turn an uninstalled plugin into a permanent inability
         * to delete a snapshot.
         *
         * @param snapshotPointer the exact bytes this engine's own {@link
         *                        Engine#attemptEngineNativeSnapshot} returned at snapshot-creation
         *                        time.
         * @throws IOException if release was attempted but failed -- logged, does not block the
         *                      snapshot delete itself from proceeding.
         */
        void release(byte[] snapshotPointer) throws IOException;
    }
}
