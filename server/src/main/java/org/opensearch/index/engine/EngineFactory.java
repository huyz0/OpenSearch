/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;

import java.io.IOException;

/**
 * Simple Engine Factory
 *
 * @opensearch.api
 */
@FunctionalInterface
@PublicApi(since = "1.0.0")
public interface EngineFactory {

    Engine newReadWriteEngine(EngineConfig config);

    /**
     * Called by {@code StoreRecovery#internalRecoverFromStore} exactly once, only when local
     * recovery expected an existing commit ({@code RecoverySource.Type.EXISTING_STORE}) but found
     * none on disk -- the point where core would otherwise fail the shard outright with {@code
     * "shard allocated for local recovery, should exist, but doesn't"}. Default {@code false}
     * (current behavior unchanged for every existing {@link EngineFactory}): this engine has
     * nothing else to try.
     *
     * <p>An {@link EngineFactory} whose durability doesn't depend on this node's own local disk
     * survival (e.g. one backed by a remote object store, addressed by its own manifest rather
     * than by any node's local allocation-id history) can override this to materialize {@code
     * store}'s local Lucene commit -- <em>and</em>, since {@code EXISTING_STORE} recovery assumes
     * a local translog already exists too and does not create one itself, a matching fresh local
     * translog (mirroring what {@code StoreRecovery#recoverEmptyStore} already does for its own,
     * unconditional case) -- from wherever its actual durable copy lives, and return {@code true}
     * so recovery proceeds normally instead of failing. Returning {@code true} without actually
     * leaving {@code store} in a state {@link Store#readLastCommittedSegmentsInfo()} can read is a
     * contract violation core cannot detect for you -- the read that follows a {@code true} return
     * will simply fail with whatever exception that leaves.
     *
     * @throws IOException if materialization was attempted but failed -- surfaced as this shard's
     *                      own recovery failure, not silently downgraded to the default "nothing to
     *                      try" outcome.
     */
    default boolean recoverMissingLocalStore(IndexShard indexShard, Store store) throws IOException {
        return false;
    }

    /**
     * Called by {@code StoreRecovery#internalRecoverFromStore} exactly once, only for a shard
     * recovering via {@code RecoverySource.Type.IN_PLACE_SPLIT_SHARD} (a child shard of an in-place
     * split, dynamic-partitioning-plan.md Phase 0) -- before this shard's local translog is created
     * or its engine is opened, the same careful ordering {@link #recoverMissingLocalStore} already
     * requires and for the identical reason (see that method's own javadoc on the stale
     * translog-UUID trap of materializing too late). Default {@code false}: this engine has nothing
     * to attach this child to.
     *
     * <p>An {@link EngineFactory} whose durability is addressed by its own manifest (the same kind
     * {@link #recoverMissingLocalStore} already describes) can override this to, in order: attach
     * this child's identity to its share of the parent shard's data by whatever mechanism it uses
     * (e.g. writing its own first manifest generation referencing the parent's data without copying
     * bytes), materialize the resulting state into {@code store}'s local Lucene commit, and create a
     * matching local translog -- then return {@code true} so recovery proceeds normally instead of
     * treating this shard as a plain new empty index. Returning {@code true} without leaving {@code
     * store} in a state {@link Store#readLastCommittedSegmentsInfo()} can read is a contract
     * violation core cannot detect for you, exactly as {@link #recoverMissingLocalStore} warns.
     *
     * @throws IOException if the attach/materialize attempt was made but failed -- surfaced as this
     *                      shard's own recovery failure, not silently downgraded to plain-empty.
     */
    default boolean recoverInPlaceSplitLocalStore(IndexShard indexShard, Store store) throws IOException {
        return false;
    }

    /**
     * Called by {@code StoreRecovery#internalRecoverFromStore} exactly once, only for a shard
     * recovering via {@code RecoverySource.Type.IN_PLACE_MERGE_SHARD} (a parent shard revived by an
     * in-place merge, dynamic-partitioning-plan.md Phase 2 item 2.1) -- the reverse of
     * {@link #recoverInPlaceSplitLocalStore} and, like it, called before this shard's local translog
     * is created or its engine is opened, for the same stale-translog-UUID-ordering reason. Default
     * {@code false}: this engine has nothing to revive the parent from.
     *
     * <p>An {@link EngineFactory} whose durability is addressed by its own manifest overrides this to
     * fold <em>both</em> retired children's current, authoritative document sets back into {@code
     * store}'s local Lucene commit and create a matching local translog, then return {@code true}. An
     * early spike proposed reviving the parent for free by taking one surviving child's own local
     * state and dropping its range filter, since both children were cloned from the same parent bundle;
     * that holds only at the instant a split commits, with zero post-split writes. Once the children
     * serve traffic each accepts its own disjoint writes into its own separate bundle/manifest (routing
     * sends each document to exactly one child by hash), so neither child's local store is a full copy
     * of the union any longer. The resolved approach folds both children together with a single {@code
     * IndexWriter#addIndexes} over each child's own <em>range-filtered</em> reader: because hash routing
     * partitions documents into disjoint ranges, each filtered reader contributes exactly that child's
     * authoritative slice (respecting that child's own deletes via {@code liveDocs}), so the union has
     * no double-counting and no cross-child version conflict to reconcile. Since the metadata is already
     * de-committed by the time the parent recovers, the retired children's ranges are carried on the
     * recovery source ({@code RecoverySource.InPlaceMergeShardRecoverySource}) rather than read from
     * {@code SplitShardsMetadata}. Returning {@code true} without leaving {@code store} in a state
     * {@link Store#readLastCommittedSegmentsInfo()} can read is a contract violation core cannot detect
     * for you, exactly as {@link #recoverMissingLocalStore} warns.
     *
     * @throws IOException if the revive/materialize attempt was made but failed -- surfaced as this
     *                      shard's own recovery failure, not silently downgraded to plain-empty.
     */
    default boolean recoverInPlaceMergeLocalStore(IndexShard indexShard, Store store) throws IOException {
        return false;
    }

    /**
     * Called by {@code StoreRecovery#recoverFromEngineNativeSnapshot} exactly once, only when the
     * shard is recovering from a snapshot this same engine originally produced via {@link
     * Engine#attemptEngineNativeSnapshot} -- the point where core has an opaque pointer this
     * engine itself wrote at snapshot-creation time and needs it turned back into this shard's
     * local Lucene commit. Called before this shard's local translog is created or its engine is
     * opened, the same ordering {@link #recoverMissingLocalStore} already requires and for the
     * identical reason. Default {@code false}: this engine never produces engine-native snapshots
     * (see {@link Engine#attemptEngineNativeSnapshot}'s own default), so it never needs to consume
     * one either.
     *
     * <p>An {@link EngineFactory} that overrides {@link Engine#attemptEngineNativeSnapshot} to
     * return a non-empty pointer must override this too -- given the exact bytes that call
     * previously returned, materialize {@code store}'s local Lucene commit and a matching local
     * translog, then return {@code true}. Returning {@code true} without leaving {@code store} in
     * a state {@link Store#readLastCommittedSegmentsInfo()} can read is a contract violation core
     * cannot detect for you, exactly as {@link #recoverMissingLocalStore} warns.
     *
     * @param snapshotPointer the exact bytes this engine's own {@link Engine#attemptEngineNativeSnapshot}
     *                        returned at snapshot-creation time -- opaque to core, round-tripped
     *                        verbatim through repository storage.
     * @throws IOException if materialization was attempted but failed -- surfaced as this shard's
     *                      own recovery failure, not silently downgraded to plain-empty.
     */
    default boolean recoverFromEngineNativeSnapshot(IndexShard indexShard, Store store, byte[] snapshotPointer) throws IOException {
        return false;
    }

    /**
     * A cheap, purely local capability check: whether this factory's engines ever produce
     * engine-native snapshots at all (i.e. whether {@link Engine#attemptEngineNativeSnapshot} can
     * ever return non-empty for a shard this factory builds). Default {@code false}, matching every
     * other engine-native default on this interface.
     *
     * <p>{@code StoreRecovery#recoverFromEngineNativeSnapshot} checks this <em>before</em> calling
     * {@link org.opensearch.repositories.Repository#getEngineNativeShardSnapshotMetadata}, which is
     * a real remote blob-existence check -- without this gate, every classic-shaped restore across
     * every {@code BlobStoreRepository}-backed deployment would pay that round trip on every shard,
     * even though the overwhelming majority of engines never produce an engine-native snapshot at
     * all. This method lets that cost be skipped entirely from a value already resolved locally
     * (the target shard's own {@link EngineFactory}), rather than always reaching out to the
     * repository to find out there was never anything to look for.
     *
     * <p>An {@link EngineFactory} that overrides {@link Engine#attemptEngineNativeSnapshot} to ever
     * return non-empty must override this to return {@code true} too, or its own snapshots will
     * fail to restore: {@code StoreRecovery} would skip the probe that finds them and fall straight
     * to the classic, copy-based restore path instead.
     */
    default boolean supportsEngineNativeSnapshots() {
        return false;
    }

    /**
     * Whether this engine already provides its own durable, remote copy of every segment it
     * writes, independent of core's own remote-store upload path ({@code
     * RemoteStoreRefreshListener}, engaged whenever {@code index.remote_store.enabled} is {@code
     * true}). Default {@code false} (current behavior unchanged for every existing {@link
     * EngineFactory}): core has no reason to believe anything but its own remote-store upload path
     * is keeping this shard's segments durable remotely, so that path stays wired in exactly as it
     * always has.
     *
     * <p>An {@link EngineFactory} that overrides this to return {@code true} is asserting that it
     * has <em>already</em> made every segment durable somewhere remote by some mechanism of its
     * own (e.g. publishing an object-store manifest referencing this shard's own segment files
     * directly, rather than delegating to core's remote-store directory/upload machinery) --
     * wiring core's remote-store upload path in on top of that would not be a correctness problem
     * (nothing about search or recovery depends on it), but would be pure wasted upload bandwidth
     * and remote storage cost for bytes nothing ever reads back through that path. This method
     * exists so such an {@link EngineFactory} can opt the shard out of that wasted work explicitly,
     * rather than silently accepting it as an unavoidable cost of using {@code
     * index.remote_store.enabled} for an unrelated reason (e.g. {@code
     * index.number_of_search_replicas}'s own prerequisite chain, which requires {@code
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
     * Called when a snapshot whose engine-native pointer bytes ({@link
     * Engine#attemptEngineNativeSnapshot}) were tagged with this factory's own {@code engineId} is
     * deleted, so whatever the engine retained/pinned on that snapshot's behalf can be released.
     * Default is a no-op: an {@link EngineFactory} that never overrides {@link
     * Engine#attemptEngineNativeSnapshot} never has anything to release either.
     *
     * <p><b>Unlike every other hook on this interface, this one may be called with no live {@link
     * IndexShard} anywhere in the cluster</b> -- a snapshot routinely outlives the index, or even
     * the node, that originally produced it. This is why the call carries only the opaque pointer
     * bytes, resolved through a node-level {@code engineId} registry rather than dispatched through
     * a specific shard's engine the way {@link #recoverFromEngineNativeSnapshot} is: nothing else
     * on this interface can assume a live shard exists at delete time. An {@link EngineFactory}
     * implementing this must be able to release purely from the pointer bytes themselves (e.g. an
     * index UUID/shard id/generation embedded in the pointer, resolved directly against durable
     * remote state), not from any node-local or in-memory shard reference.
     *
     * <p>This is deliberately best-effort from core's side: if no {@link EngineFactory} is
     * currently registered under the pointer's {@code engineId} (the producing plugin has since
     * been uninstalled, for example), the snapshot's own blobs are still deleted -- core does not
     * block a delete on finding a releaser, since that would turn an uninstalled plugin into a
     * permanent inability to delete a snapshot.
     *
     * @param snapshotPointer the exact bytes this engine's own {@link Engine#attemptEngineNativeSnapshot}
     *                        returned at snapshot-creation time.
     * @throws IOException if release was attempted but failed -- logged, does not block the
     *                      snapshot delete itself from proceeding.
     */
    default void releaseEngineNativeSnapshot(byte[] snapshotPointer) throws IOException {}
}
