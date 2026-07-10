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
}
