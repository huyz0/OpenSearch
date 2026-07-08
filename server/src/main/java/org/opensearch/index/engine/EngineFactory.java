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
}
