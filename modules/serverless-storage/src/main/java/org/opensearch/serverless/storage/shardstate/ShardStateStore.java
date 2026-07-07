/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.shardstate;

import java.io.IOException;
import java.util.Optional;

/**
 * The narrow interface hiding the shard-head truth layer (rfc-serverless-metadata-plane.md
 * &sect;4/&sect;7): per-shard linearizable state (primary term, writer/compactor lease, latest
 * published manifest generation), mutated only via compare-and-swap. Object-store-backed
 * implementations use native conditional writes (S3 If-Match, GCS generation preconditions,
 * Azure ETag If-Match); {@link org.opensearch.serverless.storage.shardstate.fs.FsShardStateStore}
 * is the reference implementation used for local testing. Swapping the implementation behind
 * this interface (e.g. to a FoundationDB-backed store, &sect;7's designed escape hatch) never
 * requires touching a caller.
 */
public interface ShardStateStore {

    /**
     * Reads the current head for a shard, if it has ever been activated. Absence means the shard
     * has never been written to (rfc-serverless-metadata-plane.md &sect;4: "heads are created
     * lazily") &mdash; callers activating a shard for the first time should
     * {@link #compareAndSet} with {@code expectedVersion} absent (i.e. put-if-absent semantics).
     */
    Optional<VersionedShardHead> get(String indexUuid, int shardId) throws IOException;

    /**
     * Attempts to atomically replace the shard's head. Pass {@code expectedVersion} empty to mean
     * "the head must not exist yet" (first-ever activation, via put-if-absent); pass it non-empty
     * to mean "the head must currently be at exactly this version" (a normal CAS-based update).
     *
     * @return {@link CasResult#SUCCESS} if the write was applied, or
     *         {@link CasResult#VERSION_CONFLICT} if another actor won the race first &mdash;
     *         callers must {@link #get} again and retry against the new state, never blindly retry
     *         the same write.
     */
    CasResult compareAndSet(String indexUuid, int shardId, Optional<Long> expectedVersion, ShardHead newHead) throws IOException;
}
