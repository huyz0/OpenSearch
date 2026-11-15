/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.node;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.engine.OverlayReadOnlyEngine;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.WalRecord;

import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * What this node tells the search nodes of the shards it writes, and asks of the writers of the shards it
 * serves. The transport layer, above this package, implements it once the transport has bound; until then
 * {@link NodeContext#push()} is null and every caller does without.
 */
public interface ReaderPush {

    /**
     * Takes the cluster settings that switch tail overlays and refresh visibility.
     *
     * @param cluster the cluster settings just read
     */
    void applyCluster(Settings cluster);

    /**
     * Tells a shard's search nodes about a commit just published.
     *
     * @param shardId the shard
     * @param commit the commit
     * @param committedCheckpoint the commit's local checkpoint, read only if the tail is being pushed
     */
    void published(ShardId shardId, CommitManifest commit, LongSupplier committedCheckpoint);

    /**
     * Keeps acknowledged records for the shard's search nodes, which may search them before they are published.
     *
     * @param shardId the shard
     * @param records the records just acknowledged
     */
    void acked(ShardId shardId, List<WalRecord> records);

    /**
     * Makes a refreshed shard visible as far as the refresh mode promises.
     *
     * @param shardId the shard just refreshed
     * @param searchedOnlyHere whether no other node searches the shard
     * @return false if the write is not visible as far as the mode promises
     */
    boolean awaitVisible(ShardId shardId, BooleanSupplier searchedOnlyHere);

    /**
     * Whether a reader brings itself up to its writer's acknowledged writes before it answers.
     *
     * @return true if readers are made fresh
     */
    boolean grantsFreshness();

    /**
     * Brings a reader up to what its writer has acknowledged.
     *
     * @param shardId the reader's shard
     * @param writer the shard's writer
     * @param commitCheckpoint the local checkpoint of the commit the reader has open
     * @param applyPending applies what was fetched
     * @return false if the reader could not be brought up to date
     */
    boolean ensureFresh(ShardId shardId, String writer, long commitCheckpoint, Runnable applyPending);

    /**
     * The commit a shard's writer pushed here, taken once.
     *
     * @param indexName the index
     * @param indexUuid the index uuid
     * @param shard the shard number
     * @return the pushed commit, if one is waiting
     */
    Optional<CommitManifest> takePushed(String indexName, String indexUuid, int shard);

    /**
     * The newer of the register and the commit the shard's writer staged after it.
     *
     * @param register the commit the register names
     * @param indexName the index
     * @param indexUuid the index uuid
     * @param shard the shard number
     * @return the newest commit known
     */
    Optional<CommitManifest> newerThan(Optional<CommitManifest> register, String indexName, String indexUuid, int shard);

    /**
     * Subscribes a reader just opened to its writer's next writes.
     *
     * @param shardId the reader's shard
     * @param writer the shard's writer
     * @param localCheckpoint the reader's local checkpoint
     */
    void opened(ShardId shardId, String writer, long localCheckpoint);

    /**
     * The tail a read-only engine searches on top of its commit.
     *
     * @return the tail
     */
    OverlayReadOnlyEngine.Tail tail();
}
