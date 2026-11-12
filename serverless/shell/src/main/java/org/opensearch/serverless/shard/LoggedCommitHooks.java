/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.NodeLogCommitter.Position;

import java.io.IOException;
import java.util.List;

/**
 * Commits a writer staged rather than wrote to the register, logged in its node's write log so that a successor opens
 * from the newest one instead of the register's older commit, and replays only what came after it.
 */
public interface LoggedCommitHooks {

    /**
     * A commit read back from a writer's log.
     *
     * @param commit the commit
     * @param writer the node whose log it was in
     * @param coveredThrough where the shard's records had landed in that log when the commit began: it holds every
     *     record there and before, so a replay from that log starts after it; null when not known
     */
    record Logged(CommitManifest commit, String writer, Position coveredThrough) {
    }

    /**
     * Logs a commit a publish staged, beside the node's records. Best effort and in the background: a commit that is
     * not logged leaves a successor to open from the register and replay more, as without logging.
     *
     * @param shardId the shard
     * @param term the term it was published at
     * @param commit the staged commit
     * @param oldestPackAgeMillis how long ago the oldest node pack a staged commit names landed
     * @param coveredThrough where the shard's records had landed in this node's log when the commit began, or null
     */
    default void staged(ShardId shardId, long term, CommitManifest commit, long oldestPackAgeMillis, Position coveredThrough) {}

    /**
     * Records that the register holds a commit of this writer with this stage sequence, or a later one: the commits
     * logged up to it are no longer needed.
     *
     * @param shardId the shard
     * @param stageSequence the sequence
     */
    default void checkpointed(ShardId shardId, long stageSequence) {}

    /**
     * Returns the newest commit the shard's earlier writers logged that was staged after the register's, if its node
     * packs cannot yet have been judged unreferenced; read after the head moved and the writers' logs were fenced.
     *
     * @param shardId the shard
     * @param writers the nodes the head names as holding the shard's records
     * @param term the term this node opens it at
     * @param register the register's commit
     * @return the commit, or null to open from the register
     * @throws IOException if a log cannot be read
     */
    default Logged loggedAfter(ShardId shardId, List<String> writers, long term, CommitManifest register) throws IOException {
        return null;
    }

    /**
     * Has the next replay of a shard from a writer's log start after where a logged commit says it covers, or, with
     * null, from the start again. Set only while an open that started from that commit recovers.
     *
     * @param shardId the shard
     * @param logged the commit it opened from, or null
     */
    default void replayAfter(ShardId shardId, Logged logged) {}
}
