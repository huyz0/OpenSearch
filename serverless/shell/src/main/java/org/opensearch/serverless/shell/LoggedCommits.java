/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.LoggedCommitHooks.Logged;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.NodeLogBlob.ShardKey;
import org.opensearch.serverless.store.NodeLogCommitter;
import org.opensearch.serverless.store.NodeLogTakeover;
import org.opensearch.serverless.store.NodePackWriter;
import org.opensearch.serverless.store.WalRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The commits a node's publishes stage, logged in its write log so that a successor opens from the newest instead of the
 * register's older commit.
 *
 * <p><b>No write of its own.</b> A staged commit joins the log's open window and rides the blob of whatever the node logs
 * next, which on a node that is publishing is soon: its writes are why it publishes. It is logged under a key of its own
 * beside the shard's, so nothing that replays the shard's records ever sees it, and only the newest is kept: once one
 * lands, the one before is let go, so a successor reads one slice per shard.
 *
 * <p><b>Why a successor may open from it.</b> Its files were uploaded before it was staged, and the predecessor's log
 * keeps every record it holds and every one after it, since the log is trimmed only behind the register. Its node packs
 * are safe from collection while a manifest has named them for less than {@link NodePackWriter#COLLECTABLE_AFTER_MILLIS};
 * so a successor takes it only while its oldest pack is younger than {@link NodePackWriter#SWAP_DEADLINE_MILLIS}, the
 * same bound its writer checkpoints within, and writes it to the register before it lets go of the predecessor's log.
 *
 * <p>Best effort throughout: a commit that is never logged, or is logged too late, leaves the successor to open from the
 * register and replay more, as without logging.
 */
final class LoggedCommits {

    /** What a staged commit's key adds to its index's uuid, so the key is never a shard's. */
    static final String KEY_SUFFIX = "#commit";
    private static final String RECORD_ID = "commit";

    private final MetadataPlane plane;
    private final NodeLogCommitter committer;
    private final Map<ShardKey, ShardId> shards;
    private final Map<ShardKey, State> states = new ConcurrentHashMap<>();

    /** Per shard, the newest logged commit's slot, and the stage sequence the register holds. */
    private static final class State {
        private long checkpointed = -1L;
        private NodeLogCommitter.Position latest;
        private long latestSequence = -1L;
    }

    LoggedCommits(MetadataPlane plane, NodeLogCommitter committer, Map<ShardKey, ShardId> shards) {
        this.plane = plane;
        this.committer = committer;
        this.shards = shards;
    }

    static ShardKey keyOf(ShardId shardId) {
        return new ShardKey(shardId.getIndex().getUUID() + KEY_SUFFIX, shardId.id(), shardId.getIndexName());
    }

    void staged(ShardId shardId, long term, CommitManifest commit, long oldestPackAgeMillis, NodeLogCommitter.Position coveredThrough) {
        final ShardKey key = keyOf(shardId);
        // The log admits the key as it admits the shard's own: by this node's head of the shard.
        shards.put(key, shardId);
        final long oldestPackAt = plane.clock().getAsLong() - Math.max(0L, oldestPackAgeMillis);
        final String source;
        try {
            // A first line of what the commit needs besides itself: when its oldest pack landed, and how far into this
            // log its records reach.
            final String covered = coveredThrough == null ? "-1 -1" : coveredThrough.epoch() + " " + coveredThrough.ordinal();
            source = oldestPackAt + " " + covered + "\n" + commit.toBytes().utf8ToString();
        } catch (IOException e) {
            return;
        }
        final long sequence = commit.stageSequence();
        committer.enqueue(
            key,
            term,
            List.of(new WalRecord(RECORD_ID, source, sequence, term, 1L)),
            landed -> landed(key, sequence, landed)
        );
    }

    /** Keeps only the newest commit that landed, and none the register already holds. Holds the committer's lock. */
    private void landed(ShardKey key, long sequence, NodeLogCommitter.Position at) {
        final State state = states.computeIfAbsent(key, k -> new State());
        synchronized (state) {
            if (sequence <= state.checkpointed || sequence < state.latestSequence) {
                committer.onPublished(key, at);
                return;
            }
            if (state.latest != null) {
                committer.onPublished(key, state.latest);
            }
            state.latest = at;
            state.latestSequence = sequence;
        }
    }

    void checkpointed(ShardId shardId, long stageSequence) {
        final ShardKey key = keyOf(shardId);
        final State state = states.get(key);
        if (state == null) {
            return;
        }
        NodeLogCommitter.Position release = null;
        synchronized (state) {
            state.checkpointed = Math.max(state.checkpointed, stageSequence);
            if (state.latest != null && state.latestSequence <= stageSequence) {
                release = state.latest;
                state.latest = null;
            }
        }
        // Outside the state's lock: a landing takes the committer's lock, then the state's.
        if (release != null) {
            committer.onPublished(key, release);
        }
    }

    void forget(ShardId shardId) {
        final ShardKey key = keyOf(shardId);
        committer.forget(key);
        states.remove(key);
        shards.remove(key);
    }

    /**
     * Returns the newest commit these writers logged for a shard that was staged after the register's commit and whose
     * packs are young enough to open from.
     *
     * @param shardId the shard
     * @param writers the nodes the head names
     * @param readers each writer's log, fenced as a replay of it would be; null for a writer whose log is not read
     * @param register the register's commit
     * @return the commit, or null
     * @throws IOException if a log cannot be read
     */
    Logged loggedAfter(ShardId shardId, List<String> writers, Function<String, NodeLogTakeover> readers, CommitManifest register)
        throws IOException {
        if (register == null || writers == null || writers.isEmpty()) {
            return null;
        }
        final ShardKey key = keyOf(shardId);
        final long now = plane.clock().getAsLong();
        final Logged[] newest = { null };
        for (String writer : writers) {
            final NodeLogTakeover reader = readers.apply(writer);
            if (reader == null) {
                continue;
            }
            reader.forEachRecord(key, record -> {
                final Logged logged = parse(record, writer, now);
                if (logged != null
                    && logged.commit().stagedAfter(register)
                    && (newest[0] == null || logged.commit().stagedAfter(newest[0].commit()))) {
                    newest[0] = logged;
                }
            });
        }
        return newest[0];
    }

    private static Logged parse(WalRecord record, String writer, long now) throws IOException {
        final String source = record.source();
        final int split = RECORD_ID.equals(record.id()) ? source.indexOf('\n') : -1;
        if (split <= 0) {
            return null;
        }
        final String[] first = source.substring(0, split).split(" ");
        if (first.length != 3) {
            return null;
        }
        final long oldestPackAt;
        final long epoch;
        final long ordinal;
        try {
            oldestPackAt = Long.parseLong(first[0]);
            epoch = Long.parseLong(first[1]);
            ordinal = Long.parseLong(first[2]);
        } catch (NumberFormatException e) {
            return null;
        }
        if (now - oldestPackAt >= NodePackWriter.SWAP_DEADLINE_MILLIS) {
            // Its packs may be judged unreferenced before a register names them: open from the register.
            return null;
        }
        final CommitManifest commit = CommitManifest.fromStream(
            new ByteArrayInputStream(source.substring(split + 1).getBytes(StandardCharsets.UTF_8))
        );
        return new Logged(commit, writer, epoch < 0L ? null : new NodeLogCommitter.Position(epoch, ordinal));
    }
}
