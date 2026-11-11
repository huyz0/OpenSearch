/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shard;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.MergedSegmentWarmerFactory;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Opens a shard on this node for {@link ShardReconciler}: builds its index service and shard, lays down its
 * local files, fences older terms, replays its log and starts it.
 */
final class ShardOpening {

    private final ShardReconciler reconciler;

    ShardOpening(ShardReconciler reconciler) {
        this.reconciler = reconciler;
    }

    void markOpening(ShardId shardId) {
        // Publish the marker before indexServiceFor can acquire a service. Closing under the index's
        // monitor either sees this marker or finishes before that acquisition, which creates a new service.
        reconciler.opening.compute(shardId.getIndex(), (index, shards) -> {
            final Set<ShardId> current = shards == null ? new HashSet<>() : shards;
            current.add(shardId);
            return current;
        });
    }

    void unmarkOpening(ShardId shardId) {
        reconciler.opening.computeIfPresent(shardId.getIndex(), (index, shards) -> {
            shards.remove(shardId);
            return shards.isEmpty() ? null : shards;
        });
    }

    /**
     * @param knownCommit the commit to recover from, when the caller already knows it; null to ask the
     *     publisher for the current one. A frozen view has to pass it, for two reasons: the commit it
     *     wants is by definition not the current one, and its synthetic index uuid has no manifest
     *     register of its own, so asking would answer "nothing published" and start an empty shard --
     *     which is a wrong answer that looks exactly like a right one. A reader passes the one it read to
     *     decide whether it could open at all, so an open reads the register once.
     */
    IndexShard openAndStart(
        IndexMetadata indexMetadata,
        ShardId shardId,
        long shardHeadTerm,
        DiscoveryNodes nodes,
        boolean replayWal,
        CommitManifest knownCommit
    ) throws IOException {
        markOpening(shardId);
        final long[] at = { System.nanoTime() };
        final int[] step = { 0 };
        final Runnable lap = () -> {
            final long now = System.nanoTime();
            if (replayWal) {
                reconciler.writerOpenNanos[step[0]].addAndGet(now - at[0]);
            }
            step[0]++;
            at[0] = now;
        };
        try {
            final IndexService indexService = indexServiceFor(indexMetadata);
            lap.run();

            // Whether anything was published decides the recovery source, and the decision must be made
            // before the shard is created because the source is baked into its routing entry.
            final SegmentPublisher publisher = reconciler.publishers == null ? null : reconciler.publishers.apply(shardId);
            final Optional<CommitManifest> published = knownCommit != null
                ? Optional.of(knownCommit)
                : (publisher == null ? Optional.empty() : publisher.readManifest());
            final boolean restoring = published.isPresent() && published.get().files().isEmpty() == false;
            lap.run();
            if (replayWal) {
                ShardReconciler.logger.debug(
                    "opening writer {} at term {} from {}",
                    shardId,
                    shardHeadTerm,
                    published.map(m -> "manifest term " + m.term() + " log ordinal " + m.walOrdinal() + " by " + m.writer())
                        .orElse("nothing published")
                );
            }

            final ShardRouting initializing = ShardRouting.newUnassigned(
                shardId,
                true,
                restoring ? RecoverySource.ExistingStoreRecoverySource.INSTANCE : RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "serverless activation")
            ).initialize(reconciler.localNode.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

            final IndexShard shard;
            long restoredCheckpoint = org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
            published.ifPresent(manifest -> reconciler.pendingManifests.put(shardId, manifest));
            try {
                shard = indexService.createShard(
                    initializing,
                    ignored -> {},
                    RetentionLeaseSyncer.EMPTY,
                    null,
                    null,
                    null,
                    reconciler.localNode,
                    null,
                    nodes,
                    new MergedSegmentWarmerFactory(null, null, null),
                    null,
                    null
                );
            } finally {
                reconciler.pendingManifests.remove(shardId);
            }
            lap.run();
            // A failed engine is reported, not just left in place: a shard whose engine has failed still counts as
            // open, and a writer's head kept naming a node that answered "engine is closed" to everything.
            shard.addShardFailureCallback(failure -> {
                final java.util.function.BiConsumer<IndexShard, Exception> listener = reconciler.onShardFailed;
                if (listener != null) {
                    listener.accept(shard, failure.cause);
                }
            });

            // From here the IndexService holds a shard that is not yet ours. Anything that fails before
            // it is must take the shard back out, or the next attempt is refused with "already exists"
            // for as long as the process lives -- while the head still names this node, so nobody else
            // can take it either. That was a permanent single-shard outage from one object-store hiccup.
            try {
                if (restoring) {
                    // Whatever the manifest names is the truth and is read from the object store; a
                    // same-named local file is this node's own earlier output and may not be the same
                    // bytes. Local commits go too, so the history bootstrap below starts from the
                    // published segments_N rather than from a stale local one that happens to be newer.
                    dropLocalFilesNamedBy(shard.store().directory(), published.get());
                    // A translog is still needed, because recovery reads one and a node taking a shard
                    // over has none.
                    restoredCheckpoint = bootstrapTranslogFor(shard);
                }
                lap.run();

                List<String> armedWriters = null;
                // Whether this open closed the shard's own log and replayed it, as a writer of its node's log: once what it
                // replayed is published, the head can say the log is quiet, and later opens skip it.
                boolean closedShardLog = false;
                if (replayWal && reconciler.walStores != null) {
                    // Armed for exactly the window in which the engine is built and recovery runs, which is
                    // the only window in which core will accept operations carrying their own sequence
                    // numbers.
                    final org.opensearch.serverless.store.WalStore log = reconciler.wal(shardId);
                    reconciler.pendingReplay.put(shardId, log);
                    if (reconciler.nodeLog != null) {
                        armedWriters = reconciler.nodeLogWriters.remove(shardId.getIndexName() + "#" + shardId.id());
                        reconciler.pendingNodeLogReplay.put(
                            shardId,
                            new ShardReconciler.NodeLogReplay(armedWriters == null ? List.of() : armedWriters, shardHeadTerm)
                        );
                    }
                    // Fence the log before recovery reads anything: every older term, so a predecessor that has
                    // not noticed it lost this shard can write nothing after what is replayed here, and this
                    // term, so an earlier writer of it -- the instance a same-term reopen replaced -- can write
                    // nothing either. Whatever either of them landed is before a fence and is replayed; nothing
                    // can land after one. See WalStore#establish and serverless/rfc-fencing-closure.md.
                    //
                    // The takeover fenced the older terms already, at the compare-and-swap; doing it again here
                    // costs a listing for a term already fenced, and covers a shard reopened without passing
                    // through an acquisition.
                    final Long fencedAt = reconciler.olderTermsFenced.remove(shardId.getIndexName() + "#" + shardId.id());
                    // Unless the head this node took says the shard's own log is quiet and this node writes its node's
                    // log: then every term of the shard's log is closed and published already, and nothing will be written
                    // there at this term, so there is nothing to fence or replay -- and the listings that would find so are
                    // most of what an open cost in listings.
                    final Long quietAt = reconciler.shardLogQuietAt.remove(shardId.getIndexName() + "#" + shardId.id());
                    if (reconciler.nodeLog != null && quietAt != null && quietAt == shardHeadTerm) {
                        log.beginQuiet(shardHeadTerm);
                    } else {
                        if (fencedAt == null || fencedAt != shardHeadTerm) {
                            log.fenceOlderTerms(shardHeadTerm);
                        }
                        final java.util.function.BiPredicate<ShardId, Long> check = reconciler.ownsAtTerm;
                        log.establish(shardHeadTerm, check == null ? null : () -> check.test(shardId, shardHeadTerm));
                        closedShardLog = reconciler.nodeLog != null;
                    }
                }
                lap.run();
                try {
                    shard.markAsRecovering("serverless-store", new RecoveryState(initializing, reconciler.localNode, null));
                    final PlainActionFuture<Boolean> recovered = PlainActionFuture.newFuture();
                    shard.recoverFromStore(recovered);
                    if (Boolean.TRUE.equals(recovered.actionGet()) == false) {
                        throw new IOException("recovery from store reported failure for " + shardId);
                    }
                } finally {
                    reconciler.pendingReplay.remove(shardId);
                    reconciler.pendingNodeLogReplay.remove(shardId);
                }
                lap.run();

                final ShardRouting started = initializing.moveToStarted();
                // The applying version is the SHARD-HEAD TERM, not the projected view's version. When a per-node
                // counter is used here, the update is silently ignored, because ReplicationTracker gates on >
                // against a value that may have crossed a
                // node boundary.
                shard.updateShardState(
                    started,
                    shardHeadTerm,
                    null,
                    shardHeadTerm,
                    Set.of(started.allocationId().getId()),
                    new IndexShardRoutingTable.Builder(shardId).addShard(started).build(),
                    nodes
                );

                if (replayWal && reconciler.walStores != null) {
                    replayRecordsWithoutSequenceIdentity(shard, shardId);
                }
                if (replayWal == false) {
                    // A reader is a reader before it is open. To the write path a shard open and not a reader is this
                    // node's writer, and on an ingest node a reader's engine accepts writes: one that landed between
                    // the two was applied at the reader's commit, numbered from there, and acknowledged -- a fleet
                    // run found a sequence number given twice that way, which replay would have dropped one of.
                    reconciler.readers.add(shardId);
                }
                lap.run();
                if (replayWal) {
                    reconciler.writerOpens.incrementAndGet();
                    if (restoring
                        && shard.seqNoStats().getMaxSeqNo() <= restoredCheckpoint
                        && published.get().walOrdinal() >= 0L
                        && published.get().digest().isEmpty() == false) {
                        // Nothing was replayed past the published commit, so that commit already holds every
                        // operation this shard has. Recorded so the first publish is not one that says so again.
                        // Not for a commit published before commits recorded their log position and digest: the
                        // publish is what records them, and a reader check and the rollup janitor wait on it.
                        reconciler.publishedThroughAtOpen.put(shardId, restoredCheckpoint);
                    } else {
                        reconciler.publishedThroughAtOpen.remove(shardId);
                    }
                }
                if (closedShardLog) {
                    if (shard.seqNoStats().getMaxSeqNo() <= restoredCheckpoint) {
                        // Nothing replayed past the commit: the shard's own log holds nothing it lacks.
                        try {
                            reconciler.nodeLog.shardLogQuiet(shardId, shardHeadTerm);
                        } catch (IOException | RuntimeException e) {
                            // The next owner closes the shard's log again, which costs listings, never records.
                            ShardReconciler.logger.debug("could not record that " + shardId + "'s own log is quiet", e);
                        }
                    } else {
                        reconciler.shardLogQuietOnPublish.put(shardId, shardHeadTerm);
                    }
                }
                reconciler.open.put(shardId, shard);
                final Long fromNodeLog = reconciler.nodeLogReplayed.remove(shardId);
                if (fromNodeLog != null && fromNodeLog > 0 && shard.seqNoStats().getMaxSeqNo() > restoredCheckpoint) {
                    // Only when the replay applied something past the commit it opened from: a log read up to its first
                    // free slot still names records a writer published before it let go, and replaying those changes
                    // nothing. Published before it serves, so what it replayed is in a commit and no later takeover depends on the
                    // log that held it: a predecessor's log names this shard's records only until the shard is taken.
                    try {
                        // What the commit is certain to hold, read before its flush, as a publish from the loop reads it.
                        final long covered = shard.seqNoStats().getLocalCheckpoint();
                        reconciler.publish(shardId, shardHeadTerm);
                        // This is the node's first publish of the shard: recorded, so the loop does not publish the same
                        // operations again under the same term as soon as the take returns.
                        reconciler.publishedThroughAtOpen.put(shardId, covered);
                    } catch (IOException | RuntimeException e) {
                        reconciler.open.remove(shardId);
                        throw e;
                    }
                }
                final ShardReconciler.NodeLogHooks hooks = reconciler.nodeLog;
                if (armedWriters != null && hooks != null) {
                    try {
                        hooks.replayedAndPublished(shardId, shardHeadTerm);
                    } catch (IOException | RuntimeException e) {
                        // The head goes on naming logs that hold nothing more of this shard: a successor reads them for
                        // nothing, which costs reads, never records.
                        ShardReconciler.logger.debug("could not stop " + shardId + "'s head naming the logs it replayed", e);
                    }
                }
                return shard;
            } finally {
                reconciler.nodeLogReplayed.remove(shardId);
                final var remainingReplay = reconciler.replayedRecords.remove(shardId);
                if (remainingReplay != null) {
                    remainingReplay.close();
                }
            }
        } catch (Exception e) {
            // Service creation can succeed before mapping, manifest lookup or shard creation fails.
            // Look it up here because indexServiceFor itself can throw after installing the service.
            abandon(reconciler.indicesService.indexService(shardId.getIndex()), shardId, e);
            throw e;
        } finally {
            unmarkOpening(shardId);
        }
    }

    /** Creates the index's service or refreshes its metadata, under the index's monitor. */
    IndexService indexServiceFor(IndexMetadata indexMetadata) throws IOException {
        final Index index = indexMetadata.getIndex();
        // Locked per index, because this is a check-then-act and shards of one index now open at the same
        // time. Sequentially it was safe by luck; the moment the search fan-out became concurrent, three
        // shards of one index raced here and two lost with ResourceAlreadyExistsException -- reported as
        // two unreachable shards, which reads like a routing problem and is not one. IndexService.createShard
        // is itself synchronized, so only this acquisition needs guarding and shard opening stays parallel.
        while (true) {
            final Object lock = reconciler.indexLocks.computeIfAbsent(index, ignored -> new Object());
            synchronized (lock) {
                if (reconciler.indexLocks.get(index) != lock) {
                    // Evicted between the lookup and the acquisition; the current one is the monitor.
                    continue;
                }
                IndexService existing = reconciler.indicesService.indexService(index);
                if (existing == null) {
                    existing = reconciler.indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
                    existing.updateMapping(null, indexMetadata);
                } else {
                    // The IndexService is created once per index but shards arrive one at a time, so by the
                    // time the second shard opens its metadata is stale -- in particular its per-shard
                    // primary terms. A shard created from stale metadata starts at the old term and then
                    // gets updateShardState at the new one, which the data plane rejects: "term is only
                    // increased as part of primary promotion". Found by the first multi-shard test; every
                    // earlier one had a single shard.
                    final IndexMetadata current = existing.getMetadata();
                    // At the same settings version the settings are, by definition, the same ones -- what
                    // this call carries is the terms. Handing core different settings at an unchanged
                    // version is an assertion failure in IndexService#updateMetadata, and the store type
                    // set on open is exactly such a difference once a settings refresh has been applied.
                    final IndexMetadata.Builder refreshed = current.getSettingsVersion() == indexMetadata.getSettingsVersion()
                        ? IndexMetadata.builder(indexMetadata).settings(current.getSettings())
                        : IndexMetadata.builder(indexMetadata);
                    existing.updateMetadata(current, keepingHigherTerms(current, refreshed));
                }
                return existing;
            }
        }
    }

    /**
     * Builds the metadata to hand an index's service, keeping each shard's term at least where the service has it.
     *
     * <p>A caller knows the term of the shard it is opening and fills the others with a default, and a settings
     * refresh knows none of them. Applied as given, an update pushes the terms of the index's other shards back
     * down: two shards opening at once each lowered the other's before it was created, so it started at the old
     * term and was refused when promoted to the one it won. Terms only ever rise within one incarnation of an
     * index, so the higher of the two is the right one.
     */
    static IndexMetadata keepingHigherTerms(IndexMetadata current, IndexMetadata.Builder incoming) {
        final IndexMetadata built = incoming.build();
        final int shards = Math.min(current.getNumberOfShards(), built.getNumberOfShards());
        boolean raised = false;
        for (int shard = 0; shard < shards; shard++) {
            if (current.primaryTerm(shard) > built.primaryTerm(shard)) {
                incoming.primaryTerm(shard, current.primaryTerm(shard));
                raised = true;
            }
        }
        return raised ? incoming.build() : built;
    }

    /** Takes a shard that never became ours back out of its IndexService, keeping the cause's own report. */
    void abandon(IndexService indexService, ShardId shardId, Exception cause) {
        try {
            if (indexService != null && indexService.hasShard(shardId.id())) {
                indexService.removeShard(shardId.id(), "activation failed: " + cause.getMessage());
            }
        } catch (Exception e) {
            cause.addSuppressed(e);
        }
        try {
            unmarkOpening(shardId);
            reconciler.closeIndexIfUnused(shardId.getIndex(), "activation failed: " + cause.getMessage(), false);
        } catch (Exception e) {
            cause.addSuppressed(e);
        }
    }

    /**
     * Applies the records recovery could not replay for itself: those written before the log carried
     * sequence numbers. Everything else was replayed during recovery, as the operation it originally was,
     * by ServerlessWriterEngine -- which is what makes _seq_no survive a failover.
     *
     * <p>These go in the old way, as fresh primary operations, because a record that does not say which
     * operation it was cannot be replayed as that operation. They therefore get new sequence numbers,
     * exactly as every replayed record used to. Dropping them instead would turn a format change into
     * silent data loss.
     */
    void replayRecordsWithoutSequenceIdentity(IndexShard shard, ShardId shardId) throws IOException {
        final var retained = reconciler.replayedRecords.remove(shardId);
        try (
            var spool = retained != null
                ? retained
                : org.opensearch.serverless.engine.ReplaySpool.captureExclusive(
                    shard.shardPath().getDataPath().resolve("wal-replay"),
                    consumer -> reconciler.wal(shardId).forEachReplayableAfterFencing(consumer)
                )
        ) {
            final int[] replayed = { 0 };
            spool.forEachLegacy(record -> {
                if (record.isDeletion()) {
                    // Replayed in order with the writes, which is the only thing that makes the result
                    // correct: a document written, deleted, and written again must end up present, and a
                    // document written and deleted must end up gone. Applying all the writes and then all
                    // the deletes would get the first case wrong.
                    final var result = shard.applyDeleteOperationOnPrimary(
                        org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                        record.id(),
                        org.opensearch.index.VersionType.INTERNAL,
                        org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
                        0
                    );
                    ensureLegacyReplaySucceeded(record, result);
                    replayed[0]++;
                    return;
                }
                final var result = shard.applyIndexOperationOnPrimary(
                    org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                    org.opensearch.index.VersionType.INTERNAL,
                    new org.opensearch.index.mapper.SourceToParse(
                        shardId.getIndexName(),
                        record.id(),
                        new org.opensearch.core.common.bytes.BytesArray(record.source()),
                        org.opensearch.common.xcontent.XContentType.JSON
                    ),
                    org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0,
                    org.opensearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                    false
                );
                ensureLegacyReplaySucceeded(record, result);
                replayed[0]++;
            });
            if (replayed[0] > 0) {
                // The replayed operations sit in the translog's write buffer, a page charged to the request breaker,
                // until synced; the object-store log is the durability, this gives the page back.
                shard.sync();
                shard.refresh("serverless-wal-replay");
            }
        }
    }

    static void ensureLegacyReplaySucceeded(
        org.opensearch.serverless.store.WalRecord record,
        org.opensearch.index.engine.Engine.Result result
    ) throws IOException {
        if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
            // A per-document failure is returned, not thrown. Continuing recovery would publish a
            // commit without this logged operation and eventually truncate the only durable copy.
            throw new IOException(
                "could not replay legacy logged operation [" + record.id() + "]: " + result.getResultType(),
                result.getFailure()
            );
        }
    }

    /**
     * Removes every local file the published commit names, and every local commit.
     *
     * <p>Local disk is this node's own output and it may be stale: the counter that mints segment names
     * travels in the commit, so a successor restoring this node's last published commit mints the same
     * next name this node minted for a segment it flushed and never published. A directory that preferred
     * the local copy would serve those bytes under the successor's manifest. Everything the manifest names
     * is therefore read from the object store, and every local {@code segments_N} goes so the history
     * bootstrap starts from the published commit rather than from a local one that happens to be newer.
     * Whatever else is local and unreferenced, Lucene removes on open.
     */
    static void dropLocalFilesNamedBy(org.apache.lucene.store.Directory directory, CommitManifest manifest) throws IOException {
        for (String name : directory.listAll()) {
            final boolean commit = name.startsWith("segments_") || name.startsWith("pending_segments_");
            if (commit == false && manifest.files().containsKey(name) == false) {
                continue;
            }
            try {
                // The block-cache directory ignores a delete of a name that is remote only, so this
                // removes exactly the local copies and leaves the published files where they are.
                directory.deleteFile(name);
            } catch (java.nio.file.NoSuchFileException | java.io.FileNotFoundException ignored) {
                // remote-only, which is the state we want
            }
        }
    }

    /**
     * Gives a restored commit a fresh, empty translog.
     *
     * <p>Restoring segments is not enough on its own: recovery reads a translog whose UUID matches the
     * commit, and a node taking a shard over has neither. Without this the shard fails recovery with a
     * corrupt-translog error naming a file that was never written.
     *
     * <p>The sequence is the one {@code StoreRecovery} uses for snapshot restore, for the same reason —
     * segments arrived from somewhere other than a peer, so the history has to be re-established rather
     * than continued. Operations the previous writer had accepted but not committed are replayed from the
     * write-ahead log, never from a local translog: this deletes the local one, on every open, which is
     * why fsyncing it per operation buys nothing.
     */
    /** Returns the local checkpoint of the restored commit, the one the new translog starts from. */
    long bootstrapTranslogFor(IndexShard shard) throws IOException {
        shard.store().bootstrapNewHistory();
        final org.apache.lucene.index.SegmentInfos segmentInfos = shard.store().readLastCommittedSegmentsInfo();
        final long localCheckpoint = Long.parseLong(
            segmentInfos.userData.get(org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY)
        );
        final String translogUUID = segmentInfos.getUserData().get(org.opensearch.index.translog.Translog.TRANSLOG_UUID_KEY);
        org.opensearch.index.translog.Translog.createEmptyTranslog(
            shard.shardPath().resolveTranslog(),
            shard.shardId(),
            localCheckpoint,
            shard.getPendingPrimaryTerm(),
            translogUUID,
            java.nio.channels.FileChannel::open
        );
        return localCheckpoint;
    }
}
