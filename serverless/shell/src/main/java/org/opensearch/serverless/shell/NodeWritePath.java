/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

/**
 * Writes documents for {@link ServerlessNode}: single index and delete operations and batches, each applied under
 * the shard's fence and appended to its log before it is acknowledged.
 */
public final class NodeWritePath {

    private final ServerlessNode node;

    NodeWritePath(ServerlessNode node) {
        this.node = node;
    }

    /**
     * Indexes a document durably: the write reaches the object store's log before it is acknowledged.
     *
     * <p>This is the ordering that closes the unpublished-write window — the record is in the log before
     * this method returns, and returning is what acknowledges the write. What changed is <em>where</em> in the method
     * the append happens: it used to come first, and now it comes after the engine has run, because the
     * sequence number the record carries does not exist until then. See {@link #appendOrRelease} for what
     * happens when the append is the thing that fails.
     *
     * @param shardId the shard to write to
     * @param id the document id
     * @param source the document source
     * @return what the engine assigned this write
     * @throws java.io.IOException if the shard is not held here, or the write fails
     */
    public WriteOutcome index(org.opensearch.core.index.shard.ShardId shardId, String id, String source) throws java.io.IOException {
        return index(shardId, id, source, org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO, 0L);
    }

    /**
     * Indexes a document, optionally only if it is still at the sequence number the caller expects.
     *
     * <p><b>The compare-and-swap is the engine's own, not a re-implementation.</b> {@code ifSeqNo} and
     * {@code ifPrimaryTerm} are passed straight through to
     * {@code IndexShard#applyIndexOperationOnPrimary}, where core compares them against the live version
     * map (falling back to a Lucene doc-values lookup) under the per-document lock it already takes. A
     * mismatch is a {@code VersionConflictEngineException}, which this method rethrows so the caller
     * cannot mistake a lost race for a completed write.
     *
     * @param shardId the shard to write to
     * @param id the document id
     * @param source the document source
     * @param ifSeqNo the sequence number the document must currently be at, or
     *     {@link org.opensearch.index.seqno.SequenceNumbers#UNASSIGNED_SEQ_NO} for an unconditional write
     * @param ifPrimaryTerm the primary term the document must currently be at, or 0 when unconditional
     * @return what the engine assigned this write
     * @throws org.opensearch.index.engine.VersionConflictEngineException if the condition was not met
     * @throws java.io.IOException if the shard is not held here, or the write fails
     */
    public WriteOutcome index(org.opensearch.core.index.shard.ShardId shardId, String id, String source, long ifSeqNo, long ifPrimaryTerm)
        throws java.io.IOException {
        return index(shardId, id, source, ifSeqNo, ifPrimaryTerm, false);
    }

    /**
     * Indexes a document, optionally only if no document with that id exists.
     *
     * <p><b>Create is one constant, not a feature.</b> Core's own {@code _create} is an ordinary index
     * operation at {@code Versions.MATCH_DELETED} — the engine compares against its live version map under
     * the per-document lock it already takes, and a document that is present fails the comparison. Nothing
     * here re-implements exists-checking, and in particular nothing does a read first, which would be a
     * race rather than a check.
     *
     * <p>A lost race arrives as a {@code VersionConflictEngineException}, the same type a failed
     * {@code if_seq_no} produces, and reaches the caller as a 409 by the same path.
     *
     * @param shardId the shard to write to
     * @param id the document id
     * @param source the document source
     * @param ifSeqNo the sequence number the document must currently be at, or
     *     {@link org.opensearch.index.seqno.SequenceNumbers#UNASSIGNED_SEQ_NO} for an unconditional write
     * @param ifPrimaryTerm the primary term the document must currently be at, or 0 when unconditional
     * @param requireAbsent whether the write must fail if the document already exists
     * @return what the engine assigned this write
     * @throws org.opensearch.index.engine.VersionConflictEngineException if the condition was not met
     * @throws java.io.IOException if the shard is not held here, or the write fails
     */
    public WriteOutcome index(
        org.opensearch.core.index.shard.ShardId shardId,
        String id,
        String source,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent
    ) throws java.io.IOException {
        return index(shardId, id, source, ifSeqNo, ifPrimaryTerm, requireAbsent, true);
    }

    /**
     * Indexes a document, optionally growing the mapping to fit it.
     *
     * <p><b>Dynamic mapping, which this shell did not have.</b> When a document carries a field the mapping
     * does not describe, core's engine does not index it and does not fail either: it returns
     * {@code MAPPING_UPDATE_REQUIRED} along with the mapping addition that would let the write succeed. A
     * classic node sends that addition to the cluster manager, waits for it to be applied, and retries.
     * There is no cluster manager here, and nothing was doing the equivalent — so an index created without a
     * complete mapping could not accept a single document, and every write of an undeclared field answered
     * with a 500. Every test in this repository happened to declare its mappings, which is why it went unseen.
     *
     * <p>The equivalent here is the one the mapping update uses: merge the addition into the descriptor under a
     * compare-and-swap, apply it to the open shard, and retry once. Concurrent writers adding different
     * fields both win, because that is what the compare-and-swap is for.
     *
     * @param shardId the shard to write to
     * @param id the document id
     * @param source the document source
     * @param ifSeqNo the sequence number the document must be at, or unassigned
     * @param ifPrimaryTerm the primary term the document must be at, or 0
     * @param requireAbsent whether the write must fail if the document already exists
     * @param mayGrowMapping whether a required mapping update should be applied and the write retried
     * @return what the engine assigned this write
     * @throws java.io.IOException if the shard is not held here, or the write fails
     */
    public WriteOutcome index(
        org.opensearch.core.index.shard.ShardId shardId,
        String id,
        String source,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent,
        boolean mayGrowMapping
    ) throws java.io.IOException {
        node.ensureStarted();
        try {
            node.fences.awaitHold(shardId);
            // Admitted before anything is applied, so a write refused for back-pressure is in neither the engine nor the log.
            try (org.opensearch.common.lease.Releasable admitted = node.writeBackpressure.admit()) {
                // Under the shard's fence from before the engine applies the operation until after it is
                // acknowledged. See appendOrRelease for the interleaving this closes.
                final java.util.concurrent.locks.Lock fence = node.fences.enterWritePath(shardId);
                try (org.opensearch.common.lease.Releasable guard = node.reconciler.writeGuard(shardId)) {
                    return indexUnderFence(shardId, id, source, ifSeqNo, ifPrimaryTerm, requireAbsent, mayGrowMapping);
                } finally {
                    fence.unlock();
                }
            }
        } catch (WriteHold.Forwarded forwarded) {
            return forwarded.target.index(shardId, id, source, ifSeqNo, ifPrimaryTerm, requireAbsent, mayGrowMapping)
                .forwardedBy(forwarded.target);
        }
    }

    WriteOutcome indexUnderFence(
        org.opensearch.core.index.shard.ShardId shardId,
        String id,
        String source,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent,
        boolean mayGrowMapping
    ) throws java.io.IOException {
        final var shard = node.reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot index into " + shardId + ": not open on " + node.nodeName);
        }
        if (node.reconciler.isReader(shardId)) {
            throw new java.io.IOException("cannot index into " + shardId + ": it is open as a reader");
        }
        node.fences.refuseIfWriteFenced(shardId);
        // Before the operation as well as after it: the read that confirms the incarnation brings the shard's mapping
        // and settings up to the descriptor, so a change made on another node is in force for the first operation
        // after it rather than the one after that. Within the fence window this is free; see INCARNATION_FENCE_MILLIS.
        node.fences().ensureIncarnationLive(shardId);
        node.shards().markUsed(shardId);
        // Apply first, then log. This is the reverse of what this path used to do, and the reason is the
        // whole point of carrying sequence numbers: the engine assigns one, so there is nothing worth
        // recording until it has run. The durability contract the javadoc above states is unchanged --
        // the record is in the log before the write is acknowledged, and acknowledgement is this method
        // returning. It is also the order classic OpenSearch uses internally: Lucene, then the translog,
        // then fsync.
        final var result = shard.applyIndexOperationOnPrimary(
            requireAbsent ? org.opensearch.common.lucene.uid.Versions.MATCH_DELETED : org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
            org.opensearch.index.VersionType.INTERNAL,
            new org.opensearch.index.mapper.SourceToParse(
                shardId.getIndexName(),
                id,
                new org.opensearch.core.common.bytes.BytesArray(source),
                org.opensearch.common.xcontent.XContentType.JSON
            ),
            ifSeqNo,
            ifPrimaryTerm,
            org.opensearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false
        );
        if (result.getResultType() == org.opensearch.index.engine.Engine.Result.Type.FAILURE) {
            // Core returns parsing and conditional-write failures as values. Preserve their native
            // exceptions so a client error keeps its status instead of becoming a generic I/O error.
            if (result.getFailure() instanceof RuntimeException failure) {
                throw failure;
            }
        }
        if (result.getResultType() == org.opensearch.index.engine.Engine.Result.Type.MAPPING_UPDATE_REQUIRED && mayGrowMapping) {
            // The document named a field the mapping does not have. Grow the mapping and write it again;
            // once, because a second MAPPING_UPDATE_REQUIRED after the update has been applied means
            // something other than a missing field, and retrying forever would hide it.
            node.growMapping(shardId, result.getRequiredMappingUpdate());
            return indexUnderFence(shardId, id, source, ifSeqNo, ifPrimaryTerm, requireAbsent, false);
        }
        if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
            // This is a value, not a throw. A caller that ignores it indexes nothing
            // and reports no error.
            throw new java.io.IOException("indexing " + id + " returned " + result.getResultType(), result.getFailure());
        }
        appendOrRelease(
            shardId,
            shard,
            java.util.List.of(
                new org.opensearch.serverless.store.WalRecord(id, source, result.getSeqNo(), result.getTerm(), result.getVersion())
            )
        );
        // The local translog is never replayed by any open path -- a shard opens from a published commit
        // and its object-store log -- so this sync is not for durability; the log append above is. It is
        // for memory: the translog holds its write buffer, a 16 KB page charged to the request breaker,
        // until it is synced, and a node with a one-kilobyte request limit found every later search
        // refused by a page a write had left behind. One local fsync per acknowledged write is the price,
        // and it is not an object-store request.
        syncIfStillOpen(shard);
        // The edge. Fired after the write is durable and applied, so a listener that publishes on it
        // can never publish a commit describing an operation the caller was not told about.
        node.signals.wrote(shardId);
        return new WriteOutcome(result.getSeqNo(), result.getTerm(), result.getVersion(), result.isCreated(), true);
    }

    /**
     * Syncs the shard's local translog, unless the shard was released after its record reached the log.
     *
     * <p>The record is durable in the object store by then, and a successor replays it; the local translog is
     * only this node's copy, and a shard released for a lapsed lease has no further use for it. Failing the
     * write here would report as refused a write that is in the log.
     */
    static void syncIfStillOpen(org.opensearch.index.shard.IndexShard shard) throws java.io.IOException {
        try {
            shard.sync();
        } catch (org.opensearch.index.shard.IndexShardClosedException e) {
            // Released after the append; see above.
        }
    }

    /**
     * Appends a record to the log, and fences the shard against further writes if that fails.
     *
     * <p><b>Why a failed append cannot simply be reported to the caller.</b> By the time this runs, the
     * operation is already in the engine. Reporting failure while leaving it there would make the refusal
     * a lie: this node would go on serving the document and would eventually publish it, so a caller told
     * the write failed would find it present, permanently. The refusal is made true by never publishing
     * that engine state and rebuilding the shard from the log, which does not contain the operation.
     *
     * <p><b>Fenced rather than released, which is a brownout decision.</b> This used to close the shard
     * on the spot, and a store that was merely slow for a moment took every writer shard on the node
     * offline for reads too -- a fully cached copy answering "not open" to a get. The rule is that
     * reads degrade to staleness and writes to rejection. So the shard stays open and readable, refuses
     * every further write ({@link WriteFences#refuseIfWriteFenced}), is skipped by publication, and is closed and
     * re-acquired from its log by the next heartbeat that reaches the store. Until then a read from this
     * node's own copy can see the operation the caller was told failed; that window is one renewal
     * interval, and the alternative was no reads at all.
     *
     * <p><b>The fence around this method is what makes acknowledgement safe against a voluntary
     * release.</b> Idle release, eviction and a clean shutdown all run publish, release the head, close.
     * With no exclusion against the write path, a write applied after the publish and appended after the
     * head release -- by which time a successor may have acquired and sealed the log -- was acknowledged
     * and never replayed. The caller holds the shard's read fence from before applying to after the
     * check below, and the release holds the write fence around the whole sequence, so the two cannot
     * interleave: a write either completes before the release begins, or finds no shard after it ends.
     *
     * @param shardId the shard being written to
     * @param shard the open shard
     * @param records the records to append, in order; empty is a no-op
     * @throws java.io.IOException if the append failed, after the shard has been fenced
     */
    void appendOrRelease(
        org.opensearch.core.index.shard.ShardId shardId,
        org.opensearch.index.shard.IndexShard shard,
        java.util.List<org.opensearch.serverless.store.WalRecord> records
    ) throws java.io.IOException {
        final var wal = node.reconciler.wal(shardId);
        if (wal == null || records.isEmpty()) {
            return;
        }
        node.fences.ensureOwnLeaseIsStillValid(shardId);
        if (wal.begun(shard.getOperationPrimaryTerm()) == false) {
            // Not this term's writer: only a writer's open begins its term, after confirming the head names it. Appending
            // anyway would begin the term here, unchecked, and fence the real writer's records off behind this one.
            node.writeFenced.add(shardId);
            throw new java.io.IOException(
                "could not log to "
                    + shardId
                    + ": this node is not writing its log at term "
                    + shard.getOperationPrimaryTerm()
                    + " -- never began it here, or stopped after a failed append; the write was not acknowledged"
            );
        }
        final long appendStarted = System.nanoTime();
        try {
            // Through the group committer rather than straight to the log: concurrent writes to this shard
            // share one PUT, and this caller returns only once the blob carrying its own records has
            // landed. The two lease checks around this call are unchanged and remain per-caller, so every
            // member of a group independently establishes that its record sits ahead of any seal.
            if (node.nodeLogEnabled) {
                // One blob per window for every shard of this node, rather than one per shard.
                node.startup().nodeLog(node.metadataPlane).commit(shardId, shard.getOperationPrimaryTerm(), records);
            } else {
                node.walCommitter.commit(shardId, shard.getOperationPrimaryTerm(), records, wal::append);
            }
            node.writeBackpressure.onAppend(System.nanoTime() - appendStarted, true);
        } catch (Exception | Error e) {
            node.writeBackpressure.onAppend(System.nanoTime() - appendStarted, false);
            node.writeFenced.add(shardId);
            if (e instanceof Error fatal) {
                throw fatal;
            }
            throw new java.io.IOException(
                "could not log "
                    + records.size()
                    + " operation(s) to "
                    + shardId
                    + "; the write was not acknowledged, the shard now refuses writes and will be reopened from its log",
                e
            );
        }
        // The PUT landed, so the record is durable: a successor fences this term before it replays, and a
        // fence occupies the slot this writer would have written next, so a record that landed is ahead of
        // every fence and every successor replays it. It is acknowledged whatever the lease now says --
        // refusing it was what turned a durable record into one "refused" and later replayed. A lapsed
        // lease still releases the shard, so nothing further is written here. See
        // serverless/rfc-fencing-closure.md.
        node.fences.releaseIfLeaseLapsed(shardId);
        // After the PUT, so the confirmation is of the index as it stood once the record was down.
        node.fences().ensureIncarnationLive(shardId);
    }

    /**
     * Deletes a document durably: the deletion reaches the object store's log before it is acknowledged.
     *
     * <p>Same ordering as {@link #index}, and for a sharper reason. A write lost between acknowledgement
     * and publication is a write the caller was told about and cannot find — bad. A <em>deletion</em>
     * lost the same way is worse than bad: replay rebuilds the shard from the log, and a deletion missing
     * from the log means the document it removed reappears, during a recovery that reports success. The
     * caller is told the delete worked, the failover is told the recovery worked, and the document is
     * there.
     *
     * @param shardId the shard to delete from
     * @param id the document id
     * @return what the engine assigned this deletion, including whether anything was actually removed
     * @throws java.io.IOException if the shard is not held here, or the delete fails
     */
    public WriteOutcome delete(org.opensearch.core.index.shard.ShardId shardId, String id) throws java.io.IOException {
        return delete(shardId, id, org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO, 0L);
    }

    /**
     * Deletes a document, optionally only if it is still at the sequence number the caller expects.
     *
     * <p>The same engine-owned compare-and-swap {@link #index(ShardId, String, String, long, long)}
     * describes, on the deletion path.
     *
     * @param shardId the shard to delete from
     * @param id the document id
     * @param ifSeqNo the sequence number the document must currently be at, or
     *     {@link org.opensearch.index.seqno.SequenceNumbers#UNASSIGNED_SEQ_NO} for an unconditional delete
     * @param ifPrimaryTerm the primary term the document must currently be at, or 0 when unconditional
     * @return what the engine assigned this deletion, including whether anything was actually removed
     * @throws org.opensearch.index.engine.VersionConflictEngineException if the condition was not met
     * @throws java.io.IOException if the shard is not held here, or the delete fails
     */
    public WriteOutcome delete(org.opensearch.core.index.shard.ShardId shardId, String id, long ifSeqNo, long ifPrimaryTerm)
        throws java.io.IOException {
        node.ensureStarted();
        try {
            node.fences.awaitHold(shardId);
            try (org.opensearch.common.lease.Releasable admitted = node.writeBackpressure.admit()) {
                final java.util.concurrent.locks.Lock fence = node.fences.enterWritePath(shardId);
                try (org.opensearch.common.lease.Releasable guard = node.reconciler.writeGuard(shardId)) {
                    return deleteUnderFence(shardId, id, ifSeqNo, ifPrimaryTerm);
                } finally {
                    fence.unlock();
                }
            }
        } catch (WriteHold.Forwarded forwarded) {
            return forwarded.target.delete(shardId, id, ifSeqNo, ifPrimaryTerm).forwardedBy(forwarded.target);
        }
    }

    WriteOutcome deleteUnderFence(org.opensearch.core.index.shard.ShardId shardId, String id, long ifSeqNo, long ifPrimaryTerm)
        throws java.io.IOException {
        final var shard = node.reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot delete from " + shardId + ": not open on " + node.nodeName);
        }
        if (node.reconciler.isReader(shardId)) {
            throw new java.io.IOException("cannot delete from " + shardId + ": it is open as a reader");
        }
        node.fences.refuseIfWriteFenced(shardId);
        // Before the operation as well as after it: the read that confirms the incarnation brings the shard's mapping
        // and settings up to the descriptor, so a change made on another node is in force for the first operation
        // after it rather than the one after that. Within the fence window this is free; see INCARNATION_FENCE_MILLIS.
        node.fences().ensureIncarnationLive(shardId);
        node.shards().markUsed(shardId);
        // Apply, then log -- the same reordering, for the same reason, as the index path above.
        final var result = shard.applyDeleteOperationOnPrimary(
            org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
            id,
            org.opensearch.index.VersionType.INTERNAL,
            ifSeqNo,
            ifPrimaryTerm
        );
        if (result.getResultType() == org.opensearch.index.engine.Engine.Result.Type.FAILURE
            && result.getFailure() instanceof org.opensearch.index.engine.VersionConflictEngineException conflict) {
            throw conflict;
        }
        if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
            // A value, not a throw -- the same trap as for indexing.
            throw new java.io.IOException("deleting " + id + " returned " + result.getResultType());
        }
        appendOrRelease(
            shardId,
            shard,
            java.util.List.of(
                org.opensearch.serverless.store.WalRecord.deletion(id, result.getSeqNo(), result.getTerm(), result.getVersion())
            )
        );
        // The same edge a write raises: a deletion changes the shard as surely as an addition, and a
        // shard whose only recent change was a delete still needs publishing or the tombstone lives
        // nowhere but this node's disk and its log.
        // For the breaker, not for durability -- see index.
        syncIfStillOpen(shard);
        node.signals.wrote(shardId);
        return new WriteOutcome(result.getSeqNo(), result.getTerm(), result.getVersion(), false, result.isFound());
    }

    /**
     * Applies a batch to one shard: one log append for all of it, then each operation in order.
     *
     * <p><b>Why this is not a loop over {@link #index} and {@link #delete}.</b> Those append one record
     * each, so a hundred-document request would be a hundred object-store PUTs — the write path priced
     * per document, which is what made {@code _bulk} worth building rather than worth faking. Here the
     * whole batch is one append, and the durability contract is unchanged: every record is in the log
     * before any of it is acknowledged, which is strictly the same promise a single write makes.
     *
     * <p><b>Ordering within the batch is preserved</b>, and it has to be. A batch that writes a document
     * and then deletes it must leave it gone, and one that deletes and then rewrites must leave it
     * present. Since a record's kind travels with it, replay reproduces the same sequence — see
     * {@link org.opensearch.serverless.store.WalStore#append(long, java.util.List)}.
     *
     * <p><b>A failed operation does not roll back the ones before it</b>, exactly as in classic
     * OpenSearch: a bulk is a batch, not a transaction. The failure is reported for its own item and the
     * rest of the batch stands.
     *
     * @param shardId the shard, which this node must own as a writer
     * @param operations the writes and deletions, in request order
     * @return one outcome per operation, in the same order
     * @throws java.io.IOException if the shard is not open here, is a reader, or the log append fails
     */
    public java.util.List<BulkOutcome> bulk(
        org.opensearch.core.index.shard.ShardId shardId,
        java.util.List<org.opensearch.serverless.store.WalRecord> operations
    ) throws java.io.IOException {
        final java.util.List<BulkOperation> plain = new java.util.ArrayList<>(operations.size());
        for (org.opensearch.serverless.store.WalRecord record : operations) {
            plain.add(BulkOperation.of(record));
        }
        return bulkOperations(shardId, plain);
    }

    /**
     * Applies a batch whose items may carry conditions.
     *
     * @param shardId the shard to write to
     * @param batch the operations, in request order
     * @return one outcome per operation, in the same order
     * @throws java.io.IOException if the shard is not open here, is a reader, or the log append fails
     */
    public java.util.List<BulkOutcome> bulkOperations(org.opensearch.core.index.shard.ShardId shardId, java.util.List<BulkOperation> batch)
        throws java.io.IOException {
        node.ensureStarted();
        try {
            node.fences.awaitHold(shardId);
            try (org.opensearch.common.lease.Releasable admitted = node.writeBackpressure.admit()) {
                final java.util.concurrent.locks.Lock fence = node.fences.enterWritePath(shardId);
                try (org.opensearch.common.lease.Releasable guard = node.reconciler.writeGuard(shardId)) {
                    return bulkOperationsUnderFence(shardId, batch);
                } finally {
                    fence.unlock();
                }
            }
        } catch (WriteHold.Forwarded forwarded) {
            final java.util.List<BulkOutcome> outcomes = forwarded.target.bulk(shardId, batch);
            for (BulkOutcome outcome : outcomes) {
                outcome.forwardedBy(forwarded.target);
            }
            return outcomes;
        }
    }

    /**
     * Makes a write visible to search where it landed, for a caller that was asked to refresh.
     *
     * <p>A write addressed to a shard whose writes a hold forwards, such as a unit being split, lands in the hold's
     * target and not in the addressed shard; refreshing the addressed shard would answer "refreshed" for a document no
     * search can see yet. So a forwarded write refreshes its target, and only a write the shard applied itself refreshes
     * the shard. The shard may be closed by then (a split retires it once its writes are forwarded); that is fine for a
     * forwarded write, as nothing of it is in the shard.
     *
     * @param shardId the shard the write was addressed to
     * @param outcome what the write path answered
     * @param source why, for core's refresh stats
     * @return true if what was written is visible to search
     * @throws java.io.IOException if the target cannot refresh
     */
    public boolean refreshWritten(org.opensearch.core.index.shard.ShardId shardId, WriteOutcome outcome, String source)
        throws java.io.IOException {
        if (outcome.forwardedTo() != null) {
            outcome.forwardedTo().refresh(source);
            return true;
        }
        return refresh(shardId, source);
    }

    /**
     * Makes a batch visible to search wherever its operations landed, as the single-write form does.
     *
     * @param shardId the shard the batch was addressed to
     * @param outcomes what the write path answered, one per operation
     * @param source why, for core's refresh stats
     * @return true if what was written is visible to search
     * @throws java.io.IOException if a target cannot refresh
     */
    public boolean refreshWritten(org.opensearch.core.index.shard.ShardId shardId, java.util.List<BulkOutcome> outcomes, String source)
        throws java.io.IOException {
        final java.util.Set<WriteHold.Target> targets = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        boolean here = outcomes.isEmpty();
        for (BulkOutcome outcome : outcomes) {
            if (outcome.forwardedTo() == null) {
                here = true;
            } else {
                targets.add(outcome.forwardedTo());
            }
        }
        for (WriteHold.Target target : targets) {
            target.refresh(source);
        }
        return (here == false || refresh(shardId, source));
    }

    /**
     * Refreshes a shard open here.
     *
     * @param shardId the shard
     * @param source why, for core's refresh stats
     * @return false if the shard is not open here
     */
    public boolean refresh(org.opensearch.core.index.shard.ShardId shardId, String source) {
        final var shard = node.reconciler.shard(shardId);
        if (shard == null) {
            return false;
        }
        shard.refresh(source);
        return true;
    }

    java.util.List<BulkOutcome> bulkOperationsUnderFence(
        org.opensearch.core.index.shard.ShardId shardId,
        java.util.List<BulkOperation> batch
    ) throws java.io.IOException {
        final var shard = node.reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot write to " + shardId + ": not open on " + node.nodeName);
        }
        if (node.reconciler.isReader(shardId)) {
            throw new java.io.IOException("cannot write to " + shardId + ": it is open as a reader");
        }
        if (batch.isEmpty()) {
            return java.util.List.of();
        }
        node.fences.refuseIfWriteFenced(shardId);
        // Before the operation as well as after it: the read that confirms the incarnation brings the shard's mapping
        // and settings up to the descriptor, so a change made on another node is in force for the first operation
        // after it rather than the one after that. Within the fence window this is free; see INCARNATION_FENCE_MILLIS.
        node.fences().ensureIncarnationLive(shardId);
        node.shards().markUsed(shardId);

        // Applied first, then logged once for the whole batch -- the same reordering the single-document
        // path took, and the batch economics are unchanged: one PUT for the batch, not one per document.
        // Only operations that actually applied are logged, so replay never reproduces an item the caller
        // was told had failed.
        // Mapping growth is coalesced per batch, and request order is kept while it is. The first item
        // that needs a field the mapping lacks stops the pass: what it needs is grown once, and the pass
        // resumes from that item. It used to grow inline, per item, so a batch of a thousand documents
        // naming one new field was a thousand descriptor swaps on one register. Stopping at the first
        // such item rather than skipping it is what keeps the batch's order: an item deferred past a
        // later delete of the same id would resurrect the document the request said to remove.
        final BulkOutcome[] outcomes = new BulkOutcome[batch.size()];
        final java.util.List<org.opensearch.serverless.store.WalRecord> applied = new java.util.ArrayList<>(batch.size());
        final boolean[] grewFor = new boolean[batch.size()];
        int from = 0;
        while (from < batch.size()) {
            int deferredAt = -1;
            org.opensearch.index.mapper.Mapping required = null;
            for (int i = from; i < batch.size(); i++) {
                final BulkOperation item = batch.get(i);
                final org.opensearch.serverless.store.WalRecord operation = item.record();
                try {
                    if (operation.isDeletion()) {
                        final var result = shard.applyDeleteOperationOnPrimary(
                            org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                            operation.id(),
                            org.opensearch.index.VersionType.INTERNAL,
                            item.ifSeqNo(),
                            item.ifPrimaryTerm()
                        );
                        if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
                            // A failure here is a value, and a caller that reads
                            // only the exception channel reports success for an operation that did nothing.
                            outcomes[i] = conflicted(result)
                                ? BulkOutcome.conflicted(operation.id(), describe("deleting", result))
                                : BulkOutcome.failed(operation.id(), describe("deleting", result));
                            continue;
                        }
                        applied.add(
                            org.opensearch.serverless.store.WalRecord.deletion(
                                operation.id(),
                                result.getSeqNo(),
                                result.getTerm(),
                                result.getVersion()
                            )
                        );
                        outcomes[i] = BulkOutcome.deleted(
                            operation.id(),
                            result.isFound(),
                            result.getSeqNo(),
                            result.getTerm(),
                            result.getVersion()
                        );
                    } else {
                        final var result = shard.applyIndexOperationOnPrimary(
                            item.requireAbsent()
                                ? org.opensearch.common.lucene.uid.Versions.MATCH_DELETED
                                : org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                            org.opensearch.index.VersionType.INTERNAL,
                            new org.opensearch.index.mapper.SourceToParse(
                                shardId.getIndexName(),
                                operation.id(),
                                new org.opensearch.core.common.bytes.BytesArray(operation.source()),
                                org.opensearch.common.xcontent.XContentType.JSON
                            ),
                            item.ifSeqNo(),
                            item.ifPrimaryTerm(),
                            org.opensearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                            false
                        );
                        if (result.getResultType() == org.opensearch.index.engine.Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                            if (grewFor[i]) {
                                // The mapping was grown for this item once already, and it still does
                                // not fit: something other than a missing field, and growing again
                                // forever would hide it.
                                outcomes[i] = BulkOutcome.failed(operation.id(), describe("indexing", result));
                                continue;
                            }
                            deferredAt = i;
                            required = result.getRequiredMappingUpdate();
                            break;
                        }
                        if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
                            // A lost condition is a FAILURE carrying a VersionConflictEngineException rather
                            // than a thrown one, so the item has to be inspected rather than only its type
                            // reported -- otherwise a caller that asked a compare-and-swap question is told
                            // only that "indexing returned FAILURE", which is not an answer to it.
                            outcomes[i] = conflicted(result)
                                ? BulkOutcome.conflicted(operation.id(), describe("indexing", result))
                                : BulkOutcome.failed(operation.id(), describe("indexing", result));
                            continue;
                        }
                        applied.add(
                            new org.opensearch.serverless.store.WalRecord(
                                operation.id(),
                                operation.source(),
                                result.getSeqNo(),
                                result.getTerm(),
                                result.getVersion()
                            )
                        );
                        outcomes[i] = BulkOutcome.indexed(
                            operation.id(),
                            result.getSeqNo(),
                            result.getTerm(),
                            result.getVersion(),
                            result.isCreated()
                        );
                    }
                } catch (Exception e) {
                    // One bad document does not fail the batch. A mapping conflict on item 40 is item 40's
                    // problem, and failing the other 99 would make a bulk request less useful than the loop
                    // it replaces.
                    final String message = e.getMessage() == null ? e.toString() : e.getMessage();
                    outcomes[i] = e instanceof org.opensearch.index.engine.VersionConflictEngineException
                        ? BulkOutcome.conflicted(operation.id(), message)
                        : BulkOutcome.failed(operation.id(), message);
                }
            }
            if (deferredAt < 0) {
                break;
            }
            // One growth for everything from here on that needs what this item needs -- which in the
            // common case, a batch of like documents, is all of it. Then the pass resumes at this item.
            grewFor[deferredAt] = true;
            try {
                node.growMapping(shardId, required);
                from = deferredAt;
            } catch (java.io.IOException e) {
                final String id = batch.get(deferredAt).record().id();
                final String message = e.getMessage() == null ? e.toString() : e.getMessage();
                // Contention on the descriptor is not this document's fault, and a retry writes it;
                // anything else about the growth is.
                outcomes[deferredAt] = message.contains("being changed concurrently")
                    ? BulkOutcome.retryable(id, message)
                    : BulkOutcome.failed(id, message);
                from = deferredAt + 1;
            }
        }

        appendOrRelease(shardId, shard, applied);
        // One local sync for the batch, for the reason index gives: it gives the translog's page back to
        // the request breaker, and costs no object-store request.
        syncIfStillOpen(shard);
        // One edge for the batch, not one per document. The
        // edge is raised after everything is applied, so a publish it triggers describes the whole batch
        // or none of it.
        node.signals.wrote(shardId);
        return java.util.Arrays.asList(outcomes);
    }

    /**
     * Describes a non-success engine result, naming the cause when there is one.
     *
     * @param result the engine's answer
     * @return whether the failure was a lost condition
     */
    static boolean conflicted(org.opensearch.index.engine.Engine.Result result) {
        return result.getFailure() instanceof org.opensearch.index.engine.VersionConflictEngineException;
    }

    /**
     * Describes a non-success engine result, naming the cause when there is one.
     *
     * @param what the operation kind, for the message
     * @param result the engine's answer
     * @return a message for the failed item
     */
    static String describe(String what, org.opensearch.index.engine.Engine.Result result) {
        if (result.getFailure() != null) {
            final String message = result.getFailure().getMessage();
            return message == null ? result.getFailure().toString() : message;
        }
        return what + " returned " + result.getResultType();
    }
}
