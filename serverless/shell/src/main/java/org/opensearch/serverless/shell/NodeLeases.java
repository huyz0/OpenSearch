/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.shard.ShardReconciler;

import java.util.concurrent.CountDownLatch;

/**
 * Keeps {@link ServerlessNode}'s claims current: renews its lease, verifies the heads it holds, and lets go of
 * shards it has lost.
 */
public final class NodeLeases {

    private final ServerlessNode node;

    NodeLeases(ServerlessNode node) {
        this.node = node;
    }

    /**
     * Renews this node's lease and lets go of anything it has lost: {@link #renewLease} then
     * {@link #verifyHeads}, as one pass.
     *
     * <p>This is the whole of failure detection, and it runs on the node that might be failing rather
     * than on one watching it. A node whose process is paused, partitioned or dead simply stops calling
     * this, its leases lapse, and another node acquires by compare-and-swap. Nothing needs to agree that
     * it died.
     *
     * <p>A lease found lapsed suspends acknowledgement until the verification has read every head; see
     * {@link #renewLease}. The two halves are separable, and the scheduler separates them: the lease is renewed on its own
     * timer, and the head scan -- one read per held writer shard -- runs after the next renewal is
     * already on the clock, so a node holding many shards on a slow store cannot lapse behind its own
     * scan. This method is the composition, for callers that drive a node by hand.
     *
     * @param plane the metadata plane
     * @return the shards released because this node no longer owns them
     * @throws Exception if the metadata plane cannot be read or written
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> heartbeat(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        node.ensureStarted();
        final java.util.Set<org.opensearch.core.index.shard.ShardId> released = new java.util.LinkedHashSet<>(renewLease(plane));
        // IfDue: the periodic half. renewLease above has already scanned every head unconditionally if the
        // lease had lapsed, which is the case where the scan is load-bearing; this is the routine sweep,
        // and running it on every heartbeat was one register read per held shard per beat. See
        // DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS.
        released.addAll(verifyHeadsIfDue(plane));
        return released;
    }

    /**
     * Renews this node's lease -- and, if the lease had already lapsed, re-verifies every held head
     * before a single write can be acknowledged again.
     *
     * <p><b>The rule this makes explicit: a node whose own lease lapsed can prove nothing about the
     * shards it holds until it has renewed and re-read their heads.</b> The moment its lease is expired
     * by its own clock (less the skew margin, {@code BlobLeaseMembership#selfLeaseLapsedAt}), any other
     * node is entitled to take any of its shards and seal their logs, and this node cannot tell from
     * here which ones were taken. Renewing first and finding out afterwards -- which is what this did --
     * reopened the write path on every shard for the length of the head scan: a coordinator with a
     * stale hint forwarded a write, the lease was valid again, the record landed behind a successor's
     * seal, and the client was told 201. Nobody replays a record behind a seal.
     *
     * <p>So a lapse <em>suspends acknowledgement</em> ({@link ServerlessNode#ownershipUnverified}) before the renewal,
     * and the suspension is lifted only by a head verification that started after the renewal and read
     * every head. Between the two, every write is refused before it is applied. A shard whose head still
     * names this node is kept -- nobody took it, and closing it would move it for no reason; one whose
     * head names another node is released. Once the lease is live again nothing can be taken, so what the
     * verification found is what this node holds.
     *
     * <p>Heads this node lost on the write path ({@link WriteFences#ensureOwnLeaseIsStillValid}) are given back in
     * the register here, keyed on the term they were lost at, so a closed shard does not sit naming a node
     * that is not serving it once the lease is live again. A give-back the store refused is retried on the
     * next renewal.
     *
     * <p>Always, not only under lease batching. Under batching this is what keeps the node's shards
     * alive; in either mode it is what keeps the node addressable, since peers resolve a forwarding
     * target from its lease. One write per renewal per node.
     *
     * @param plane the metadata plane
     * @return the writer shards released because the verification after a lapse found them taken
     * @throws Exception if the lease cannot be renewed
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> renewLease(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        return renewLease(plane, true);
    }

    /**
     * Renews the lease and, after a lapse, re-reads the held heads -- unless asked not to wait and another thread is already
     * re-reading them, as an activation is.
     */
    java.util.Set<org.opensearch.core.index.shard.ShardId> renewLease(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        boolean verifyAfterLapse
    ) throws Exception {
        node.ensureStarted();
        node.adopt(plane);
        final java.util.Set<org.opensearch.core.index.shard.ShardId> released = new java.util.LinkedHashSet<>();
        final long now = plane.clock().getAsLong();
        final boolean lapsed = plane.membership().selfLeaseLapsedAt(now);
        if (lapsed) {
            // Before the renewal, never after: from here until a verification that started after the
            // renewal completes, nothing is acknowledged. The epoch is what tells a verification that
            // began before this moment from one that began after it.
            node.lapseEpoch.incrementAndGet();
            node.ownershipUnverified = true;
            ServerlessNode.logger.info(
                "this node's lease lapsed at {} by its own clock (now {}); acknowledgement is suspended until every held head is re-read",
                plane.membership().ownExpiresAtMillis() - plane.membership().skewMarginMillis(),
                now
            );
        }
        for (java.util.Map.Entry<org.opensearch.core.index.shard.ShardId, Long> lost : new java.util.ArrayList<>(
            node.headsToGiveBack.entrySet()
        )) {
            try {
                plane.heads()
                    .release(
                        lost.getKey().getIndexName(),
                        lost.getKey().id(),
                        node.localNode.getId(),
                        lost.getValue(),
                        lost.getKey().getIndex().getUUID(),
                        node.localNode.getEphemeralId()
                    );
                node.headsToGiveBack.remove(lost.getKey(), lost.getValue());
            } catch (java.io.IOException e) {
                ServerlessNode.logger.warn("could not give back the head of " + lost.getKey() + "; will retry on the next renewal", e);
            }
        }
        boolean revoked = false;
        synchronized (node.leaseRenewalLock) {
            try {
                node.renewOwnLease(plane);
            } catch (org.opensearch.serverless.membership.LeaseRevokedException e) {
                // Someone changed this node's lease: a node taking one of its shards revoked it, which a
                // taker does only once it judges the lease expired -- so a clock beyond the skew budget
                // has had this node serving shards it may have lost. The same suspension as a lapse, and
                // the same full scan before it lifts. The renewal after this one starts the lease afresh.
                node.lapseEpoch.incrementAndGet();
                node.ownershipUnverified = true;
                revoked = true;
                ServerlessNode.logger.warn("{}; acknowledgement is suspended until every held head is re-read", e.getMessage());
                node.renewOwnLease(plane);
            }
        }
        if ((lapsed || revoked) && (verifyAfterLapse || node.verifyingHeads.get() == false)) {
            // Now, not on the next timer: the suspension lasts exactly as long as this scan.
            released.addAll(verifyHeads(plane));
        }
        return released;
    }

    /**
     * Whether the shard's head names this very process as owner at exactly this term: what a writer asks
     * after fencing its own term, so one superseded before it had written anything stops rather than writes.
     * Unreadable answers no, which stops the writer and costs a retry, never a second writer.
     */
    boolean ownsAtTerm(org.opensearch.serverless.metadata.MetadataPlane plane, org.opensearch.core.index.shard.ShardId id, long term) {
        try {
            final var head = plane.heads().read(id.getIndexName(), id.id());
            return head.isPresent()
                && head.get().term() == term
                && (head.get().indexUuid() == null || id.getIndex().getUUID().equals(head.get().indexUuid()))
                && node.localNode.getId().equals(head.get().ownerNodeId())
                && (head.get().ownerEphemeralId() == null || node.localNode.getEphemeralId().equals(head.get().ownerEphemeralId()));
        } catch (java.io.IOException e) {
            return false;
        }
    }

    /**
     * Reads the head of every writer shard this node holds and lets go of the ones it has lost.
     *
     * <p>The release half of failure detection. A node that has lost a shard must stop serving it, and
     * this is where it finds out — a head that no longer names this node means someone else won it.
     * Also where a shard of a deleted index is let go, where the head was never deleted: the descriptor
     * is read once per open index anyway, and a shard whose index is tombstoned or recreated under a
     * different uuid is an orphan whatever its head says.
     *
     * <p><b>Bounded and parallel.</b> One register read per held writer shard, in {@link ServerlessNode#HEAD_READ_LANES}
     * lanes on the fan-out pool once there are more shards than lanes, so a node holding a thousand pays
     * a thousand reads in a hundred-odd round trips rather than a thousand. Single-flight: a pass that
     * finds one already running returns nothing, rather than interleaving two sets of releases.
     *
     * @param plane the metadata plane
     * @return the shards released because this node no longer owns them
     * @throws Exception if a head could not be read; the shards whose heads were read are still judged
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> verifyHeads(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        node.ensureStarted();
        node.adopt(plane);
        synchronized (node.headVerificationLock) {
            node.verifyingHeads.set(true);
            try {
                return verifyHeadsExclusively(plane, true);
            } finally {
                node.verifyingHeads.set(false);
            }
        }
    }

    /**
     * Verifies every held head, but only if {@link ServerlessNode#DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS} has passed since
     * the last time.
     *
     * <p>What the periodic callers use. A lapse, a doubt raised against a specific shard, and an operator
     * asking all still go through {@link #verifyHeads}, which never skips.
     *
     * @param plane the metadata plane
     * @return the shards released, empty when this pass did not scan
     * @throws Exception if the metadata plane cannot be reached
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> verifyHeadsIfDue(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        node.ensureStarted();
        node.adopt(plane);
        final long last = node.headsVerifiedAtNanos;
        // Due on every pass while acknowledgement is suspended: only a full read of the heads lifts the suspension,
        // and the read right after a lapse fails under exactly the conditions that cause one. Waiting for the interval
        // left a node refusing every write for minutes after a minute of SlowDown had ended.
        final boolean due = node.ownershipUnverified
            || node.headVerifyIntervalMillis <= 0L
            || last == Long.MIN_VALUE
            || System.nanoTime() - last >= java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(node.headVerifyIntervalMillis);
        if (due) {
            node.headsVerifiedAtNanos = System.nanoTime();
        }
        // Not due still runs the pass, minus the per-shard head reads: the index half of it -- releasing a
        // shard whose index was deleted or recreated, and reopening one whose log could not be appended to
        // -- is not what costs, and deferring it would leave a deleted index served until the next scan.
        synchronized (node.headVerificationLock) {
            node.verifyingHeads.set(true);
            try {
                return verifyHeadsExclusively(plane, due);
            } finally {
                node.verifyingHeads.set(false);
            }
        }
    }

    java.util.Set<org.opensearch.core.index.shard.ShardId> verifyHeadsExclusively(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        boolean readHeads
    ) throws Exception {
        final java.util.Set<org.opensearch.core.index.shard.ShardId> released = new java.util.LinkedHashSet<>();
        // Taken before any read: a lapse that happens during this pass bumps it, and this pass's reads
        // then predate the renewal and may not lift the suspension.
        final long epoch = node.lapseEpoch.get();

        // First, shards whose log could not be appended to. Reaching this point means the store answered
        // a lease renewal, so it is time to reopen them from their logs: close, and ask for activation,
        // which re-acquires at a bumped term, seals, and replays -- without the operation the failed
        // append left in the engine, which is the state the caller was told about.
        for (org.opensearch.core.index.shard.ShardId shardId : new java.util.ArrayList<>(node.writeFenced)) {
            if (node.reconciler.shard(shardId) == null) {
                node.writeFenced.remove(shardId);
                continue;
            }
            releaseLost(plane, shardId, "reopening from its log after a failed append", false);
            released.add(shardId);
            node.signals.ownershipDoubted(shardId.getIndexName(), shardId.id());
        }

        final java.util.Map<String, java.util.Optional<IndexDescriptor>> descriptors = node.descriptorRefresh
            .refreshDescriptorsOfOpenIndices(plane);

        final java.util.List<org.opensearch.core.index.shard.ShardId> writers = new java.util.ArrayList<>();
        for (org.opensearch.core.index.shard.ShardId shardId : node.reconciler.openShards()) {
            if (node.reconciler.isReader(shardId) == false) {
                writers.add(shardId);
            }
        }
        final java.util.Map<
            org.opensearch.core.index.shard.ShardId,
            java.util.Optional<org.opensearch.serverless.metadata.ShardHead>> heads = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.Map<org.opensearch.core.index.shard.ShardId, java.io.IOException> failures =
            new java.util.concurrent.ConcurrentHashMap<>();
        if (readHeads) {
            readHeads(plane, writers, heads, failures);
        }

        for (org.opensearch.core.index.shard.ShardId shardId : writers) {
            // The index half first, and it does not need a head. A shard whose index is gone is an orphan
            // whatever any head says, so this runs on every pass even when the head scan does not -- which
            // is what keeps a deleted index's shards from being served until the next scan comes round.
            // It also now runs for a shard whose head read failed, where it used to be skipped with it: a
            // store that could not answer for one register is no reason to go on serving a deleted index.
            final java.util.Optional<IndexDescriptor> descriptor = descriptors.get(shardId.getIndexName());
            if (descriptor != null && (descriptor.isEmpty() || descriptor.get().uuid().equals(shardId.getIndex().getUUID()) == false)) {
                // The index is gone, or has been recreated under another uuid, and this shard's head
                // was not deleted with it -- a delete that crashed between the tombstone and its head
                // deletes. The head may well still name this node; it is an orphan regardless.
                releaseLost(plane, shardId, "its index was deleted or recreated; this shard belongs to a previous incarnation", true);
                released.add(shardId);
                continue;
            }
            if (readHeads == false) {
                // The ownership half is what this pass skipped; see DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS.
                continue;
            }
            final java.util.Optional<org.opensearch.serverless.metadata.ShardHead> head = heads.get(shardId);
            if (head == null) {
                // The read failed. Not evidence of anything; judged on the next pass.
                continue;
            }
            // The head still needs reading, because losing a shard is something only the head can tell
            // us -- but it is a read, not a write, and that is the whole saving. This used to have an
            // else-branch that renewed each head individually; that mode is gone.
            node.noteHead(shardId.getIndexName(), shardId.id(), head.orElse(null));
            if (namesThisNode(head, shardId)) {
                continue;
            }
            releaseLost(plane, shardId, "lease lost: the shard-head no longer names " + node.nodeName, true);
            released.add(shardId);
        }
        if (failures.isEmpty() == false) {
            throw failures.values().iterator().next();
        }
        // Only a pass that actually read every head may lift the suspension: that is the whole content of
        // the claim it is making. A pass that skipped the scan has established nothing about ownership.
        if (readHeads && node.ownershipUnverified && node.lapseEpoch.get() == epoch) {
            // Every head this node holds was read after the renewal that followed the lapse, and each
            // one either still names this node or has been let go. Nothing can be taken from a node with
            // a live lease, so acknowledgement is safe again.
            node.ownershipUnverified = false;
            ServerlessNode.logger.info("every held head re-read after the lapse; acknowledgement resumes");
        }
        return released;
    }

    /**
     * Reads the given heads, in lanes on the fan-out pool once there are more than fit in one.
     *
     * <p>Each lane's reads are issued together and waited for together, so the store sees at most
     * {@link ServerlessNode#HEAD_READ_LANES} reads from this pass at once. A pool that refuses the work runs the read
     * inline instead, which is slower and correct.
     */
    void readHeads(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        java.util.List<org.opensearch.core.index.shard.ShardId> shards,
        java.util.Map<org.opensearch.core.index.shard.ShardId, java.util.Optional<org.opensearch.serverless.metadata.ShardHead>> heads,
        java.util.Map<org.opensearch.core.index.shard.ShardId, java.io.IOException> failures
    ) throws InterruptedException {
        if (shards.size() <= ServerlessNode.HEAD_READ_LANES) {
            for (org.opensearch.core.index.shard.ShardId shardId : shards) {
                try {
                    heads.put(shardId, plane.heads().read(shardId.getIndexName(), shardId.id()));
                } catch (java.io.IOException e) {
                    failures.put(shardId, e);
                }
            }
            return;
        }
        final java.util.concurrent.Executor executor = node.threadPool.executor(ServerlessNode.FANOUT_POOL);
        for (int start = 0; start < shards.size(); start += ServerlessNode.HEAD_READ_LANES) {
            final java.util.List<org.opensearch.core.index.shard.ShardId> lane = shards.subList(
                start,
                Math.min(shards.size(), start + ServerlessNode.HEAD_READ_LANES)
            );
            final CountDownLatch done = new CountDownLatch(lane.size());
            for (org.opensearch.core.index.shard.ShardId shardId : lane) {
                final Runnable read = () -> {
                    try {
                        heads.put(shardId, plane.heads().read(shardId.getIndexName(), shardId.id()));
                    } catch (java.io.IOException e) {
                        failures.put(shardId, e);
                    } catch (RuntimeException e) {
                        failures.put(shardId, new java.io.IOException("could not read the head of " + shardId, e));
                    } finally {
                        done.countDown();
                    }
                };
                try {
                    executor.execute(read);
                } catch (RuntimeException rejected) {
                    read.run();
                }
            }
            done.await();
        }
    }

    /** Whether a head names this node, this incarnation of it, and this incarnation of the index. */
    boolean namesThisNode(
        java.util.Optional<org.opensearch.serverless.metadata.ShardHead> head,
        org.opensearch.core.index.shard.ShardId shardId
    ) {
        return head.isPresent() && node.localNode.getId().equals(head.get().ownerNodeId())
        // This incarnation's, not a previous one's under the same node id.
            && (head.get().ownerEphemeralId() == null || node.localNode.getEphemeralId().equals(head.get().ownerEphemeralId()))
            // And for this index, not a deleted one of the same name.
            && (head.get().indexUuid() == null || head.get().indexUuid().equals(shardId.getIndex().getUUID()));
    }

    /**
     * Closes a writer shard this node no longer holds, under its fence, and tidies what referred to it.
     *
     * @param plane the metadata plane
     * @param shardId the shard
     * @param reason why, for the log
     * @param forgetClaim whether to drop this node's claim blob too; not for a shard that is about to be
     *        re-acquired by this node
     */
    void releaseLost(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        org.opensearch.core.index.shard.ShardId shardId,
        String reason,
        boolean forgetClaim
    ) throws Exception {
        node.fences().underShardFence(shardId, () -> {
            node.reconciler.releaseShard(shardId, reason);
            return null;
        });
        node.writeFenced.remove(shardId);
        node.forgetShardRecords(shardId);
        if (forgetClaim) {
            // Drop the claim too. Leaving it is safe -- it is verified on read -- but a node that has
            // churned through shards for a year should not list a year of them to find today's.
            try {
                plane.forgetAssignment(node.localNode.getId(), shardId.getIndexName(), shardId.id());
            } catch (java.io.IOException e) {
                ServerlessNode.logger.warn(
                    "could not forget the claim on " + shardId + "; it costs one head read per resync until it is",
                    e
                );
            }
        }
    }

    /**
     * Closes a shard whose engine has failed, and has it taken again from the object store.
     *
     * <p><b>Why a failed writer must not stay.</b> The shard still counted as open, so this node kept the head with a
     * live lease: every peer forwarded to it, every activation found it already held, and every request was told
     * "engine is closed". A fleet run stranded acknowledged writes behind one -- durable in the log, and readable
     * by nobody, because nobody would take the shard. The engine's state is gone, but nothing acknowledged was ever
     * only there: the published commit and the log behind it hold every acknowledged write.
     *
     * <p>So the writer is closed under its fence and the head left as it is, still naming this node -- given back
     * unpublished it would be read from a commit behind its log -- and its ownership doubted. The doubt's activation
     * takes the shard at a higher term, which fences whatever the failed engine might still append, opens it from
     * the published commit and replays the log. A failed reader holds no head and is simply closed.
     */
    void onShardFailed(org.opensearch.index.shard.IndexShard failed, Exception cause) {
        final var shardId = failed.shardId();
        ServerlessNode.logger.warn("shard " + shardId + " failed; closing it and taking it again from its published commit and log", cause);
        try {
            final var released = node.fences().underShardFence(shardId, () -> {
                final var result = node.reconciler.releaseFailedShard(failed, "its engine failed");
                if (result != ShardReconciler.FailedShardRelease.NOT_CURRENT) {
                    node.writeFenced.remove(shardId);
                    node.forgetShardRecords(shardId);
                }
                return result;
            });
            if (released == ShardReconciler.FailedShardRelease.WRITER && node.metadataPlane != null) {
                node.signals.ownershipDoubted(shardId.getIndexName(), shardId.id());
            }
        } catch (Exception e) {
            ServerlessNode.logger.warn("could not close failed shard " + shardId, e);
        }
    }
}
