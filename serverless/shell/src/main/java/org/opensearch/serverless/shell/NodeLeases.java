/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.shard.FailedShardRelease;

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
     * Heads this node lost to its own lapsed lease and has not yet given back in the register, with the
     * term it lost them at. Drained on every renewal, so a store that was unreachable when the loss was
     * discovered does not leave a head naming a node that is not serving the shard.
     */
    final java.util.Map<org.opensearch.core.index.shard.ShardId, Long> headsToGiveBack = new java.util.concurrent.ConcurrentHashMap<>();
    final Object leaseRenewalLock = new Object();
    /** Serialises head verification: two passes interleaving their releases was the hazard, and waiting is cheaper than skipping. */
    final Object headVerificationLock = new Object();
    /** Whether a head verification is running, so an activation can tell it would only wait behind one. */
    final java.util.concurrent.atomic.AtomicBoolean verifyingHeads = new java.util.concurrent.atomic.AtomicBoolean();
    /**
     * True from the moment this node's lease is found lapsed until a head verification that began after
     * the renewal has read every head. While set, no write is acknowledged and no forwarded write is
     * accepted. See {@link NodeLeases#renewLease}.
     */
    volatile boolean ownershipUnverified;
    /** Bumped on every lapse, so a verification that began before the renewal cannot lift the suspension. */
    final java.util.concurrent.atomic.AtomicLong lapseEpoch = new java.util.concurrent.atomic.AtomicLong();
    /** How many shard-head reads a verification pass has in flight at once. */
    public static final int HEAD_READ_LANES = 8;

    /**
     * How long a node may go without re-reading the head of every writer shard it holds.
     *
     * <p>One lease TTL, where this used to run on every renewal <em>and</em> every backstop -- four scans
     * per TTL at the defaults, each of them one register read per held shard. At the thousand-shard cap
     * that was 480 reads per shard per hour paid to be told nothing had changed, and it is the largest
     * standing cost a node has when it is doing nothing at all.
     *
     * <p><b>Why cutting it is safe, and why it is not cut further.</b> The scan is not what fences a
     * writer. A node refuses to acknowledge past its own lease deadline less the skew margin
     * ({@code ensureOwnLeaseIsStillValid}), which needs no I/O and so happens on every single write; and a
     * successor seals the log at takeover, so anything appended behind that seal is never replayed. A head
     * cannot be taken from a node whose lease is live, because {@code ShardHeadStore#acquire} refuses an
     * owner the liveness oracle says is alive. So in steady state this scan is looking for something that
     * cannot have happened.
     *
     * <p>What it does still catch is worth a bounded wait rather than nothing: a shard of an index deleted
     * and recreated under a new uuid, a head given back and won elsewhere, and the clock-skew edge where
     * two nodes disagree about whether a lease had expired. A lapse takes the unconditional path and is
     * unaffected -- {@code renewLease} scans every head before acknowledgement resumes, which is where the
     * correctness argument actually lives.
     *
     * <p><b>Zero by default, which means the scan still runs on every pass.</b> The saving is real and
     * measured -- between scans a tick reads one register whatever the node holds, against one per shard --
     * but switching it on trades something a deployment may not want to trade, and three existing tests
     * encode the current bargain as a guarantee: a tick reacquires a shard whose lease was lost, a
     * forwarded write finds the owner has moved, a node stops holding what it no longer owns. All of them
     * describe how quickly a node <em>notices</em>, and all of them get slower by up to this interval.
     *
     * <p>What does not get worse is worth saying precisely, because it is what makes the option safe to
     * offer at all. A node cannot acknowledge a write past its own lease deadline less the skew margin
     * ({@code ensureOwnLeaseIsStillValid}), which needs no I/O and so happens on every write; a successor
     * seals the log at takeover, so anything appended behind that seal never replays; and a head cannot be
     * taken from a node whose lease is live, because {@code ShardHeadStore#acquire} refuses an owner the
     * liveness oracle says is alive. A lapse takes the unconditional path regardless. So the exposure a
     * longer interval buys is a node serving reads from a shard it has lost, for up to the interval --
     * bounded, and never an acknowledged write that nobody replays.
     *
     * <p><b>What made a long interval safe to deploy is a signal carried on the node's own lease.</b> A
     * revocation marker written somewhere else fails because it is not atomic with the swap that moves
     * ownership, so its absence proves nothing -- {@code RegisterMap#assignments} says the same of the
     * listing it maintains. A revocation written <em>onto the owner's lease register</em>, before the head
     * is swapped, does not: the taker's swap of the lease and the owner's renewal of it contend on one
     * register, so either the owner renewed first and the taker backs off, or the revocation landed first
     * and the owner's next renewal fails, which it treats as a lapse -- suspend, re-read every head (see
     * {@code BlobLeaseMembership#revoke} and {@link NodeLeases#renewLease}). A node whose renewals keep succeeding has
     * therefore lost no head to a takeover, at one write per renewal however many shards it holds, and
     * {@code ServerlessBootstrap#HEAD_VERIFY_INTERVAL} runs this scan every five minutes as a backstop for a
     * head released by someone other than its owner. The library default below stays per pass, because the
     * tests that encode the old bargain construct nodes directly.
     */
    public static final long DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS = 0L;

    volatile long headVerifyIntervalMillis = DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS;
    volatile long headsVerifiedAtMillis = Long.MIN_VALUE;

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
     * <p>So a lapse <em>suspends acknowledgement</em> ({@link #ownershipUnverified}) before the renewal,
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
            lapseEpoch.incrementAndGet();
            ownershipUnverified = true;
            ServerlessNode.logger.info(
                "this node's lease lapsed at {} by its own clock (now {}); acknowledgement is suspended until every held head is re-read",
                plane.membership().ownExpiresAtMillis() - plane.membership().skewMarginMillis(),
                now
            );
        }
        for (java.util.Map.Entry<org.opensearch.core.index.shard.ShardId, Long> lost : new java.util.ArrayList<>(
            headsToGiveBack.entrySet()
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
                headsToGiveBack.remove(lost.getKey(), lost.getValue());
            } catch (java.io.IOException e) {
                ServerlessNode.logger.warn("could not give back the head of " + lost.getKey() + "; will retry on the next renewal", e);
            }
        }
        boolean revoked = false;
        synchronized (leaseRenewalLock) {
            try {
                node.renewOwnLease(plane);
            } catch (org.opensearch.serverless.membership.LeaseRevokedException e) {
                // Someone changed this node's lease: a node taking one of its shards revoked it, which a
                // taker does only once it judges the lease expired -- so a clock beyond the skew budget
                // has had this node serving shards it may have lost. The same suspension as a lapse, and
                // the same full scan before it lifts. The renewal after this one starts the lease afresh.
                lapseEpoch.incrementAndGet();
                ownershipUnverified = true;
                revoked = true;
                ServerlessNode.logger.warn("{}; acknowledgement is suspended until every held head is re-read", e.getMessage());
                node.renewOwnLease(plane);
            }
        }
        if ((lapsed || revoked) && (verifyAfterLapse || verifyingHeads.get() == false)) {
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
     * <p><b>Bounded and parallel.</b> One register read per held writer shard, in {@link #HEAD_READ_LANES}
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
        synchronized (headVerificationLock) {
            verifyingHeads.set(true);
            try {
                return verifyHeadsExclusively(plane, true);
            } finally {
                verifyingHeads.set(false);
            }
        }
    }

    /**
     * Whether the periodic head check last read every head within the given time, by the plane's clock.
     *
     * @param plane the metadata plane
     * @param millis how long ago it may have been
     * @return true if it did, and no lapse since has suspended ownership
     */
    boolean headsVerifiedWithin(org.opensearch.serverless.metadata.MetadataPlane plane, long millis) {
        final long last = headsVerifiedAtMillis;
        return ownershipUnverified == false && last != Long.MIN_VALUE && plane.clock().getAsLong() - last <= millis;
    }

    /**
     * Verifies every held head, but only if {@link #DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS} has passed since
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
        final long last = headsVerifiedAtMillis;
        final long now = plane.clock().getAsLong();
        // Due on every pass while acknowledgement is suspended: only a full read of the heads lifts the suspension,
        // and the read right after a lapse fails under exactly the conditions that cause one. Waiting for the interval
        // left a node refusing every write for minutes after a minute of SlowDown had ended.
        final boolean due = ownershipUnverified
            || headVerifyIntervalMillis <= 0L
            || last == Long.MIN_VALUE
            || now - last >= headVerifyIntervalMillis;
        if (due) {
            // By the plane's clock, which every other interval of the loop is kept on.
            headsVerifiedAtMillis = now;
        }
        // Not due still runs the pass, minus the per-shard head reads: the index half of it -- releasing a
        // shard whose index was deleted or recreated, and reopening one whose log could not be appended to
        // -- is not what costs, and deferring it would leave a deleted index served until the next scan.
        synchronized (headVerificationLock) {
            verifyingHeads.set(true);
            try {
                return verifyHeadsExclusively(plane, due);
            } finally {
                verifyingHeads.set(false);
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
        final long epoch = lapseEpoch.get();

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
            node.sightings().noteHead(shardId.getIndexName(), shardId.id(), head.orElse(null));
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
        if (readHeads && ownershipUnverified && lapseEpoch.get() == epoch) {
            // Every head this node holds was read after the renewal that followed the lapse, and each
            // one either still names this node or has been let go. Nothing can be taken from a node with
            // a live lease, so acknowledgement is safe again.
            ownershipUnverified = false;
            ServerlessNode.logger.info("every held head re-read after the lapse; acknowledgement resumes");
        }
        return released;
    }

    /**
     * Reads the given heads, in lanes on the fan-out pool once there are more than fit in one.
     *
     * <p>Each lane's reads are issued together and waited for together, so the store sees at most
     * {@link #HEAD_READ_LANES} reads from this pass at once. A pool that refuses the work runs the read
     * inline instead, which is slower and correct.
     */
    void readHeads(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        java.util.List<org.opensearch.core.index.shard.ShardId> shards,
        java.util.Map<org.opensearch.core.index.shard.ShardId, java.util.Optional<org.opensearch.serverless.metadata.ShardHead>> heads,
        java.util.Map<org.opensearch.core.index.shard.ShardId, java.io.IOException> failures
    ) throws InterruptedException {
        if (shards.size() <= NodeLeases.HEAD_READ_LANES) {
            for (org.opensearch.core.index.shard.ShardId shardId : shards) {
                try {
                    heads.put(shardId, plane.heads().read(shardId.getIndexName(), shardId.id()));
                } catch (java.io.IOException e) {
                    failures.put(shardId, e);
                }
            }
            return;
        }
        final java.util.concurrent.Executor executor = node.threadPool.executor(NodeThreadPools.FANOUT_POOL);
        for (int start = 0; start < shards.size(); start += NodeLeases.HEAD_READ_LANES) {
            final java.util.List<org.opensearch.core.index.shard.ShardId> lane = shards.subList(
                start,
                Math.min(shards.size(), start + NodeLeases.HEAD_READ_LANES)
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
        node.shards().forgetShardRecords(shardId);
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
                if (result != FailedShardRelease.NOT_CURRENT) {
                    node.writeFenced.remove(shardId);
                    node.shards().forgetShardRecords(shardId);
                }
                return result;
            });
            if (released == FailedShardRelease.WRITER && node.metadataPlane != null) {
                node.signals.ownershipDoubted(shardId.getIndexName(), shardId.id());
            }
        } catch (Exception e) {
            ServerlessNode.logger.warn("could not close failed shard " + shardId, e);
        }
    }
}
