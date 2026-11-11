/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.DigestRollups;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Takes over and settles {@link BackgroundReconciler}'s abandoned shards: shards whose owner died, taken here
 * or given back once their log is published, and the dead nodes' write logs collected.
 */
final class AbandonedShards {

    private final BackgroundReconciler reconciler;

    AbandonedShards(BackgroundReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Takes over the shards a node held when its lease ran out, rather than waiting for a request to find each one.
     *
     * <p>Without this a dead node's shard was taken only when a write or a get for it arrived, failed to reach the
     * dead owner, and raised a doubt -- so takeover time was the lease plus however long the next request for the
     * shard took to come, and a dormant shard with acknowledged writes in its log waited for its first reader.
     *
     * <p><b>Split, not raced.</b> Every survivor hears the departure. Each takes at once the shards that hash to it
     * among the live nodes, and returns the rest for a second look after {@code later} -- by when the survivor they
     * hash to has taken them, unless it is full or slow, in which case anyone may. The head's compare-and-swap
     * decides any race; the split only keeps every survivor from activating every shard at the same moment.
     *
     * <p>Only shards whose head still names the dead node. A node that shut down cleanly released its heads, and
     * those shards stay dormant until something asks for them.
     *
     * @param deadNodeId the node whose lease ran out
     * @return the claims left for the second look
     */
    public List<Map.Entry<String, Integer>> takeOverFrom(String deadNodeId) {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return List.of();
            }
            if (reconciler.demandDriven == false || deadNodeId.equals(reconciler.node.localNode().getId())) {
                return List.of();
            }
            // Gone from this node's view is not gone: a view loses a member whose lease read failed, or that it read
            // late. A fleet run had a node that had just started take shards from all five of its live peers.
            try {
                final java.util.OptionalLong liveUntil = leaseLiveUntil(deadNodeId);
                if (liveUntil.isPresent()) {
                    BackgroundReconciler.logger.info(
                        "node {} left this node's view, but its lease runs {} ms more: its shards stay with it",
                        deadNodeId,
                        liveUntil.getAsLong() - reconciler.plane.clock().getAsLong()
                    );
                    return List.of();
                }
            } catch (Exception e) {
                BackgroundReconciler.logger.warn(
                    "could not read the lease of departed node " + deadNodeId + "; its shards wait for requests",
                    e
                );
                return List.of();
            }
            final List<Map.Entry<String, Integer>> claims;
            try {
                claims = reconciler.plane.claimsOf(deadNodeId);
            } catch (Exception e) {
                BackgroundReconciler.logger.warn(
                    "could not list the claims of departed node " + deadNodeId + "; its shards wait for requests",
                    e
                );
                return List.of();
            }
            final List<String> live = new java.util.ArrayList<>();
            for (var lease : reconciler.plane.membership().current()) {
                if (lease.nodeId().equals(deadNodeId) == false && lease.isExpiredAt(reconciler.plane.clock().getAsLong()) == false) {
                    live.add(lease.nodeId());
                }
            }
            java.util.Collections.sort(live);
            final int rank = live.indexOf(reconciler.node.localNode().getId());
            final List<Map.Entry<String, Integer>> later = new java.util.ArrayList<>();
            final long settledBefore = reconciler.settledClean.get();
            int taking = 0;
            for (Map.Entry<String, Integer> claim : claims) {
                // The same choice a request for the shard is routed by, so the shard is queued on one survivor only.
                final Optional<String> winner = reconciler.node.nodeCapacity()
                    .takeoverWinner(reconciler.plane, claim.getKey(), claim.getValue(), deadNodeId);
                if (rank < 0 || winner.isEmpty() || winner.get().equals(reconciler.node.localNode().getId())) {
                    if (takeIfStillHeldBy(deadNodeId, claim)) {
                        taking++;
                    }
                } else {
                    later.add(claim);
                }
            }
            BackgroundReconciler.logger.info(
                "node {} left: taking {} of its {} claimed shards and giving back {} fully published, {} more if still unclaimed later",
                deadNodeId,
                taking,
                claims.size(),
                reconciler.settledClean.get() - settledBefore,
                later.size()
            );
            return later;
        }
    }

    /**
     * When a node's lease runs out, read from the store rather than from the membership view.
     *
     * @param nodeId the node
     * @return its lease's expiry if the lease exists and has not run out, empty if it is gone or expired
     * @throws java.io.IOException if the lease cannot be read
     */
    public java.util.OptionalLong leaseLiveUntil(String nodeId) throws java.io.IOException {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return java.util.OptionalLong.empty();
            }
            final var lease = reconciler.plane.membership().read(nodeId);
            if (lease.isEmpty() || lease.get().isExpiredAt(reconciler.plane.clock().getAsLong())) {
                return java.util.OptionalLong.empty();
            }
            return java.util.OptionalLong.of(lease.get().expiresAtMillis());
        }
    }

    /**
     * The second look at a departed node's shards: any still named to it are taken now.
     *
     * @param deadNodeId the node whose lease ran out
     * @param claims the claims {@link #takeOverFrom} left for later
     */
    public void takeOverRemaining(String deadNodeId, List<Map.Entry<String, Integer>> claims) {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return;
            }
            takeOverRemaining(deadNodeId, claims, false);
        }
    }

    /**
     * The second look at a departed node's shards, or the last.
     *
     * <p>The second look leaves alone what a live member other than this one is named to take: ten seconds after a
     * departure the members named are still opening their shares, and every survivor queueing everything not yet taken
     * put a dead node's whole set back in every queue. The last look, a lease later, takes whatever is still the dead
     * node's, whoever is named: a member that was named and could not is not waited on for ever.
     *
     * @param deadNodeId the node whose lease ran out
     * @param claims its shards left for this look
     * @param last whether this is the last look
     * @return the shards left for the last look
     */
    public List<Map.Entry<String, Integer>> takeOverRemaining(String deadNodeId, List<Map.Entry<String, Integer>> claims, boolean last) {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return List.of();
            }
            int taking = 0;
            final List<Map.Entry<String, Integer>> deferred = new java.util.ArrayList<>();
            for (Map.Entry<String, Integer> claim : claims) {
                if (last == false
                    && reconciler.node.nodeCapacity().takeoverBelongsElsewhere(reconciler.plane, claim.getKey(), claim.getValue())) {
                    deferred.add(claim);
                    continue;
                }
                if (takeIfStillHeldBy(deadNodeId, claim)) {
                    taking++;
                }
            }
            if (taking > 0) {
                BackgroundReconciler.logger.info("node {} left: took {} more of its shards that no other survivor had", deadNodeId, taking);
            }
            return deferred;
        }
    }

    boolean takeIfStillHeldBy(String deadNodeId, Map.Entry<String, Integer> claim) {
        try {
            final var head = reconciler.plane.heads().read(claim.getKey(), claim.getValue());
            if (head.isEmpty() || deadNodeId.equals(head.get().ownerNodeId()) == false) {
                return false;
            }
            // Given back rather than taken when everything the dead node acknowledged is published: most of what a
            // node holds is idle, and taking all of it put a dead node's shards on survivors already near their cap.
            // Only a shard whose log is ahead of its commit needs a writer, to replay and publish it.
            final MetadataPlane.Settled settled = reconciler.plane.settleAbandoned(claim.getKey(), claim.getValue(), deadNodeId);
            if (settled == MetadataPlane.Settled.RELEASED_CLEAN) {
                reconciler.settledClean.incrementAndGet();
                return false;
            }
            if (settled == MetadataPlane.Settled.NOT_ABANDONED) {
                return false;
            }
        } catch (Exception e) {
            BackgroundReconciler.logger.debug(
                "could not settle " + claim + " of departed node " + deadNodeId + "; it waits for a request",
                e
            );
            return false;
        }
        reconciler.countSource("departure");
        reconciler.activations.activate(claim.getKey(), claim.getValue(), true);
        return true;
    }

    void collectNodeLogs(List<String> live, int rank, long now, long settledAfter) {
        final java.util.Set<String> enrolled;
        try {
            enrolled = reconciler.plane.membership().enrolledIds();
        } catch (Exception e) {
            BackgroundReconciler.logger.debug("janitor could not read the members index", e);
            return;
        }
        int looked = 0;
        for (String nodeId : new java.util.TreeSet<>(enrolled)) {
            if (looked >= BackgroundReconciler.DEFAULT_NODE_LOGS_PER_PASS) {
                break;
            }
            if (live.contains(nodeId) || Math.floorMod(("node-log#" + nodeId).hashCode(), live.size()) != rank) {
                continue;
            }
            try {
                final var lease = reconciler.plane.membership().read(nodeId);
                if (lease.isEmpty() || lease.get().nodeLog().isEmpty() || now - lease.get().expiresAtMillis() < settledAfter) {
                    continue;
                }
                looked++;
                if (reconciler.plane.collectNodeLog(nodeId) == MetadataPlane.NodeLogCollected.COLLECTED) {
                    reconciler.nodeLogsCollected.incrementAndGet();
                    BackgroundReconciler.logger.info(
                        "janitor: deleted the write log of departed node {}: every shard it named had published",
                        nodeId
                    );
                }
            } catch (Exception e) {
                BackgroundReconciler.logger.debug("janitor could not collect the write log of " + nodeId, e);
            }
        }
    }

    /**
     * Cleans up after crashed nodes, a bounded amount at a time.
     *
     * <p>A crash leaves three things naming the dead node: shard heads, claims, and rollup marks that keep its shards
     * from being ruled out of a wide search. A departure noticed live is cleaned up by {@link #takeOverFrom}; this is
     * for everything else -- a fleet that crashed as a whole, a departure nobody heard, a mark whose owner released
     * its head and could not clear it. Each pass lists the claim directories once and reads one rollup bucket,
     * rotating, and the work is split between live nodes by hash, so the steady cost is a few reads a pass.
     *
     * @param budget how many shards to look at, at most
     * @param replays how many shards to take for replay, at most
     * @return what the pass did
     * @throws IOException if the store cannot be listed
     */
    public JanitorPass sweepAbandoned(int budget, int replays) throws IOException {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return new JanitorPass(0, 0, 0, 0, null, false);
            }
            final long now = reconciler.plane.clock().getAsLong();
            final List<String> live = new java.util.ArrayList<>();
            for (var lease : reconciler.plane.membership().current()) {
                if (lease.isExpiredAt(now) == false) {
                    live.add(lease.nodeId());
                }
            }
            java.util.Collections.sort(live);
            final int rank = live.indexOf(reconciler.node.localNode().getId());
            if (rank < 0) {
                return new JanitorPass(0, 0, 0, 0, null, false);
            }
            final int[] counts = new int[4]; // examined, released, replays, forgotten
            final int[] replaysLeft = { replays };

            // Claims of nodes dead for longer than a live departure takes to be handled -- at most half the budget, or a
            // crashed fleet's thousands of stale claims starve the marks, which are what make a wide search slow.
            final int claimsBudget = Math.max(1, budget / 2);
            final long settledAfter = ReconcileScheduler.RECENT_DEPARTURE_TTLS * reconciler.plane.leaseTtlMillis();
            for (String nodeId : reconciler.plane.nodesWithClaims()) {
                if (counts[0] >= claimsBudget) {
                    break;
                }
                if (live.contains(nodeId)) {
                    continue;
                }
                final var lease = reconciler.plane.membership().read(nodeId);
                if (lease.isPresent() && now - lease.get().expiresAtMillis() < settledAfter) {
                    continue;
                }
                for (Map.Entry<String, Integer> claim : reconciler.plane.claimsOf(nodeId)) {
                    if (counts[0] >= claimsBudget) {
                        break;
                    }
                    if (Math.floorMod((claim.getKey() + "#" + claim.getValue()).hashCode(), live.size()) != rank) {
                        continue;
                    }
                    counts[0]++;
                    try {
                        settleForJanitor(claim.getKey(), claim.getValue(), nodeId, -1L, counts, replaysLeft);
                    } catch (Exception e) {
                        BackgroundReconciler.logger.debug("janitor could not settle " + claim + " of " + nodeId, e);
                    }
                }
            }

            // Write logs of nodes dead for as long: each looked at by the one live node it hashes to, which deletes it once
            // every shard it names has published past it. A node going down moves its share to the others with the live
            // list. One read of the members index, and a lease read for each node not live, whose leases are kept only
            // while they name a log.
            collectNodeLogs(live, rank, now, settledAfter);

            // One rollup bucket, rotating through every group's buckets; this node reads the ones that hash to it.
            final List<String> groups = reconciler.plane.rollups().groups();
            java.util.Collections.sort(groups);
            String read = null;
            final int slots = groups.size() * DigestRollups.BUCKETS;
            // This node's slots are every live-size-th one from its rank; the cursor counts through them, and stays on a
            // bucket until a pass gets through it within the budget -- a crashed fleet leaves hundreds of marks in one.
            final int mine = slots <= rank ? 0 : (slots - rank + live.size() - 1) / live.size();
            if (mine > 0) {
                final long slot = rank + (long) live.size() * Math.floorMod(reconciler.janitorCursor.get(), (long) mine);
                final String group = groups.get((int) (slot / DigestRollups.BUCKETS));
                final int bucket = (int) (slot % DigestRollups.BUCKETS);
                read = group + "/" + bucket;
                for (Map.Entry<String, DigestRollups.Entry> entry : reconciler.plane.rollups().readBucket(group, bucket).entrySet()) {
                    if (entry.getValue().alias() || counts[0] >= budget) {
                        continue;
                    }
                    if (DigestRollups.recordsNothing(entry.getValue())) {
                        // An entry no wide search can rule out: opened once and given back with nothing written, which
                        // needs no entry; or holding data under no digest, which gets the one its commit carries -- or a
                        // replay, whose publish computes one.
                        counts[0]++;
                        try {
                            if (reconciler.plane.forgetIfNothing(entry.getKey())) {
                                counts[1]++;
                            } else {
                                final List<Integer> replay = reconciler.plane.backfillDigest(entry.getKey());
                                if (replay.isEmpty()) {
                                    counts[1]++;
                                }
                                for (int shard : replay) {
                                    if (replaysLeft[0] > 0) {
                                        replaysLeft[0]--;
                                        counts[2]++;
                                        replayAndRelease(entry.getKey(), shard);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            BackgroundReconciler.logger.debug("janitor could not take " + entry.getKey() + " out of its rollup", e);
                        }
                        continue;
                    }
                    final List<DigestRollups.ShardState> states = entry.getValue().states();
                    for (int shard = 0; shard < states.size() && counts[0] < budget; shard++) {
                        final long marked = states.get(shard).ownerTerm();
                        if (marked == 0L) {
                            continue;
                        }
                        counts[0]++;
                        try {
                            final var head = reconciler.plane.heads().read(entry.getKey(), shard);
                            if (head.isEmpty()) {
                                continue;
                            }
                            final String owner = head.get().ownerNodeId();
                            if (owner == null) {
                                settleForJanitor(entry.getKey(), shard, null, marked, counts, replaysLeft);
                            } else if (owner.equals(reconciler.node.localNode().getId()) == false && leaseLiveUntil(owner).isEmpty()) {
                                settleForJanitor(entry.getKey(), shard, owner, -1L, counts, replaysLeft);
                            }
                        } catch (Exception e) {
                            BackgroundReconciler.logger.debug(
                                "janitor could not settle the mark on " + entry.getKey() + "[" + shard + "]",
                                e
                            );
                        }
                    }
                }
                if (counts[0] < budget) {
                    // Got through it: the next pass reads the next bucket.
                    reconciler.janitorCursor.incrementAndGet();
                }
            }
            final JanitorPass pass = new JanitorPass(counts[0], counts[1], counts[2], counts[3], read, counts[0] >= budget);
            if (pass.released() + pass.replays() + pass.claimsForgotten() > 0) {
                BackgroundReconciler.logger.info(
                    "janitor: looked at {} shards, gave back {} fully published, took {} to replay, forgot {} stale claims",
                    pass.examined(),
                    pass.released(),
                    pass.replays(),
                    pass.claimsForgotten()
                );
            }
            return pass;
        }
    }

    /** One shard: given back clean, taken to replay within the budget, or -- a claim whose head moved on -- forgotten. */
    void settleForJanitor(String index, int shard, String deadOwner, long markedTerm, int[] counts, int[] replaysLeft) throws IOException {
        final MetadataPlane.Settled settled = deadOwner == null
            ? reconciler.plane.settleReleased(index, shard, markedTerm)
            : reconciler.plane.settleAbandoned(index, shard, deadOwner);
        switch (settled) {
            case RELEASED_CLEAN -> counts[1]++;
            case NEEDS_REPLAY -> {
                if (deadOwner != null) {
                    reconciler.plane.forgetAssignment(deadOwner, index, shard);
                }
                if (replaysLeft[0] > 0) {
                    replaysLeft[0]--;
                    counts[2]++;
                    replayAndRelease(index, shard);
                }
            }
            case NOT_ABANDONED -> {
                if (deadOwner != null && markedTerm < 0L) {
                    final var head = reconciler.plane.heads().read(index, shard);
                    if (head.isEmpty() || deadOwner.equals(head.get().ownerNodeId()) == false) {
                        reconciler.plane.forgetAssignment(deadOwner, index, shard);
                        counts[3]++;
                    }
                }
            }
        }
    }

    /**
     * Takes a shard left behind its log, replays and publishes it, and gives it straight back.
     *
     * <p>Held after the replay, every such shard sat on this node's cap until idle release: a pass could afford to
     * replay only a few, and a crashed fleet leaves thousands -- every shard whose commit predates commits recording
     * how far into the log they reach looks behind until replayed once. Given back as soon as it is published, a
     * replay costs a slot for seconds. A shard somebody used meanwhile is kept, for the idle release to judge.
     */
    void replayAndRelease(String index, int shard) {
        // The janitor's: counted with its passes, though it runs on the activation threads.
        final java.util.concurrent.atomic.AtomicLong sink = org.opensearch.serverless.store.ObjectStores.currentAttribution();
        reconciler.countSource("janitor");
        reconciler.activations.activate(index, shard, true, sink).whenComplete((taken, failure) -> {
            if (failure != null || taken.isEmpty()) {
                return;
            }
            final ShardId shardId = taken.get();
            final Long opened = reconciler.openedAt.get(shardId);
            final java.util.OptionalLong used = reconciler.node.reconciler().lastUsed(shardId);
            if (opened != null && used.isPresent() && used.getAsLong() > opened) {
                return;
            }
            try {
                org.opensearch.serverless.store.ObjectStores.attributedTo(
                    sink,
                    () -> reconciler.eviction.letGo(shardId, "replayed and published by the janitor; nobody is using it")
                );
            } catch (Exception e) {
                BackgroundReconciler.logger.debug("could not give back " + shardId + " after the janitor replayed it", e);
            }
        });
    }
}
