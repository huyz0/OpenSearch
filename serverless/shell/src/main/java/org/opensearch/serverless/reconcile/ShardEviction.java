/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Lets go of {@link BackgroundReconciler}'s shards: idle shards, shards evicted to make room under the cap, and
 * shards handed to a member with room.
 */
final class ShardEviction {

    private final BackgroundReconciler reconciler;

    ShardEviction(BackgroundReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Lets go of shards nobody has asked about, so that a node can shrink instead of only ever growing.
     *
     * <p><b>Until this existed, nothing ever released a shard voluntarily.</b> A shard was let go only
     * when ownership was taken away — fenced, reassigned, or the lease lost — so a node that answered one
     * query for an index held that shard for the life of the process, and {@code maxShardsHeld} was a
     * refusal rather than an eviction. "Scale to zero" was something an operator did by killing the
     * process, not something the system did.
     *
     * <p><b>The trade is measured on both sides.</b> Holding an idle shard
     * costs reads on every reconcile tick, in proportion to how many are held. Releasing one and opening
     * it again costs about 41 object-store requests, of which 12 are data. So releasing pays as soon as a
     * shard stays cold for longer than the re-open cost divided by the per-tick holding cost, and the
     * default below is chosen to sit well beyond that rather than at it — a shard released a moment before
     * it was wanted costs both numbers and gains nothing.
     *
     * <p><b>The order is the correctness argument, and it is not the obvious one.</b> A writer publishes,
     * <em>then</em> releases its head, <em>then</em> closes. Publishing first is not what makes the data
     * safe — the log already does that, and a successor would replay it — it is what stops the release
     * handing the next owner a log to replay for no reason. Releasing the head before closing is the part
     * that matters: a node that closed a shard while the head still named it would answer 503 for that
     * shard until its lease lapsed, having volunteered to become the thing failover exists to route
     * around. If the head cannot be released, the shard is kept.
     *
     * <p>A shard that has never been asked for anything is treated as used when it opened, not as
     * infinitely idle — otherwise a demand-driven node would release each shard immediately after taking
     * it and spin.
     *
     * <p>A pinned index ({@link BackgroundReconciler#pin}) is exempt here too, not only from cap eviction: "this stays
     * resident" would be a thin promise if it only held during a busy hour and let go the moment traffic
     * quieted down.
     *
     * @param nowMillis the observer's clock
     * @return the shards released for idleness
     */
    public Set<ShardId> releaseIdle(long nowMillis) {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            final Set<ShardId> letGoSet = new java.util.LinkedHashSet<>();
            if (reconciler.idleAfterMillis <= 0) {
                return letGoSet;
            }
            for (ShardId shardId : reconciler.node.reconciler().openShards()) {
                if (reconciler.pinnedIndices.contains(shardId.getIndexName())) {
                    continue;
                }
                final long lastUsed = reconciler.node.reconciler()
                    .lastUsed(shardId)
                    .orElse(reconciler.openedAt.getOrDefault(shardId, nowMillis));
                if (nowMillis - lastUsed < reconciler.idleAfterMillis) {
                    continue;
                }
                if (letGo(shardId, "idle for " + (nowMillis - lastUsed) + "ms")) {
                    letGoSet.add(shardId);
                }
            }
            for (ShardId shardId : letGoSet) {
                reconciler.openedAt.remove(shardId);
                // Stop trying to take it straight back. A demand-driven node re-acquires on the next write or
                // read for it, which is the point; a node that was told to want it keeps wanting it.
                if (reconciler.demandDriven) {
                    reconciler.wanted.remove(Map.entry(shardId.getIndexName(), shardId.id()));
                }
            }
            return letGoSet;
        }
    }

    /**
     * Gives up the least recently used shard, if one has been idle long enough to be worth giving up.
     *
     * <p><b>Why a node at its cap should evict rather than refuse.</b> The cap exists to bound what a node
     * holds, not to decide which shards it holds. A node that reached it and then refused everything new
     * would serve whatever it happened to acquire first for as long as it ran — the working set frozen at
     * whatever arrived earliest, which is the opposite of what a demand-driven node is for.
     *
     * <p><b>Why there is a grace period, and why it is not zero.</b> Evicting the least recently used shard
     * unconditionally means a node holding N+1 hot shards evicts one to serve another on every request, and
     * spends its life opening and closing shards instead of answering. A shard is only given up if nobody
     * has touched it for {@link BackgroundReconciler#setEvictAfterMillis(long)} — so a full node whose shards are all in use
     * still refuses, and that refusal is a real capacity signal rather than thrash.
     *
     * <p>Shorter than the idle threshold on purpose: idle release is housekeeping that can wait for a tick,
     * and this is a request waiting for room now.
     *
     * @return true if a shard was released and there is room
     */
    boolean makeRoom() {
        if (reconciler.evictAfterMillis <= 0) {
            return false;
        }
        // The floor eviction clears to, not merely the one slot the caller needs -- see
        // DEFAULT_EVICT_HEADROOM_FRACTION for why clearing further than the immediate request pays for
        // itself under sustained demand. Never above the cap itself, so a headroom fraction close to or
        // above 1.0 cannot make this evict shards nobody needed room for.
        evictDownTo(Math.min(reconciler.maxShardsHeld, Math.max(0, reconciler.maxShardsHeld - headroomShards())), Integer.MAX_VALUE, false);
        return reconciler.node.reconciler().heldShards().size() < reconciler.maxShardsHeld;
    }

    /**
     * Gives work to a member with room: a node nearly at its cap, when another live writer has room, lets go of shards
     * past their eviction grace -- least recently used first, down to {@link BackgroundReconciler#HAND_OFF_DOWN_TO} of its cap and at most a
     * tenth of it a pass -- so the next write for one is taken where there is room.
     *
     * <p>Only when someone has room. Shedding on a timer whatever the fleet looked like was measured and dropped: every
     * node kept giving shards up and taking them back, and misrouted writes doubled. A node that joins, or one restarted
     * empty, is what this is for -- without it the full ones stayed full until their shards went idle five minutes later,
     * and the new node took only what happened to arrive at it.
     *
     * @return how many shards were let go
     */
    public int handOffToMemberWithRoom() {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return 0;
            }
            if (reconciler.demandDriven == false || reconciler.evictAfterMillis <= 0) {
                return 0;
            }
            if (reconciler.node.nodeCapacity().nearlyFull()
                && reconciler.node.nodeCapacity().memberWithRoom(reconciler.plane).isPresent()) {
                final int floor = (int) Math.floor(reconciler.maxShardsHeld * BackgroundReconciler.HAND_OFF_DOWN_TO);
                return evictDownTo(floor, Math.max(1, reconciler.maxShardsHeld / 10), true);
            }
            // Far above its share, near its cap or not, while a member is below the mean: down towards the mean, at the same
            // pace. The writes for what it lets go are steered to that member, so they do not come straight back.
            if (reconciler.node.nodeCapacity().aboveFleetShare(reconciler.plane)
                && reconciler.node.nodeCapacity().memberBelowMean(reconciler.plane).isPresent()) {
                final int floor = (int) Math.ceil(
                    reconciler.node.nodeCapacity().fleetMeanHeld(reconciler.plane)
                        * org.opensearch.serverless.shell.NodeCapacity.HAND_OFF_DOWN_TO_MEAN
                );
                return evictDownTo(floor, Math.max(1, reconciler.maxShardsHeld / 10), true);
            }
            return 0;
        }
    }

    int evictDownTo(int floor, int most, boolean handOff) {
        int evicted = 0;
        while (reconciler.node.reconciler().heldShards().size() > floor && evicted < most) {
            final long now = reconciler.plane.clock().getAsLong();
            final ShardId victim = pickVictim(now);
            if (victim == null) {
                break;
            }
            final long victimLastUsed = reconciler.node.reconciler().lastUsed(victim).orElse(reconciler.openedAt.getOrDefault(victim, now));
            final String why = handOff ? "handed off to a member with room" : "evicted to make room";
            if (letGo(victim, why + ", unused for " + (now - victimLastUsed) + "ms") == false) {
                // The head could not be released; letGo already logged why. Stopping rather than trying a
                // different victim next: a head release failing once tends to keep failing, and spinning
                // through every open shard to find one that happens to release is work for no room gained.
                break;
            }
            reconciler.openedAt.remove(victim);
            if (reconciler.demandDriven) {
                reconciler.wanted.remove(Map.entry(victim.getIndexName(), victim.id()));
            }
            reconciler.node.nodeCapacity().noteEvicted(handOff);
            evicted++;
        }
        if (evicted > 0) {
            BackgroundReconciler.logger.info(
                "{} {} shard(s), {} held against a cap of {} (floor {})",
                handOff ? "handed off" : "evicted to make room",
                evicted,
                reconciler.node.reconciler().heldShards().size(),
                reconciler.maxShardsHeld,
                floor
            );
        }
        return evicted;
    }

    /** The number of shards headroom clears beyond the one slot immediately needed, at least one. */
    int headroomShards() {
        return Math.max(1, (int) Math.round(reconciler.maxShardsHeld * reconciler.evictHeadroomFraction));
    }

    /**
     * Picks the least recently used evictable shard, or null if none qualifies.
     *
     * <p>A pinned index's shards are never candidates, at any age -- pinning means "this stays resident,"
     * not "this stays resident a little longer than the rest."
     *
     * @param now the observer's clock
     * @return the shard to evict next, or null
     */
    ShardId pickVictim(long now) {
        ShardId victim = null;
        long victimLastUsed = Long.MAX_VALUE;
        for (ShardId shardId : reconciler.node.reconciler().openShards()) {
            if (reconciler.pinnedIndices.contains(shardId.getIndexName())) {
                continue;
            }
            final long lastUsed = reconciler.node.reconciler().lastUsed(shardId).orElse(reconciler.openedAt.getOrDefault(shardId, now));
            if (now - lastUsed < reconciler.evictAfterMillis) {
                continue;
            }
            if (lastUsed < victimLastUsed) {
                victim = shardId;
                victimLastUsed = lastUsed;
            }
        }
        return victim;
    }

    /**
     * Gives up one shard, in the only order that is safe.
     *
     * <p><b>Publish, then release the head, then close.</b> Publishing first is not what makes the data
     * durable — the log already did that — it is what keeps a successor from having to replay more than it
     * must. Releasing the head before closing is the part that matters: a node that closed a shard while
     * the head still named it would answer 503 for a shard it had told the world it owns.
     *
     * <p>Shared by idle release and by eviction, because two copies of this sequence is two chances for one
     * of them to get the order wrong.
     *
     * @param shardId the shard to give up
     * @param reason why, for the log
     * @return true if it was released
     */
    boolean letGo(ShardId shardId, String reason) {
        try {
            // A reader holds no head and no claim on anything, so closing it loses nothing -- decided and done in one
            // step, because a reader that has become this node's writer since the caller chose it holds a head and
            // must go the writer's way below.
            switch (reconciler.node.reconciler().releaseReader(shardId, null, reason)) {
                case RELEASED -> {
                    reconciler.node.readers().forgetReader(shardId);
                    return true;
                }
                case BUSY -> {
                    return false;
                }
                case NOT_A_READER -> {
                    // A writer: below.
                }
            }
            // Under the shard's fence, from before the publish until after the close. A write that has
            // applied to the engine and is about to append to the log holds the other side of that fence;
            // without it the sequence below interleaved with one: the publish completed, a request thread
            // applied a document, this thread released the head, a successor acquired and sealed the log,
            // and the request thread then appended behind the seal and acknowledged. Nobody replays a
            // record behind a seal. The fence makes the release wait for writes in flight and makes writes
            // that arrive during it wait for the close -- after which they find no shard and are refused,
            // which is the truthful answer.
            return reconciler.node.fences().underShardFence(shardId, () -> letGoFenced(shardId, reason));
        } catch (Exception e) {
            // Releasing is an optimisation. Failing to do it costs money and nothing else, so it is
            // logged and retried on the next pass rather than propagated into the tick.
            BackgroundReconciler.logger.warn("could not release shard " + shardId, e);
            return false;
        }
    }

    /** The body of {@link #letGo} for a writer, run while the shard's fence is held. */
    boolean letGoFenced(ShardId shardId, String reason) throws Exception {
        final var shard = reconciler.node.reconciler().shard(shardId);
        if (shard == null || reconciler.node.reconciler().isReader(shardId)) {
            return false;
        }
        final long ownedTerm = shard.getOperationPrimaryTerm();
        final var head = reconciler.plane.heads().read(shardId.getIndexName(), shardId.id());
        if (head.isEmpty()
            || reconciler.node.localNode().getId().equals(head.get().ownerNodeId()) == false
            || head.get().term() != ownedTerm
            || (head.get().ownerEphemeralId() != null
                && reconciler.node.localNode().getEphemeralId().equals(head.get().ownerEphemeralId()) == false)
            || (head.get().indexUuid() != null && shardId.getIndex().getUUID().equals(head.get().indexUuid()) == false)) {
            // A stable node ID may now belong to a different process or index lifetime.
            return false;
        }
        final long maxSeqNo = shard == null ? -1L : shard.seqNoStats().getMaxSeqNo();
        // Publish only when there is something the last publish did not cover. An unconditional publish
        // here forced a fresh commit, uploaded a new segments_N and swapped the manifest on a shard
        // nobody had written to since it was last published -- seven requests and an orphan blob per
        // idle release, times every shard a node under cap pressure cycles through. The head release is
        // still required either way.
        //
        // And never for a shard whose log could not be appended to: its engine holds an operation the
        // log does not, and publishing would make the refusal the caller was given a lie. A successor
        // rebuilds it from the log, which is the state the caller was told about.
        // Unchanged only if the last publish's commit covered every operation this shard has: see #publish.
        final boolean unchanged = maxSeqNo < 0 || maxSeqNo <= reconciler.lastPublishedMaxSeqNo.getOrDefault(shardId, -1L);
        if (unchanged == false && reconciler.node.fences().isWriteFenced(shardId) == false) {
            synchronized (reconciler.publishLock(shardId)) {
                reconciler.lastManifests.put(shardId, reconciler.node.shards().publishShard(shardId, head.get().term()));
            }
        } else {
            // A staged commit is written before the head is let go, so the successor restores it rather than replaying.
            synchronized (reconciler.publishLock(shardId)) {
                final var staged = reconciler.node.reconciler().checkpoint(shardId, false);
                if (staged != null) {
                    reconciler.lastManifests.put(shardId, staged);
                }
            }
        }
        if (reconciler.plane.heads()
            .release(
                shardId.getIndexName(),
                shardId.id(),
                reconciler.node.localNode().getId(),
                ownedTerm,
                shardId.getIndex().getUUID(),
                reconciler.node.localNode().getEphemeralId()
            ) == false) {
            // Somebody else's head now, or the swap lost. Keep the shard and try again next pass;
            // closing it while the head still points here is the one outcome to avoid.
            BackgroundReconciler.logger.warn("could not release the head for {}; keeping it open", shardId);
            return false;
        }
        reconciler.node.reconciler().releaseShard(shardId, reason);
        reconciler.lastPublishedMaxSeqNo.remove(shardId);
        reconciler.lastPublishedAtMillis.remove(shardId);
        // Unmarked only now, with the last publish recorded and no writer left: from here a coordinator may
        // rule the shard out from its digest, so the clear also widens the entry to the published commit, which
        // the publishes made under the mark did not. A clear that fails leaves it marked, which costs a check.
        try {
            // This writer's last publish is the published commit while it held the head; read only if it never published.
            final org.opensearch.serverless.store.CommitManifest mine = reconciler.lastManifests.get(shardId);
            final Optional<org.opensearch.serverless.store.CommitManifest> published = mine != null && mine.term() == head.get().term()
                ? Optional.of(mine)
                : reconciler.plane.segmentPublisher(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id()).readManifest();
            reconciler.plane.rollups()
                .clearOwned(
                    shardId.getIndexName(),
                    shardId.getIndex().getUUID(),
                    shardId.id(),
                    head.get().term(),
                    published.map(org.opensearch.serverless.store.CommitManifest::digest).orElse(null)
                );
        } catch (Exception e) {
            BackgroundReconciler.logger.warn(
                "could not clear the writer mark of " + shardId + " in its digest rollup; searches will not skip it",
                e
            );
        }
        // And forget the claim, so a node that churns through shards does not read a head per stale
        // claim on every heartbeat for the rest of its life.
        reconciler.plane.forgetAssignment(reconciler.node.localNode().getId(), shardId.getIndexName(), shardId.id());
        if (maxSeqNo < 0) {
            // Opened and given back with nothing ever written: its entry would keep every wide search from ruling
            // it out. See MetadataPlane#forgetIfNothing.
            try {
                reconciler.plane.forgetIfNothing(shardId.getIndexName());
            } catch (Exception e) {
                BackgroundReconciler.logger.debug("could not take empty " + shardId + " out of its rollup", e);
            }
        }
        return true;
    }

    /**
     * Publishes and releases every writer shard this node holds, for a clean shutdown.
     *
     * <p>Without this a node that stopped cleanly left its heads in place, and on restart with the same id
     * inherited them at the old term while every peer read them as dead. Handing them over is what makes
     * a restart a handover rather than a failover: the next owner acquires at a fresh term, and nothing
     * this node acknowledged is behind a seal.
     *
     * @return how many shards were handed over
     */
    public int handOverAll() {
        int handed = 0;
        for (ShardId shardId : new java.util.ArrayList<>(reconciler.node.reconciler().openShards())) {
            if (reconciler.node.reconciler().isReader(shardId)) {
                continue;
            }
            if (letGo(shardId, "shutting down")) {
                handed++;
            }
        }
        return handed;
    }
}
