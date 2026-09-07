/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.membership;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The default {@link MembershipSource}: one register per node under a shared prefix, listed and
 * filtered by expiry. {@code rfc-serverless-shell.md} §10.2.
 *
 * <p>Zero new infrastructure — it needs only a {@link BlobContainer} that implements the register
 * primitive, which S3, GCS, Azure and the filesystem all do. It is fate-shared with the data: if the
 * object store is gone, membership being unreadable is not the pressing problem.
 *
 * <p><b>Poll-only.</b> Object stores have no watch, so this learns about change only when
 * {@link #refresh()} is called. That is the cost §9.5 records, and the reason §10.2 treats a Kubernetes
 * informer as interesting rather than redundant — it is a real push channel for exactly this.
 *
 * <p><b>What the expiry check assumes, and the margin that makes the assumption a budget.</b> A lease
 * carries an expiry stamped by its writer, compared against the reader's clock. Two clocks are involved
 * in every takeover: the holder decides when to stop acknowledging writes by its own clock, and the
 * taker decides when it may steal by its own. If the taker's clock runs ahead by δ, it steals at
 * holder-time {@code expiry − δ}, and every write the holder acknowledged in those δ milliseconds sits
 * behind the successor's seal and is never replayed. That is acknowledged-write loss, and it needs no
 * skew "beyond the TTL" — any positive δ does it. So the two sides are deliberately asymmetric: the taker
 * treats a lease as live until its stamped expiry, exactly as written; the holder treats its own lease as
 * gone {@link #skewMarginMillis()} <em>before</em> that ({@link #selfLeaseValidAt}), and considers every
 * shard it held lost from that moment ({@link #selfLeaseLapsedAt}). The margin is therefore the skew
 * budget: clocks that disagree by less than it cannot lose an acknowledged write. It sits entirely on
 * the holder because the taker's reading is the one every failover latency and every timing contract
 * in the system is measured against; moving it there would make failover slower by the margin for no
 * additional safety.
 *
 * <p>Observer-local liveness — expiring a lease on the observer's own clock, counted from when it last
 * saw the generation move — would remove the assumption rather than budget for it. It is still not
 * implemented, for the reason it never was: it cannot classify a node it has observed only once.
 *
 * <p>Safety does not rest on any of this. A node wrongly believed alive still cannot take a shard it
 * does not hold the head for.
 */
public final class BlobLeaseMembership implements MembershipSource {

    /** Prefix under which leases are written, so the container may hold other things. */
    public static final String LEASE_PREFIX = "lease-";

    /**
     * The register naming every member, kept beside the leases.
     *
     * <p>The index that turns a refresh from a listing into a read. A node adds itself once, when it
     * first renews, and removes itself when it releases; a refresh that finds a listed node with no lease
     * at all prunes it, and one that finds a lease dead for {@link #DEAD_LEASE_PRUNE_TTLS} lease lifetimes
     * prunes that too, blob and all. Nothing on the per-pass path writes it, so it adds no writes to a
     * steady state -- only one compare-and-swap per join, per clean leave and per pruning. A deployment
     * from before the index exists is listed once and the index written from that listing.
     */
    public static final String MEMBERS = "members";

    /**
     * The holder's skew margin as a fraction of the TTL: one thirty-second, about a second at the default
     * thirty-second TTL.
     *
     * <p>Why that and not more. A second is an order of magnitude above the skew a fleet keeping time by
     * NTP actually shows, so it is a real budget rather than a token one. It is also inside the slack the
     * system's own timing contracts already assume: a node that renews later than {@code TTL − margin}
     * after its last renewal was one lost renewal from lapsing anyway, and the scheduler renews at a
     * third of the TTL. A wider margin would buy nothing against well-kept clocks and would shorten the
     * window a node has to renew in; an operator with worse clocks raises it with
     * {@link #setSkewMarginMillis}.
     */
    public static final int SKEW_MARGIN_DIVISOR = 32;

    /**
     * How many lease lifetimes a lease may be expired for before a refresh removes it from the index and
     * deletes its blob.
     *
     * <p>An expired lease that is still present used to be kept forever: "the node may be slow rather than
     * gone". Ten lifetimes is not slow. A fleet that mints a node id per pod incarnation accumulates one
     * dead lease per restart, and every refresh on every node reads every one of them, which is the
     * O(fleet history) term nothing else in the steady state has. A node that does come back after that
     * long renews from an absent register and re-enrols, so pruning costs it one extra index swap.
     */
    public static final int DEAD_LEASE_PRUNE_TTLS = 10;

    private final BlobContainer container;
    private final LongSupplier clock;
    private final long ttlMillis;
    private final List<Consumer<MembershipDelta>> listeners = new CopyOnWriteArrayList<>();

    private volatile Set<NodeLease> observed = Set.of();
    private volatile long lastRefreshedAt = Long.MIN_VALUE;
    private volatile long lastIndexGeneration = BlobRegister.ABSENT_GENERATION;
    private final Set<String> enrolled = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * Each member's lease as this node last read it, kept only while its own stamped expiry is still in
     * the future. This is what makes a refresh cost one register read rather than one per member.
     */
    private final java.util.Map<String, NodeLease> lastRead = new java.util.concurrent.ConcurrentHashMap<>();
    /** The expiry this node last published for itself, so the write path can check it without I/O. */
    private volatile long ownExpiresAtMillis = 0L;
    private volatile long skewMarginMillis;

    private volatile long ownGeneration = BlobRegister.ABSENT_GENERATION;

    /**
     * Creates a membership source over a container of lease registers.
     *
     * @param container a container dedicated to member leases
     * @param clock source of wall-clock millis, injectable so tests need not sleep
     * @param ttlMillis how far ahead a renewal stamps its expiry
     */
    public BlobLeaseMembership(BlobContainer container, LongSupplier clock, long ttlMillis) {
        this.container = container;
        this.clock = clock;
        this.ttlMillis = ttlMillis;
        this.skewMarginMillis = Math.max(0L, ttlMillis / SKEW_MARGIN_DIVISOR);
    }

    /**
     * Sets how far ahead of its stamped expiry this node treats its own lease as gone.
     *
     * @param skewMarginMillis the clock-skew budget; see the class documentation
     * @return this, for chaining
     */
    public BlobLeaseMembership setSkewMarginMillis(long skewMarginMillis) {
        if (skewMarginMillis < 0L || skewMarginMillis >= ttlMillis) {
            throw new IllegalArgumentException("the skew margin must be within [0, ttl): " + skewMarginMillis + " against " + ttlMillis);
        }
        this.skewMarginMillis = skewMarginMillis;
        return this;
    }

    /**
     * Returns the clock-skew budget: how much earlier than its stamped expiry this node stops trusting
     * its own lease.
     *
     * @return the margin in millis
     */
    public long skewMarginMillis() {
        return skewMarginMillis;
    }

    /**
     * Returns the lease TTL this source stamps.
     *
     * @return the TTL in millis
     */
    public long ttlMillis() {
        return ttlMillis;
    }

    /**
     * Writes or renews this node's own lease, stamping expiry at {@code now + ttl}.
     *
     * <p>Uses compare-and-swap against the generation this node last wrote. §9.3 gives each node sole
     * ownership of its own lease register, so a conflict means something unexpected — a duplicate node
     * id, or a restart that lost track — and is resolved by re-reading rather than by forcing.
     *
     * <p><b>This does not check whether the lease had already lapsed.</b> That is the caller's rule to
     * enforce, and it is a rule about shards, not about the register: a node whose lease lapsed has lost
     * every shard it held and must let go of them <em>before</em> it renews, or the renewal reopens the
     * write path on shards a successor has already sealed. {@link #selfLeaseLapsedAt} is the question to
     * ask first; {@code ServerlessNode#renewLease} asks it.
     *
     * <p>Synchronized because two renewals at once -- the renewal timer and a backstop, or an activation
     * -- would each expect the other's generation and both take the re-read path for nothing.
     *
     * @param self the lease to write, whose expiry is replaced
     * @return the lease as written
     * @throws IOException if the register cannot be written
     */
    public synchronized NodeLease renew(NodeLease self) throws IOException {
        final NodeLease renewed = self.renewedUntil(clock.getAsLong() + ttlMillis);
        final String name = LEASE_PREFIX + self.nodeId();
        BlobRegisterCasResult result = container.compareAndSwapRegister(name, ownGeneration, renewed.toBytes());
        if (result.applied() == false) {
            // Re-read and retry once against the generation actually stored -- unless what is stored is
            // another live incarnation under this node id. Two processes with one id share every head
            // and every term, and the exactly-one-writer property has nothing left to stand on; refusing
            // to renew is what lets this node fence itself out of writes rather than fight.
            final Optional<BlobRegister> actual = container.readRegister(name);
            if (actual.isPresent()) {
                try (java.io.InputStream in = actual.get().value().streamInput()) {
                    final NodeLease other = NodeLease.fromStream(in);
                    // One exception, and it is an argument rather than a convenience: a lease at the same
                    // transport address cannot belong to a live process. Two processes cannot both hold
                    // one listening socket on one host, and a node's id is minted by its data directory,
                    // which the node lock keeps to one process at a time. So an unexpired lease under
                    // this id at this address is the previous incarnation of this very node -- a fast
                    // restart -- and waiting a TTL for it to expire would delay this node's first
                    // activation for nothing. A live lease at a *different* address is the genuine
                    // duplicate: a cloned data directory, or a lock bypassed, and refusing is right.
                    if (other.ephemeralId() != null
                        && other.ephemeralId().equals(self.ephemeralId()) == false
                        && other.expiresAtMillis() > clock.getAsLong()
                        && other.address().equals(self.address()) == false) {
                        throw new IOException(
                            "duplicate node id ["
                                + self.nodeId()
                                + "]: another live process ("
                                + other.ephemeralId()
                                + " at "
                                + other.address()
                                + ") holds this node's lease; this node will not renew"
                        );
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception ignored) {
                    // An unreadable lease is not a duplicate; fall through to the retry.
                }
            }
            final long actualGeneration = actual.map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
            result = container.compareAndSwapRegister(name, actualGeneration, renewed.toBytes());
            if (result.applied() == false) {
                throw new IOException(
                    "could not renew lease for " + self.nodeId() + "; register contended at generation " + result.currentGeneration()
                );
            }
            // The register moved underneath this node -- most often because it was collected while the
            // node was away, and a refresh somewhere pruned the id from the index when it found the lease
            // gone. Whatever this node remembered about being enrolled is no longer evidence of anything,
            // so it enrols again below. Without this a node whose lease blob was swept stayed off the
            // index for the life of the process: live, renewing, and invisible to reader placement.
            enrolled.clear();
        }
        ownGeneration = result.currentGeneration();
        ownExpiresAtMillis = renewed.expiresAtMillis();
        if (enrolled.contains(self.nodeId()) == false && enrol(self.nodeId())) {
            enrolled.add(self.nodeId());
        }
        return renewed;
    }

    /**
     * Reports whether this node is currently on the members index, as far as it knows.
     *
     * @param nodeId this node's id
     * @return true once an enrolment has succeeded since the register was last moved underneath it
     */
    public boolean isEnrolled(String nodeId) {
        return enrolled.contains(nodeId);
    }

    /**
     * Adds a node to the members index; true once it is there.
     *
     * <p>Bounded per call and retried on every renewal until it succeeds, so enrolment is retried in the
     * background at the renewal cadence rather than given up on. The whole fleet is this register's
     * writer population, and a co-started fleet contends on it: each round admits one writer, so the
     * attempts here are spread by a short random pause rather than fired back to back, and a node that
     * loses its three still comes back a renewal later.
     */
    private boolean enrol(String nodeId) throws IOException {
        for (int attempt = 0; attempt < 3; attempt++) {
            if (attempt > 0) {
                pauseBriefly(attempt);
            }
            final Optional<BlobRegister> index = container.readRegister(MEMBERS);
            if (index.isEmpty()) {
                // No index yet: this deployment predates it. Built from a listing, once, so a member that
                // never enrolled is not left out.
                final Set<String> ids = listedIds();
                ids.add(nodeId);
                if (container.createRegisterIfAbsent(MEMBERS, encodeIds(ids)).applied()) {
                    return true;
                }
                continue;
            }
            final Set<String> ids = decodeIds(index.get());
            if (ids.contains(nodeId)) {
                return true;
            }
            ids.add(nodeId);
            if (container.compareAndSwapRegister(MEMBERS, index.get().generation(), encodeIds(ids)).applied()) {
                return true;
            }
        }
        return false;
    }

    /** A few to a few tens of milliseconds, growing with the attempt, so contending nodes do not retry in step. */
    private static void pauseBriefly(int attempt) {
        final long upper = Math.min(200L, 10L * (1L << Math.min(attempt, 4)));
        try {
            Thread.sleep(java.util.concurrent.ThreadLocalRandom.current().nextLong(1L, upper + 1L));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Set<String> listedIds() throws IOException {
        final Set<String> ids = new LinkedHashSet<>();
        for (String blobName : container.listBlobsByPrefix(LEASE_PREFIX).keySet()) {
            ids.add(blobName.substring(LEASE_PREFIX.length()));
        }
        return ids;
    }

    private static org.opensearch.core.common.bytes.BytesReference encodeIds(Set<String> ids) {
        return new org.opensearch.core.common.bytes.BytesArray(
            String.join("\n", new java.util.TreeSet<>(ids)).getBytes(java.nio.charset.StandardCharsets.UTF_8)
        );
    }

    private static Set<String> decodeIds(BlobRegister register) {
        final Set<String> ids = new LinkedHashSet<>();
        for (String line : register.value().utf8ToString().split("\n")) {
            if (line.isBlank() == false) {
                ids.add(line.trim());
            }
        }
        return ids;
    }

    /** Takes ids out of the index, one attempt; a lost swap is left for the next refresh to repeat. */
    private void unenrol(Set<String> ids) {
        try {
            final Optional<BlobRegister> index = container.readRegister(MEMBERS);
            if (index.isEmpty()) {
                return;
            }
            final Set<String> remaining = decodeIds(index.get());
            if (remaining.removeAll(ids)) {
                container.compareAndSwapRegister(MEMBERS, index.get().generation(), encodeIds(remaining));
            }
        } catch (Exception ignored) {
            // Best effort: the index over-approximates until the next refresh prunes again, and an
            // over-approximation costs one register read per pruned id, never a wrong answer.
        }
    }

    /**
     * Whether this node's own lease is still valid, by this node's own clock, with no I/O.
     *
     * <p>This exists so the write path can be fenced on every write. A check that costs an object-store
     * read cannot be made per write, so it would not be made at all, and the write path would stay
     * unfenced between heartbeats — which is precisely the window a partitioned node keeps writing in.
     *
     * <p><b>Valid means inside the expiry less the skew margin.</b> The successor's clock may run ahead
     * of this one by up to the margin and still find this lease expired exactly when this node finds it
     * live; stopping the margin early is what keeps that from being an acknowledged write behind a seal.
     * See the class documentation.
     *
     * <p><b>What it is worth, and what it is not.</b> A node stops writing no later than the expiry it
     * last published, less the margin, so the interval in which a zombie can still write is bounded by
     * the TTL rather than by however long it takes the zombie to notice. It does <em>not</em> make writing
     * safe: the check and the write are not atomic, so a long enough pause between them lands a write
     * after the deadline passed. Closing that needs a fence at the log, which is what the replay cutoff in
     * {@code WalStore#replayable(Map)} is for. This bounds; that fences.
     *
     * <p><b>Before the first renewal this answers true</b>, because a node that has never published a
     * lease has no deadline to have missed, and refusing writes on that basis would break a node that
     * writes before its first heartbeat rather than protect anything. The fence engages once there is a
     * lease to lose.
     *
     * @param nowMillis this node's current time
     * @return whether this node may still act as though it holds what it held
     */
    public boolean selfLeaseValidAt(long nowMillis) {
        final long expiry = ownExpiresAtMillis;
        return expiry == 0L || nowMillis < expiry - skewMarginMillis;
    }

    /**
     * Whether this node once held a lease and no longer does, by its own clock and its own margin.
     *
     * <p>The other half of {@link #selfLeaseValidAt}, and the question that has to be asked <em>before</em>
     * renewing: a lease that lapsed and is then renewed looks, to the write path, exactly like one that
     * never lapsed -- but in between, a successor may have taken every shard this node held and sealed
     * every log. The rule this makes explicit is that a node whose own lease lapsed has lost every shard
     * it held, whether or not anyone has actually taken them yet.
     *
     * @param nowMillis this node's current time
     * @return true if a lease was published and has passed its expiry less the margin
     */
    public boolean selfLeaseLapsedAt(long nowMillis) {
        final long expiry = ownExpiresAtMillis;
        return expiry != 0L && nowMillis >= expiry - skewMarginMillis;
    }

    /**
     * Returns the expiry this node last stamped on its own lease, or zero before the first renewal.
     *
     * @return the expiry in wall-clock millis
     */
    public long ownExpiresAtMillis() {
        return ownExpiresAtMillis;
    }

    /**
     * Removes this node's lease. A clean shutdown does not wait for its own TTL to elapse.
     *
     * @param nodeId the node whose lease to drop
     * @throws IOException if the delete fails
     */
    public void release(String nodeId) throws IOException {
        container.deleteBlobsIgnoringIfNotExists(List.of(LEASE_PREFIX + nodeId));
        ownGeneration = BlobRegister.ABSENT_GENERATION;
        // The lease first, then the index: a refresh between the two finds a listed node with no lease
        // and prunes it, which is the same end state.
        unenrol(Set.of(nodeId));
        enrolled.remove(nodeId);
    }

    /**
     * Refreshes only if the snapshot is older than the given age.
     *
     * <p>What a request path calls. A search refreshed membership on every request, which was one
     * listing and a read per node per search; the snapshot is good for as long as a lease is, so a
     * fraction of the lease's life is the right age.
     *
     * @param maxAgeMillis how old the snapshot may be
     * @return the live members
     * @throws IOException if the store cannot be read
     */
    @Override
    public Set<NodeLease> refreshIfOlderThan(long maxAgeMillis) throws IOException {
        final long now = clock.getAsLong();
        if (lastRefreshedAt == Long.MIN_VALUE || now - lastRefreshedAt >= maxAgeMillis) {
            return refresh();
        }
        // Young enough, but a join or a clean leave moves the index's generation, and one register read
        // tells whether it moved: a node that just joined is visible to the next request rather than to
        // the first one after the snapshot ages out.
        final Optional<BlobRegister> index = container.readRegister(MEMBERS);
        if (index.isPresent() && index.get().generation() == lastIndexGeneration) {
            return observed;
        }
        return refresh();
    }

    /**
     * Reads one node's lease directly, without a listing.
     *
     * <p>Used to answer liveness at activation time, where freshness matters more than the cost of a
     * read: taking a shard from a node that is actually alive is the one mistake the whole protocol
     * exists to prevent.
     *
     * @param nodeId the node
     * @return its lease, or empty if absent or unreadable
     * @throws IOException if the register cannot be read
     */
    public Optional<NodeLease> read(String nodeId) throws IOException {
        final Optional<BlobRegister> register = container.readRegister(LEASE_PREFIX + nodeId);
        if (register.isEmpty()) {
            return Optional.empty();
        }
        try (InputStream in = register.get().value().streamInput()) {
            return Optional.of(NodeLease.fromStream(in));
        }
    }

    @Override
    public Set<NodeLease> current() {
        return observed;
    }

    @Override
    public void subscribe(Consumer<MembershipDelta> listener) {
        listeners.add(listener);
    }

    @Override
    public Set<NodeLease> refresh() throws IOException {
        final long now = clock.getAsLong();
        final Set<NodeLease> live = new LinkedHashSet<>();
        final Set<String> missing = new LinkedHashSet<>();
        final Set<String> longDead = new LinkedHashSet<>();

        // The members index, and a listing only when there is none yet -- in which case the index is
        // written from that listing so the next refresh is a read.
        final Optional<BlobRegister> index = container.readRegister(MEMBERS);
        final Set<String> ids;
        if (index.isPresent()) {
            ids = decodeIds(index.get());
            lastIndexGeneration = index.get().generation();
        } else {
            ids = listedIds();
            try {
                container.createRegisterIfAbsent(MEMBERS, encodeIds(ids));
            } catch (Exception ignored) {
                // Another node may be writing it at the same moment; either copy is a full listing.
            }
        }
        for (String nodeId : ids) {
            // A register is NOT a plain blob: its bytes carry a generation frame ahead of the value,
            // so a lease written with compareAndSwapRegister must be read with readRegister. Reading it
            // with readBlob yields framed bytes that fail to parse — and, because the catch below
            // treats an unreadable lease as an absent one, that mistake presents as "no nodes exist"
            // rather than as an error. Found exactly that way.
            // A lease this node has already read and whose own stamp says it has not run out yet. Skipped,
            // and that is the whole saving: this loop was one register read per member on every refresh,
            // so a fleet of N nodes spent N reads each and N-squared between them, to be told that leases
            // renewed every third of a lifetime had not expired in the last half of one.
            //
            // Safe because an expiry only ever moves forward -- renewal writes now + ttl, and nothing
            // shortens it -- so a stamp still in the future cannot have become false. What it can miss is
            // a node that left cleanly or restarted at another address inside the remaining lifetime, and
            // neither is a safety question: this snapshot picks reader placements and renders _cat and
            // _nodes. Nothing that decides ownership consults it. The liveness oracle behind
            // ShardHeadStore reads the lease directly, and so does ShardRouter when it resolves an address
            // to forward to, precisely so that being wrong here costs a retry rather than a shard.
            final NodeLease known = lastRead.get(nodeId);
            if (known != null && known.isExpiredAt(now) == false) {
                live.add(known);
                continue;
            }
            try {
                final Optional<BlobRegister> register = container.readRegister(LEASE_PREFIX + nodeId);
                if (register.isEmpty()) {
                    lastRead.remove(nodeId);
                    // Listed and gone: a clean leave whose index swap was lost. Pruned below.
                    missing.add(nodeId);
                    continue;
                }
                try (InputStream in = register.get().value().streamInput()) {
                    final NodeLease lease = NodeLease.fromStream(in);
                    if (lease.isExpiredAt(now) == false) {
                        live.add(lease);
                        lastRead.put(nodeId, lease);
                    } else if (now - lease.expiresAtMillis() >= DEAD_LEASE_PRUNE_TTLS * ttlMillis) {
                        // Expired for many lifetimes. A slow node is expired for seconds; this is a node
                        // that is gone, and every refresh everywhere has been paying a read to confirm it.
                        // Pruned below, blob and index entry both. Recently expired leases are kept: the
                        // node may be slow rather than gone, and it renews the same register when it
                        // returns.
                        longDead.add(nodeId);
                    }
                }
                if (live.contains(lastRead.get(nodeId)) == false) {
                    // Expired, or read as something this pass did not accept. Forgotten, so the next
                    // refresh reads it again and sees a renewal the moment there is one.
                    lastRead.remove(nodeId);
                }
            } catch (IOException e) {
                lastRead.remove(nodeId);
                // A lease being deleted underneath a read is normal, not exceptional. Skipping an
                // unreadable one is safe: it can only make us believe fewer nodes exist, and nothing
                // about safety depends on the member list (§10.1).
            }
        }
        if (longDead.isEmpty() == false) {
            // The index entry first, then the blob. In the other order a refresh between the two finds a
            // listed id with no lease, which is also pruned -- the same end state -- but this order means
            // a node that comes back in the gap renews a register that still exists and is simply
            // re-enrolled by the swap below losing to its own. Best effort on both: a failure leaves the
            // lease for the next refresh to prune again.
            unenrol(longDead);
            final List<String> blobs = new java.util.ArrayList<>();
            for (String nodeId : longDead) {
                blobs.add(LEASE_PREFIX + nodeId);
            }
            try {
                container.deleteBlobsIgnoringIfNotExists(blobs);
            } catch (Exception ignored) {
                // The index no longer names them, so nothing reads them again; the blobs are a storage
                // cost only, and the next refresh that lists (none, once the index exists) would collect.
            }
        }
        if (missing.isEmpty() == false && index.isPresent()) {
            unenrol(missing);
        }
        lastRefreshedAt = now;

        final Set<NodeLease> previous = observed;
        final Set<NodeLease> joined = new HashSet<>(live);
        joined.removeAll(previous);
        final Set<NodeLease> left = new HashSet<>(previous);
        left.removeAll(live);
        observed = Set.copyOf(live);

        final MembershipDelta delta = new MembershipDelta(joined, left);
        if (delta.isEmpty() == false) {
            for (Consumer<MembershipDelta> listener : listeners) {
                listener.accept(delta);
            }
        }
        return observed;
    }

    @Override
    public void close() {
        listeners.clear();
    }
}
