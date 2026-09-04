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
 * <p><b>What the expiry check assumes.</b> A lease carries an expiry stamped by its writer, compared
 * against the reader's clock. That trusts the two clocks to be roughly aligned; skew beyond the TTL
 * makes a live node look dead, or a dead one live. The refinement that removes the assumption is
 * observer-local liveness — track when *this* node last saw a lease's generation change, and expire on
 * the observer's own clock only. It is deliberately not implemented here: it cannot classify a node
 * until it has been observed twice, which a freshly started node has not done, and phase 1 has no need
 * of it. Recorded so the assumption is visible rather than inherited.
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
     * at all prunes it. Nothing on the per-pass path writes it, so it adds no writes to a steady state --
     * only one compare-and-swap per join and per clean leave. A deployment from before the index exists
     * is listed once and the index written from that listing.
     */
    public static final String MEMBERS = "members";

    private final BlobContainer container;
    private final LongSupplier clock;
    private final long ttlMillis;
    private final List<Consumer<MembershipDelta>> listeners = new CopyOnWriteArrayList<>();

    private volatile Set<NodeLease> observed = Set.of();
    private volatile long lastRefreshedAt = Long.MIN_VALUE;
    private volatile long lastIndexGeneration = BlobRegister.ABSENT_GENERATION;
    private final Set<String> enrolled = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** The expiry this node last published for itself, so the write path can check it without I/O. */
    private volatile long ownExpiresAtMillis = 0L;

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
    }

    /**
     * Writes or renews this node's own lease, stamping expiry at {@code now + ttl}.
     *
     * <p>Uses compare-and-swap against the generation this node last wrote. §9.3 gives each node sole
     * ownership of its own lease register, so a conflict means something unexpected — a duplicate node
     * id, or a restart that lost track — and is resolved by re-reading rather than by forcing.
     *
     * @param self the lease to write, whose expiry is replaced
     * @return the lease as written
     * @throws IOException if the register cannot be written
     */
    public NodeLease renew(NodeLease self) throws IOException {
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
                    if (other.ephemeralId() != null
                        && other.ephemeralId().equals(self.ephemeralId()) == false
                        && other.expiresAtMillis() > clock.getAsLong()) {
                        throw new IOException(
                            "duplicate node id ["
                                + self.nodeId()
                                + "]: another live process ("
                                + other.ephemeralId()
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
        }
        ownGeneration = result.currentGeneration();
        ownExpiresAtMillis = renewed.expiresAtMillis();
        if (enrolled.contains(self.nodeId()) == false && enrol(self.nodeId())) {
            enrolled.add(self.nodeId());
        }
        return renewed;
    }

    /** Adds a node to the members index; true once it is there. Bounded, and retried on the next renewal. */
    private boolean enrol(String nodeId) throws IOException {
        for (int attempt = 0; attempt < 3; attempt++) {
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
     * <p><b>What it is worth, and what it is not.</b> A node stops writing no later than the expiry it
     * last published, so the interval in which a zombie can still write is bounded by the TTL rather than
     * by however long it takes the zombie to notice. It does <em>not</em> make writing safe: the check and
     * the write are not atomic, so a long enough pause between them lands a write after the deadline
     * passed. Closing that needs a fence at the log, which is what the replay cutoff in
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
        return expiry == 0L || nowMillis < expiry;
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
            try {
                final Optional<BlobRegister> register = container.readRegister(LEASE_PREFIX + nodeId);
                if (register.isEmpty()) {
                    // Listed and gone: a clean leave whose index swap was lost. Pruned below. An expired
                    // lease that is still there is not pruned -- the node may be slow rather than gone,
                    // and it renews the same register when it returns.
                    missing.add(nodeId);
                    continue;
                }
                try (InputStream in = register.get().value().streamInput()) {
                    final NodeLease lease = NodeLease.fromStream(in);
                    if (lease.isExpiredAt(now) == false) {
                        live.add(lease);
                    }
                }
            } catch (IOException e) {
                // A lease being deleted underneath a read is normal, not exceptional. Skipping an
                // unreadable one is safe: it can only make us believe fewer nodes exist, and nothing
                // about safety depends on the member list (§10.1).
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
