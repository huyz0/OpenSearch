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
import java.util.ArrayList;
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

    private final BlobContainer container;
    private final LongSupplier clock;
    private final long ttlMillis;
    private final List<Consumer<MembershipDelta>> listeners = new CopyOnWriteArrayList<>();

    private volatile Set<NodeLease> observed = Set.of();
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
        return renewed;
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
        final List<String> unreadable = new ArrayList<>();

        for (String blobName : container.listBlobsByPrefix(LEASE_PREFIX).keySet()) {
            // A register is NOT a plain blob: its bytes carry a generation frame ahead of the value,
            // so a lease written with compareAndSwapRegister must be read with readRegister. Reading it
            // with readBlob yields framed bytes that fail to parse — and, because the catch below
            // treats an unreadable lease as an absent one, that mistake presents as "no nodes exist"
            // rather than as an error. Found exactly that way.
            try {
                final Optional<BlobRegister> register = container.readRegister(blobName);
                if (register.isEmpty()) {
                    continue;
                }
                try (InputStream in = register.get().value().streamInput()) {
                    final NodeLease lease = NodeLease.fromStream(in);
                    if (lease.isExpiredAt(now) == false) {
                        live.add(lease);
                    }
                }
            } catch (IOException e) {
                // A lease being deleted underneath a listing is normal, not exceptional. Skipping an
                // unreadable one is safe: it can only make us believe fewer nodes exist, and nothing
                // about safety depends on the member list (§10.1).
                unreadable.add(blobName);
            }
        }

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
