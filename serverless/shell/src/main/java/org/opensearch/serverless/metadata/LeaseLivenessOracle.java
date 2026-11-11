/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.serverless.membership.BlobLeaseMembership;

import java.util.function.LongSupplier;

/**
 * Answers whether a node is live, and revokes a dead one's lease, from the node leases. One revocation is shared among
 * the takeovers of one dead node's shards running at the same moment. Split out of {@link MetadataPlane}, where it was
 * an anonymous class.
 */
final class LeaseLivenessOracle implements LivenessOracle {

    private final BlobLeaseMembership leases;
    private final LongSupplier clock;
    private final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.CompletableFuture<Boolean>> revocations =
        new java.util.concurrent.ConcurrentHashMap<>();

    LeaseLivenessOracle(BlobLeaseMembership leases, LongSupplier clock) {
        this.leases = leases;
        this.clock = clock;
    }

    @Override
    public boolean isLive(String nodeId, String ephemeralId) {
        try {
            final var lease = leases.read(nodeId);
            // An ephemeral id that has moved on means the process that took the shard is gone, even
            // though a node with the same name is back. It does not inherit the claim.
            return lease.isPresent()
                && lease.get().isExpiredAt(clock.getAsLong()) == false
                && (ephemeralId == null || ephemeralId.equals(lease.get().ephemeralId()));
        } catch (java.io.IOException e) {
            // Unreadable is not "dead": refusing to acquire is the safe answer, since the cost is a
            // retry and the cost of the alternative is two writers.
            return true;
        }
    }

    @Override
    public boolean revoke(String nodeId, String ephemeralId) {
        return revoking(nodeId, ephemeralId, () -> leases.revoke(nodeId, ephemeralId));
    }

    /** The revocation reads the lease and refuses a live one, so it answers both questions at once. */
    @Override
    public boolean revokeUnlessLive(String nodeId, String ephemeralId) {
        return revoke(nodeId, ephemeralId);
    }

    /**
     * Shares one revocation among the takeovers of one dead node's shards running at the same moment. Each used
     * to read the lease and swap it, and every swap but the first lost and read it again: three shards taken
     * together cost five reads and three swaps of one register. Only calls in flight share an answer, so a
     * node that renews afresh after its revocation is judged by a read made after it did.
     */
    private boolean revoking(String nodeId, String ephemeralId, java.util.function.BooleanSupplier revocation) {
        final String key = nodeId + "#" + ephemeralId;
        final java.util.concurrent.CompletableFuture<Boolean> mine = new java.util.concurrent.CompletableFuture<>();
        final java.util.concurrent.CompletableFuture<Boolean> running = revocations.putIfAbsent(key, mine);
        if (running != null) {
            return running.join();
        }
        try {
            final boolean revoked = revocation.getAsBoolean();
            mine.complete(revoked);
            return revoked;
        } catch (RuntimeException e) {
            // A revocation that failed is not one: refusing the takeover is the safe answer.
            mine.complete(false);
            throw e;
        } finally {
            revocations.remove(key, mine);
        }
    }
}
