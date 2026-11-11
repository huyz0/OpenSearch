/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.IOException;
import java.util.Optional;

/**
 * Where one request's documents belong: the descriptor and owner lookups it has already made, the placement of a
 * document, and what to do when a forward to the owner fails.
 *
 * <p>Split out of {@link ShardOperations}, which builds one per request, so these memos live exactly as long as
 * before.
 */
final class RequestRouting {

    private final ServerlessNode node;
    private final MetadataPlane plane;
    private final boolean refuseSystemIndices;

    RequestRouting(ServerlessNode node, MetadataPlane plane, boolean refuseSystemIndices) {
        this.node = node;
        this.plane = plane;
        this.refuseSystemIndices = refuseSystemIndices;
    }

    /**
     * What this request has already looked up.
     *
     * <p><b>Scoped to the request because this object is.</b> One of these is built per REST request and per
     * client call, so memoising inside it is memoising for exactly as long as it is safe to: a multi-get of
     * ten documents in one index read that index's descriptor ten times, and each shard's head once per
     * document that landed on it. Measurement made that visible — ten documents fetched singly cost thirty
     * object-store requests and the same ten as one multi-get cost twenty, which is batching that does not
     * batch the part that matters.
     *
     * <p><b>What it costs is freshness within one request, and that changes nothing.</b> A shard-head read
     * at the start of a request can be stale by the end of it whether or not it was read again — routing is
     * a moment in time, and every caller of this already handles being wrong about the owner. Reading it
     * twice narrows no window; it only pays twice for the same answer.
     *
     * <p>{@code computeIfAbsent} rather than check-then-put, so two items for the same index cannot both
     * pay: the second blocks on the first rather than racing it, which makes the saving a number rather
     * than a likelihood.
     */
    final java.util.concurrent.ConcurrentHashMap<String, Optional<IndexDescriptor>> described =
        new java.util.concurrent.ConcurrentHashMap<>();

    final java.util.concurrent.ConcurrentHashMap<String, Optional<String>> owners = new java.util.concurrent.ConcurrentHashMap<>();

    void assumeDescribed(IndexDescriptor descriptor) {
        described.put(descriptor.name(), Optional.of(descriptor));
    }

    Optional<IndexDescriptor> describeOnce(String index) throws IOException {
        // Every use here is placement -- a shard number and a uuid -- so the routing cache answers it: a
        // stale uuid lands on a shard that refuses to answer for a deleted incarnation (see
        // WriteFences#INCARNATION_FENCE_MILLIS) or on none at all.
        return once(described, index, () -> plane.describeForRouting(index));
    }

    Optional<String> ownerOnce(String index, int shard) throws IOException {
        // The owner as this node last saw it, before the register: a wrong hint is refused by the node it
        // names and corrected by one read then, see freshOwner.
        return once(owners, index + "#" + shard, () -> {
            final Optional<String> hinted = node.sightings().ownerHint(index, shard);
            if (hinted.isPresent()) {
                return hinted;
            }
            return freshOwner(index, shard);
        });
    }

    /** Reads the head and notes it for the node. Called from inside the cache's compute, so it does not touch the cache. */
    Optional<String> freshOwner(String index, int shard) throws IOException {
        final var head = plane.heads().read(index, shard);
        node.sightings().noteHead(index, shard, head.orElse(null));
        return head.map(h -> h.ownerNodeId());
    }

    /**
     * What to do after a forward failed, whatever the failure was.
     *
     * <p><b>The hint is forgotten first, unconditionally.</b> It used to survive everything except the one
     * refusal whose message said "does not own", so a hint naming a node that had died, or that held the
     * shard only as a reader, or that was shedding load, was consulted again on every request and every
     * request failed the same way -- indefinitely, on that coordinator, while the same write through
     * {@code _bulk} succeeded because the batch path reads the register every time.
     *
     * <p>Then the failure is sorted by {@link org.opensearch.serverless.transport.ForwardFailure}. A refusal or an unreachable owner means the
     * request was never applied: the register is read once and, if it names somebody new, the caller
     * forwards there. A deadline or a dropped connection means it may have been applied, so it is not sent
     * again; the caller is told to read before retrying. An answer from the owner -- a lost condition, a
     * 429, a mapper's 400 -- is the owner's answer and is thrown as itself.
     *
     * @return the placement to forward to next
     * @throws IOException when there is nothing to retry, worded for the caller
     */
    Placement afterForwardFailure(String index, Placement stale, Exception failure, boolean mayRetry) throws IOException {
        node.sightings().forgetOwner(index, stale.shard());
        owners.remove(index + "#" + stale.shard());
        final org.opensearch.serverless.transport.ForwardFailure kind = org.opensearch.serverless.transport.ForwardFailure.classify(
            failure
        );
        switch (kind) {
            case CONFLICT:
                // A lost compare-and-swap came back from a healthy owner that answered correctly. It is
                // not stale routing, must not cast doubt on ownership, and must not be retried blindly --
                // so it is rethrown as itself rather than dressed as a routing failure.
                throw (org.opensearch.index.engine.VersionConflictEngineException) org.opensearch.ExceptionsHelper.unwrap(
                    failure,
                    org.opensearch.index.engine.VersionConflictEngineException.class
                );
            case REJECTED:
            case REMOTE:
                // The owner answered. A client-side status -- its 429, a mapper's 400 -- is the owner's
                // answer and reaches the caller as it is, with the status the owner gave it. A server-side
                // one means the operation did not land there, and the caller retries against a re-read head.
                if (org.opensearch.serverless.transport.ForwardFailure.isClientStatus(failure)) {
                    if (failure instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    throw new IOException(failure);
                }
                throw new NotHereException(
                    org.opensearch.serverless.transport.ForwardFailure.describe(stale.owner(), failure),
                    stale.owner(),
                    true,
                    failure
                );
            case DEADLINE:
            case INTERRUPTED:
                // May have been applied: not sent again, and worded so the caller reads before it retries.
                node.signals().ownershipDoubted(index, stale.shard());
                throw new NotHereException(
                    org.opensearch.serverless.transport.ForwardFailure.describe(stale.owner(), failure),
                    stale.owner(),
                    true,
                    failure
                );
            case NOT_OWNER:
            case UNREACHABLE:
            default:
                break;
        }
        // The owner never applied it. One register read, and one more forward to whoever it names now.
        final Optional<String> fresh = mayRetry ? freshOwner(index, stale.shard()) : Optional.empty();
        if (mayRetry) {
            owners.put(index + "#" + stale.shard(), fresh);
            if (fresh.isPresent() && fresh.get().equals(stale.owner()) == false && fresh.get().equals(node.localNode().getId()) == false) {
                return new Placement(null, stale.shard(), fresh.get(), stale.uuid());
            }
        }
        node.signals().ownershipDoubted(index, stale.shard());
        if (mayRetry && fresh.isEmpty()) {
            // Whoever the hint named is gone and nobody has taken over: the same state, and the same
            // answer, as a register that never named anyone.
            throw new NotHereException(
                "no node currently owns shard " + stale.shard() + " of " + index + "; activate it before writing",
                null,
                false,
                failure
            );
        }
        if (mayRetry && fresh.get().equals(node.localNode().getId())) {
            throw new NotHereException(
                "this node is acquiring shard " + stale.shard() + " of " + index + "; retry",
                fresh.get(),
                true,
                failure
            );
        }
        // An owner with no live lease keeps its own words, which name the missing lease; anything else is
        // worded as the forward that failed.
        throw new NotHereException(
            failure instanceof org.opensearch.serverless.transport.OwnerUnreachableException
                ? failure.getMessage()
                : org.opensearch.serverless.transport.ForwardFailure.describe(stale.owner(), failure),
            stale.owner(),
            true,
            failure
        );
    }

    static <T> T once(
        java.util.concurrent.ConcurrentHashMap<String, T> cache,
        String key,
        org.opensearch.common.CheckedSupplier<T, IOException> read
    ) throws IOException {
        try {
            return cache.computeIfAbsent(key, ignored -> {
                try {
                    return read.get();
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        } catch (java.io.UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * Explains why this node cannot serve a placement, in the terms every caller needs.
     *
     * <p>Split out because the three "not here" cases are genuinely different and collapsing them is how
     * a brief, self-healing state gets reported as a permanent failure. A head naming <em>this</em> node
     * with no open shard is the activation window; a head naming somebody else is routing; no head at all
     * means nothing will change until a node activates the shard, and the arrival of this request is the
     * only reason one would.
     */
    NotHereException notHere(String index, Placement placement) {
        if (placement.owner() == null) {
            node.signals().ownershipDoubted(index, placement.shard());
            return new NotHereException("no node currently owns shard " + placement.shard() + " of " + index, null, false);
        }
        if (placement.owner().equals(node.localNode().getId())) {
            // Brief only if an activation is actually running. A head naming this node with nothing open and
            // nothing opening is a wedge -- every peer forwards here and gets this answer -- so the doubt goes
            // up either way: it joins an activation in flight, or starts the one that re-takes and replays.
            node.signals().ownershipDoubted(index, placement.shard());
            return new NotHereException(
                "this node is acquiring shard " + placement.shard() + " of " + index + "; retry",
                placement.owner(),
                true
            );
        }
        return new NotHereException(
            "shard " + placement.shard() + " of " + index + " is owned by " + placement.owner(),
            placement.owner(),
            true
        );
    }

    Placement place(String index, String id) throws IOException {
        if (refuseSystemIndices && node.isSystemIndex(index)) {
            // Before the descriptor is read: a refusal that first confirmed the index exists would say so.
            throw new SystemIndexException(index);
        }
        final Optional<IndexDescriptor> descriptor = describeOnce(index);
        if (descriptor.isEmpty()) {
            throw new NoSuchIndexException(index);
        }
        final int shard = org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor.get(), id);
        // Exact identity keeps routing independent of the number of other shards held here and
        // cannot select a previous incarnation or a frozen view with the same logical name.
        final var writer = node.reconciler().writerShard(new ShardId(index, descriptor.get().uuid(), shard));
        final ShardId local = writer == null ? null : writer.shardId();
        final String owner = ownerOnce(index, shard).orElse(null);
        return new Placement(local, shard, owner, descriptor.get().uuid());
    }
}
