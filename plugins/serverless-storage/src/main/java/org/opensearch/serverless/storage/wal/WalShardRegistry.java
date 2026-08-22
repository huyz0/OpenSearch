/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * A durable, CAS-backed registry of every shard known to have mirrored at least one operation
 * into a shared WAL container (rfc-serverless-opensearch.md &sect;6.4) -- the piece a future
 * retention sweep needs to know *whose* latest published {@code WalPosition} bounds what is still
 * safe to delete, since the WAL container itself carries no per-shard structure of its own (chunk
 * blobs are keyed purely by a globally-continuous sequence number, not by shard -- see {@link
 * WalChunkService}'s own class javadoc).
 *
 * <p><b>Deliberately grow-only</b>: this registry never removes an entry on its own. A sweep that
 * treats "known to use this WAL" as monotonically growing is safe by construction -- the same
 * shape {@code formal/CloneGc.tla}'s {@code Fixed} variant already proves sound for a different
 * mechanism (pin before read, never remove early) -- whereas *removing* an entry safely requires
 * knowing a shard will genuinely never publish through this container again, which is real,
 * separate design work (see this class's own removal method for what's actually safe to remove
 * today: an explicitly deleted index, not staleness or inactivity).
 *
 * <p><b>One marker blob per shard, not one shared set.</b> Registration writes {@code
 * registered-shard-&lt;shardId&gt;.&lt;encoded index uuid&gt;} and nothing else; the retention sweep lists that
 * prefix. The original design kept the whole set in a single CAS register, which meant every registration
 * -- once per writer-engine activation, so once per shard per reactivation storm -- read the entire set,
 * deserialized it, added one element and CAS-wrote the whole thing back. At a hundred thousand registered
 * shards that is roughly five megabytes down and five megabytes up <em>per activation</em>, and every
 * concurrently-activating shard in the cluster contended the same register: the CAS loop gives up after
 * fifty attempts and fails hard, so a large enough storm turned an O(1) fact into O(n^2) bytes moved and a
 * cluster-wide serialization point that could fail outright. A marker blob is an idempotent PUT of zero
 * bytes with nothing to contend.
 *
 * <p><b>The old register is still read, and that is not vestigial.</b> {@code WalGcSchedulerTask} deletes
 * WAL chunks that no registered shard still needs, so an entry this class forgets is a chunk deleted out
 * from under a live shard. A repository written by an earlier build has its whole registry inside that one
 * register, so {@link #registeredShards()} answers with the union of both representations and
 * {@link #deregister} removes from both. Registration only ever writes the new representation, so the
 * register drains as indices are deleted and never grows again.
 */
public final class WalShardRegistry {

    private static final int MAX_CAS_ATTEMPTS = 50;
    private static final String REGISTER_NAME = "registered-shards";

    /**
     * Prefix for the per-shard marker blobs. Deliberately not a prefix of {@link #REGISTER_NAME} (it ends
     * in {@code -} where that ends in {@code s}), so listing the markers never returns the legacy register
     * blob itself.
     */
    private static final String MARKER_PREFIX = "registered-shard-";

    /**
     * Separates the shard id from the encoded index uuid in a marker blob name. A dot rather than the
     * doubled underscore used elsewhere in this codebase because the uuid is encoded with URL-safe base64,
     * whose alphabet contains {@code _} -- so {@code __} would not be an unambiguous separator, while
     * {@code .} cannot occur in either half.
     */
    private static final char MARKER_SEPARATOR = '.';

    private final BlobContainer blobContainer;

    /**
     * Wraps a shared WAL container.
     *
     * @param blobContainer the shared WAL container this registry tracks shards against.
     */
    public WalShardRegistry(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /**
     * Every shard ever registered -- a full listing, expected to be read rarely (a retention sweep), not on
     * any indexing hot path.
     *
     * <p>The union of the per-shard marker blobs and whatever a pre-marker build left in the legacy
     * register; see this class's own javadoc for why both are consulted rather than only the new one.
     */
    public Set<RegisteredShard> registeredShards() throws IOException {
        Set<RegisteredShard> shards = blobContainer.readRegister(REGISTER_NAME).map(this::deserialize).orElseGet(HashSet::new);
        for (String blobName : blobContainer.listBlobsByPrefix(MARKER_PREFIX).keySet()) {
            shards.add(parseMarker(blobName));
        }
        return shards;
    }

    /**
     * Idempotently records that {@code indexUuid}/{@code shardId} has mirrored into this
     * container. Safe, and expected, to call more than once for the same shard (e.g. once per
     * writer engine activation, not once ever) -- a no-op once already registered.
     *
     * <p>One zero-byte PUT, with no read and no compare-and-swap: the blob's <em>name</em> carries the
     * whole fact, so writing it twice is indistinguishable from writing it once and two shards registering
     * at the same moment contend nothing. See this class's own javadoc for the shared-register version this
     * replaced and why it did not scale.
     *
     * @param indexUuid the shard's owning index UUID.
     * @param shardId the shard number within {@code indexUuid}.
     */
    public void register(String indexUuid, int shardId) throws IOException {
        blobContainer.writeBlob(markerName(indexUuid, shardId), new ByteArrayInputStream(new byte[0]), 0L, false);
    }

    /**
     * Removes exactly one shard from the registry -- safe to call only when the caller has
     * independent proof this shard will never publish through this container again (today, that
     * proof is "its index was just deleted": see {@code ServerlessStoragePlugin}'s clone-pin
     * deletion listener for the established pattern this follows). A no-op if the shard was never
     * registered, or already removed.
     *
     * @param indexUuid the shard's owning index UUID.
     * @param shardId the shard number within {@code indexUuid}.
     */
    public void deregister(String indexUuid, int shardId) throws IOException {
        blobContainer.deleteBlobsIgnoringIfNotExists(List.of(markerName(indexUuid, shardId)));
        RegisteredShard shard = new RegisteredShard(indexUuid, shardId);
        // The legacy register too, so a shard registered by a pre-marker build genuinely goes away rather
        // than pinning WAL chunks forever. Costs one read on a path that runs once per index deletion, and
        // the mutation is a no-op (returning before any CAS) once the register is empty or absent.
        mutate(current -> {
            if (current.contains(shard) == false) {
                return current;
            }
            Set<RegisteredShard> next = new HashSet<>(current);
            next.remove(shard);
            return next;
        });
    }

    /** The marker blob name for one shard. Reversible by {@link #parseMarker}. */
    private static String markerName(String indexUuid, int shardId) {
        String encodedUuid = Base64.getUrlEncoder().withoutPadding().encodeToString(indexUuid.getBytes(StandardCharsets.UTF_8));
        return MARKER_PREFIX + shardId + MARKER_SEPARATOR + encodedUuid;
    }

    /**
     * The shard a marker blob name identifies.
     *
     * <p><b>Throws rather than skipping an unparseable name</b>, which is the conservative direction here
     * and the opposite of what a "be lenient about junk" instinct would do. This listing is what {@code
     * WalGcSchedulerTask} subtracts from to decide which WAL chunks nothing needs any more, so an entry
     * quietly dropped from it is a chunk deleted out from under a live shard. Failing the listing stops
     * that sweep for the tick, which is exactly what that task already does whenever its information is
     * incomplete (see its own javadoc). A name under this prefix that this class did not write is a real
     * anomaly, not an expected condition.
     */
    private static RegisteredShard parseMarker(String blobName) {
        String remainder = blobName.substring(MARKER_PREFIX.length());
        int separator = remainder.indexOf(MARKER_SEPARATOR);
        if (separator <= 0 || separator == remainder.length() - 1) {
            throw new IllegalStateException("unrecognised WAL shard registry marker blob [" + blobName + "]");
        }
        try {
            int shardId = Integer.parseInt(remainder.substring(0, separator));
            String indexUuid = new String(Base64.getUrlDecoder().decode(remainder.substring(separator + 1)), StandardCharsets.UTF_8);
            return new RegisteredShard(indexUuid, shardId);
        } catch (IllegalArgumentException e) {
            // Covers both NumberFormatException from the shard id and a malformed base64 uuid.
            throw new IllegalStateException("unrecognised WAL shard registry marker blob [" + blobName + "]", e);
        }
    }

    private void mutate(UnaryOperator<Set<RegisteredShard>> mutation) throws IOException {
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            long currentGeneration;
            Set<RegisteredShard> current;
            var existing = blobContainer.readRegister(REGISTER_NAME);
            if (existing.isPresent()) {
                currentGeneration = existing.get().generation();
                current = deserialize(existing.get());
            } else {
                currentGeneration = BlobRegister.ABSENT_GENERATION;
                current = new HashSet<>();
            }

            Set<RegisteredShard> next = mutation.apply(current);
            if (next.equals(current)) {
                return; // no-op mutation
            }

            BlobRegisterCasResult result = blobContainer.compareAndSwapRegister(REGISTER_NAME, currentGeneration, serialize(next));
            if (result.applied()) {
                return;
            }
            // conflict -- another registration raced us; re-read and retry.
        }
        throw new IOException("failed to update WAL shard registry after " + MAX_CAS_ATTEMPTS + " CAS attempts");
    }

    private Set<RegisteredShard> deserialize(BlobRegister register) {
        try {
            StreamInput in = register.value().streamInput();
            return new HashSet<>(in.readList(RegisteredShard::new));
        } catch (IOException e) {
            throw new IllegalStateException("failed to deserialize WAL shard registry", e);
        }
    }

    private static BytesReference serialize(Set<RegisteredShard> shards) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeCollection(shards, (o, shard) -> shard.writeTo(o));
        return out.bytes();
    }
}
