/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

/**
 * What deletes still owe: one intent per deleted index, written before its descriptor is removed and
 * taken once it falls due.
 *
 * <p><b>Intent first.</b> A delete that removed the descriptor and then crashed would leave heads and bytes
 * nothing could find, because the descriptor was what named them. Written before, an intent survives any
 * crash after it; one whose delete then never happened is found, on reclaim, to name a live index and is
 * simply dropped.
 *
 * <p><b>Due after a delay, not at once.</b> The delete itself already removes what it can see. What it
 * cannot see is a writer that had not yet noticed: one that re-creates a head, or completes a publish it had
 * begun, after the delete. That writer's lease runs out within a TTL and it answers nothing past the
 * incarnation fence, so by {@link #DEFAULT_DELAY_MILLIS} it has stopped, and what it left is there to be
 * taken.
 *
 * <p>Immutable objects in minute buckets, one per intent: nothing is rewritten, and a reclaimer lists only
 * the buckets that are due. Consumed by whichever node rendezvous hashing assigns each bucket to, and safe to
 * consume twice: every step of a reclaim is conditional or scoped to a uuid no live index has.
 */
public final class ReclaimQueue {

    /**
     * How long after a delete its intent falls due: two minutes, far longer than a lease TTL plus the
     * incarnation fence, which is how long a writer that missed the delete can go on acting.
     */
    public static final long DEFAULT_DELAY_MILLIS = 120_000L;

    private static final DateTimeFormatter BUCKET = DateTimeFormatter.ofPattern("uuuuMMddHHmm", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final int PAGE = 1000;

    private final BlobStore blobStore;
    private final BlobPath path;
    private volatile long delayMillis = DEFAULT_DELAY_MILLIS;

    /** One deleted index and what it takes to finish deleting it. */
    public record Intent(String name, String uuid, int shards) {
    }

    /**
     * Creates a queue.
     *
     * @param blobStore the store
     * @param path where the queue lives; see {@link RegisterMap#reclaim}
     */
    public ReclaimQueue(BlobStore blobStore, BlobPath path) {
        this.blobStore = blobStore;
        this.path = path;
    }

    /**
     * Sets how long after a delete its intent falls due.
     *
     * @param millis the delay
     * @return this, for chaining
     */
    public ReclaimQueue setDelayMillis(long millis) {
        this.delayMillis = Math.max(0L, millis);
        return this;
    }

    /**
     * Records what a delete about to happen will owe.
     *
     * @param intent the index being deleted
     * @param nowMillis the plane's clock
     * @throws IOException if the intent cannot be written, in which case the delete must not proceed
     */
    public void enqueue(Intent intent, long nowMillis) throws IOException {
        final byte[] body = ("{\"name\":\"" + intent.name() + "\",\"uuid\":\"" + intent.uuid() + "\",\"shards\":" + intent.shards() + "}")
            .getBytes(StandardCharsets.UTF_8);
        final String bucket = BUCKET.format(Instant.ofEpochMilli(nowMillis + delayMillis));
        blobStore.blobContainer(path.add(bucket))
            .writeBlob(intent.name() + RegisterMap.SHARD_SEPARATOR + intent.uuid(), new ByteArrayInputStream(body), body.length, false);
    }

    /** Handles one due intent. */
    @FunctionalInterface
    public interface Reclaimer {
        /**
         * Finishes one delete.
         *
         * @param intent the intent
         * @throws IOException if it could not be finished; the intent is kept and tried again
         */
        void reclaim(Intent intent) throws IOException;
    }

    /**
     * Takes every due intent in the buckets this caller owns.
     *
     * @param nowMillis the plane's clock
     * @param ownsBucket whether this caller should drain a bucket
     * @param reclaimer what finishing a delete means
     * @return how many intents were taken
     * @throws IOException if the queue cannot be listed
     */
    public int drain(long nowMillis, Predicate<String> ownsBucket, Reclaimer reclaimer) throws IOException {
        int taken = 0;
        for (String bucket : new ArrayList<>(blobStore.blobContainer(path).children().keySet())) {
            final long start = bucketStart(bucket);
            // Due once the whole minute has passed: an intent is filed under the minute its delay ends in,
            // so only the end of that minute is certain to be at least the delay after its delete.
            if (start < 0L || start + 60_000L > nowMillis || ownsBucket.test(bucket) == false) {
                continue;
            }
            taken += drainBucket(blobStore.blobContainer(path.add(bucket)), reclaimer);
        }
        return taken;
    }

    private int drainBucket(BlobContainer entries, Reclaimer reclaimer) throws IOException {
        int taken = 0;
        boolean kept = false;
        // Paged, and relisted from the start: each page deletes what it finished, so the next listing
        // begins past it. A page that finished nothing is all kept, and listing again would return it.
        while (true) {
            final List<BlobMetadata> page = entries.listBlobsByPrefixInSortedOrder("", PAGE, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
            final List<String> finished = new ArrayList<>();
            for (BlobMetadata entry : page) {
                final Intent intent;
                try (InputStream in = entries.readBlob(entry.name())) {
                    intent = parse(in);
                } catch (IOException e) {
                    // Not an intent this queue wrote: nothing to do for it.
                    finished.add(entry.name());
                    continue;
                }
                try {
                    reclaimer.reclaim(intent);
                    finished.add(entry.name());
                } catch (IOException e) {
                    kept = true;
                }
            }
            if (finished.isEmpty() == false) {
                entries.deleteBlobsIgnoringIfNotExists(finished);
                taken += finished.size();
            }
            if (page.size() < PAGE || finished.isEmpty()) {
                break;
            }
        }
        if (kept == false && entries.listBlobs().isEmpty()) {
            entries.delete();
        }
        return taken;
    }

    private static Intent parse(InputStream in) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, in)
        ) {
            final Map<String, Object> map = parser.map();
            if (map.get("name") instanceof String name && map.get("uuid") instanceof String uuid && map.get("shards") instanceof Number n) {
                return new Intent(name, uuid, n.intValue());
            }
            throw new IOException("not a reclaim intent");
        }
    }

    /** When a bucket falls due, or -1 for a name that is not one. */
    static long bucketStart(String bucket) {
        if (bucket.length() != 12 || bucket.chars().allMatch(Character::isDigit) == false) {
            return -1L;
        }
        try {
            return LocalDateTime.of(
                Integer.parseInt(bucket.substring(0, 4)),
                Integer.parseInt(bucket.substring(4, 6)),
                Integer.parseInt(bucket.substring(6, 8)),
                Integer.parseInt(bucket.substring(8, 10)),
                Integer.parseInt(bucket.substring(10, 12))
            ).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (java.time.DateTimeException e) {
            return -1L;
        }
    }
}
