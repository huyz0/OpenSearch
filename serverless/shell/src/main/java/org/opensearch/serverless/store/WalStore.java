/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The write-ahead log that closes the durability window.
 *
 * <p>Before this, a shard's data reached the object store only when a commit was published, so
 * everything written since the last publish was lost on failover. Phase 6's zombie test measured
 * exactly that: the zombie's unpublished document was gone, and the notes recorded it as the WAL gap
 * rather than a fencing failure. This is that gap closed — a record is durable before the write is
 * acknowledged, so a successor can replay what the previous writer never published.
 *
 * <p><b>Term-scoped, like segments.</b> A writer at term T appends under {@code wal/t=T}, so a zombie
 * appending after it lost the shard writes where the successor is not reading, for the same reason and
 * by the same mechanism as §9.6's fencing of segment blobs.
 *
 * <p><b>Truncation is deliberately conservative.</b> Records are appended <em>before</em> being applied
 * to the engine, so a record present at the moment of a flush is not necessarily <em>in</em> that
 * flush. Deleting what the current publish snapshotted could therefore drop a write that had not landed
 * yet. Instead each publish deletes what the <em>previous</em> publish saw, which guarantees a full
 * cycle elapsed and the record was applied and committed. Replaying a few extra records costs nothing
 * because replay is idempotent — see {@link WalRecord}.
 *
 * <p><b>One blob per record</b>, which is the obvious thing to batch and is not batched. The design's
 * own principle is "batch writes, stream reads"; a group commit belongs here and is not built, so a
 * write costs an object-store PUT.
 */
public final class WalStore {

    private final BlobStore blobStore;
    private final BlobPath shardBase;
    /** A record's blob name: a zero-padded ordinal, and nothing else in the container is ours. */
    private static final java.util.regex.Pattern RECORD_NAME = java.util.regex.Pattern.compile("\\d{20}");

    private final AtomicLong ordinal = new AtomicLong();
    private volatile List<String> previousSnapshot = List.of();

    /**
     * Creates a WAL for one shard.
     *
     * @param blobStore the backing store
     * @param shardBase the shard's base path
     */
    public WalStore(BlobStore blobStore, BlobPath shardBase) {
        this.blobStore = blobStore;
        this.shardBase = shardBase;
    }

    private BlobContainer containerFor(long term) {
        return blobStore.blobContainer(shardBase.add("wal").add(SegmentPublisher.termSegment(term)));
    }

    /**
     * Appends a record, durably, before the caller applies the write.
     *
     * @param term the writing node's term
     * @param record the write
     * @throws IOException if the append fails
     */
    public void append(long term, WalRecord record) throws IOException {
        final byte[] bytes = org.opensearch.core.common.bytes.BytesReference.toBytes(record.toBytes());
        // Zero-padded so a lexicographic listing is a chronological one. Within a term there is exactly
        // one writer, so the ordinal needs no coordination.
        final String name = String.format(java.util.Locale.ROOT, "%020d", ordinal.incrementAndGet());
        containerFor(term).writeBlob(name, new ByteArrayInputStream(bytes), bytes.length, false);
    }

    /**
     * Reads every record still in the log, in the order it was written.
     *
     * <p>Ordered by term and then by ordinal, so a later write to the same document id wins on replay
     * exactly as it did originally.
     *
     * @return the records to replay
     * @throws IOException if listing or reading fails
     */
    public List<WalRecord> replayable() throws IOException {
        final BlobContainer walRoot = blobStore.blobContainer(shardBase.add("wal"));
        final Map<Long, BlobContainer> byTerm = new TreeMap<>();
        for (Map.Entry<String, BlobContainer> child : walRoot.children().entrySet()) {
            final Long term = parseTerm(child.getKey());
            if (term != null) {
                byTerm.put(term, child.getValue());
            }
        }

        final List<WalRecord> records = new ArrayList<>();
        for (BlobContainer container : byTerm.values()) {
            final List<String> names = new ArrayList<>(container.listBlobs().keySet());
            names.removeIf(name -> RECORD_NAME.matcher(name).matches() == false);
            names.sort(Comparator.naturalOrder());
            for (String name : names) {
                try (InputStream in = container.readBlob(name)) {
                    records.add(WalRecord.fromStream(in));
                } catch (IOException e) {
                    // A name that matches ours and does not parse is corruption, and it propagates --
                    // naming the blob, because a parse failure that does not say which record failed
                    // leaves an operator with a corrupt log and nowhere to look. Anything not matching
                    // was filtered above: a foreign object sharing the prefix is not ours to interpret,
                    // and must not brick a shard's recovery. Absent, foreign and corrupt are three
                    // different answers.
                    throw new IOException("unreadable WAL record at " + container.path().buildAsString() + name, e);
                }
            }
        }
        return records;
    }

    /**
     * Records that a commit has been published, and drops what the previous publish had seen.
     *
     * <p>Deleting the <em>previous</em> snapshot rather than the current one is the whole safety
     * argument; see this class's documentation.
     *
     * @param term the publishing writer's term
     * @return the number of records dropped
     * @throws IOException if listing or deleting fails
     */
    public int onPublished(long term) throws IOException {
        final BlobContainer container = containerFor(term);
        final List<String> current = new ArrayList<>(container.listBlobs().keySet());
        current.removeIf(name -> RECORD_NAME.matcher(name).matches() == false);

        final List<String> toDelete = new ArrayList<>(previousSnapshot);
        toDelete.retainAll(current);
        if (toDelete.isEmpty() == false) {
            container.deleteBlobsIgnoringIfNotExists(toDelete);
        }
        previousSnapshot = current;
        return toDelete.size();
    }

    /**
     * Removes every record for a shard, for use when the index itself is deleted.
     *
     * @throws IOException if listing or deleting fails
     */
    public void deleteAll() throws IOException {
        final BlobContainer walRoot = blobStore.blobContainer(shardBase.add("wal"));
        for (BlobContainer child : walRoot.children().values()) {
            final List<String> ours = new ArrayList<>(child.listBlobs().keySet());
            ours.removeIf(name -> RECORD_NAME.matcher(name).matches() == false);
            child.deleteBlobsIgnoringIfNotExists(ours);
        }
    }

    private static Long parseTerm(String containerName) {
        if (containerName.startsWith("t=") == false) {
            return null;
        }
        try {
            return Long.parseLong(containerName.substring(2));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
