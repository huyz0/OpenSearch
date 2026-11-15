/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.Acquisition;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.store.NodeLogBlob;
import org.opensearch.serverless.store.NodeLogEpoch;
import org.opensearch.serverless.store.NodeLogStore;
import org.opensearch.serverless.store.NodeLogTakeover;
import org.opensearch.serverless.store.WalRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Plants records in the node log of a writer that took a shard and died, so the next owner replays exactly them.
 *
 * <p>For a test of what replay does with records no live writer would produce: two operations under one sequence number,
 * a hole, a record the mapping refuses. The planted writer takes the shard's head, names one epoch of its log in its lease
 * and lands one blob there holding the records; then its lease runs out. A node taking the shard afterwards finds the
 * writer named in the head, fences its log and replays the blob.
 */
final class PlantedNodeLog {

    static final String WRITER = "planted-writer";

    private PlantedNodeLog() {}

    /**
     * Has the planted writer take the shard and log records at its term, then lets its lease run out.
     *
     * @param plane the plane
     * @param index the index
     * @param shard the shard number
     * @param clock the plane's clock, moved past the writer's lease
     * @param ttl the plane's lease time to live
     * @param records the records as they are to be replayed; their sequence identity is kept as given
     * @return the term the planted writer held the shard at
     * @throws IOException if the store cannot be written
     */
    static long plant(MetadataPlane plane, IndexDescriptor index, int shard, AtomicLong clock, long ttl, List<WalRecord> records)
        throws IOException {
        return plant(plane, index, shard, clock, ttl, (Iterable<List<WalRecord>>) List.of(records));
    }

    /**
     * Like {@link #plant(MetadataPlane, IndexDescriptor, int, AtomicLong, long, List)}, landing one blob per batch, so a
     * history larger than the heap can be planted a batch at a time.
     *
     * @param plane the plane
     * @param index the index
     * @param shard the shard number
     * @param clock the plane's clock, moved past the writer's lease
     * @param ttl the plane's lease time to live
     * @param batches the records, one blob's worth at a time
     * @return the term the planted writer held the shard at
     * @throws IOException if the store cannot be written
     */
    static long plant(MetadataPlane plane, IndexDescriptor index, int shard, AtomicLong clock, long ttl, Iterable<List<WalRecord>> batches)
        throws IOException {
        final Acquisition taken = plane.activate(index.name(), shard, WRITER, WRITER + "-eph", index.uuid());
        if (taken.acquired() == false) {
            throw new IllegalStateException("the planted writer could not take " + index.name() + "[" + shard + "]: " + taken.head());
        }
        final long term = taken.head().term();
        plane.membership()
            .renew(
                new NodeLease(WRITER, WRITER + "-eph", "127.0.0.1:1", Set.of("ingest"), 0L).withNodeLog(
                    List.of(new NodeLogEpoch(0L, 0L, 0L))
                )
            );
        final NodeLogStore log = new NodeLogStore(plane.blobStore(), plane.basePath(), WRITER);
        Map<NodeLogBlob.ShardKey, List<NodeLogBlob.Range>> runs = Map.of();
        long ordinal = 0;
        for (List<WalRecord> batch : batches) {
            final NodeLogBlob.Encoded blob = NodeLogBlob.encode(
                WRITER,
                0L,
                ordinal,
                0L,
                List.of(new NodeLogBlob.ShardBatch(key(index, shard), term, batch)),
                runs
            );
            log.append(0L, ordinal, blob.bytes());
            runs = blob.header().index();
            ordinal++;
        }
        clock.addAndGet(2 * ttl);
        return term;
    }

    /**
     * Reads back what the planted writer's log holds for a shard, without fencing it.
     *
     * @param plane the plane
     * @param index the index
     * @param shard the shard number
     * @return the records, oldest first
     * @throws IOException if the log cannot be read
     */
    static List<WalRecord> records(MetadataPlane plane, IndexDescriptor index, int shard) throws IOException {
        final NodeLogTakeover.LeaseReader lease = () -> plane.membership().read(WRITER).map(NodeLease::nodeLog).orElse(List.of());
        final NodeLogStore log = new NodeLogStore(plane.blobStore(), plane.basePath(), WRITER);
        return NodeLogTakeover.unfenced(log, lease.epochs(), lease).records(key(index, shard));
    }

    private static NodeLogBlob.ShardKey key(IndexDescriptor index, int shard) {
        return new NodeLogBlob.ShardKey(index.uuid(), shard, index.name());
    }
}
