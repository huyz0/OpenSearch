/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * What one index's work has cost the object store on this node since the node started: requests by kind, and bytes.
 *
 * <p><b>Requests are kept in thousandths.</b> One upload can carry many indices -- a node-log blob holds a slice of
 * every shard written in its window -- and is charged to them by their share of its bytes. Whole requests would round a
 * small writer's share to nothing, and the per-index sums would stop adding up to what the store counted.
 *
 * <p>Twelve longs or so a live index, in one array: the counters are only ever added to and read, never reset, so a
 * reader takes deltas between two reads of its own.
 */
public final class IndexUsage {

    /**
     * What is counted. The request kinds match {@link MeteredBlobStore}'s own counters, one for one; the last four count
     * work that is not a store request.
     */
    public enum Kind {
        /** A register read: a GET. */
        REGISTER_READ,
        /** A register compare-and-swap or create: a conditional PUT, after a GET for the version on S3. */
        REGISTER_WRITE,
        /** A blob read, ranged or whole, or an existence check: a GET or HEAD. */
        BLOB_READ,
        /** A blob write: a PUT. */
        BLOB_WRITE,
        /** A listing, priced as a PUT. */
        LISTING,
        /** A delete: free on S3, counted anyway. */
        DELETE,
        /** Bytes pulled through reads. */
        BYTES_READ,
        /** Bytes sent by writes. */
        BYTES_WRITTEN,
        /** Documents written or deleted on this node, counted at the writer. */
        DOCS,
        /** Searches coordinated on this node. */
        QUERIES,
        /** Time searches coordinated on this node took, in nanoseconds. */
        SEARCH_NANOS,
        /** Bytes of written records kept for pushing to search nodes, once per record whatever the readers. */
        PUSH_BYTES;

        /**
         * Whether this kind counts requests, in thousandths, rather than bytes.
         *
         * @return true for a request kind
         */
        public boolean isRequest() {
            return ordinal() <= DELETE.ordinal();
        }
    }

    /** One request, in the thousandths requests are kept in. */
    public static final long ONE_REQUEST = 1_000L;

    private static final Kind[] KINDS = Kind.values();

    private final AtomicLongArray counts = new AtomicLongArray(KINDS.length);

    /** Creates empty counters. */
    public IndexUsage() {}

    void add(Kind kind, long amount) {
        if (amount != 0L) {
            counts.addAndGet(kind.ordinal(), amount);
        }
    }

    /**
     * Returns a counter: thousandths of a request for a request kind, bytes otherwise.
     *
     * @param kind the counter
     * @return its value
     */
    public long get(Kind kind) {
        return counts.get(kind.ordinal());
    }

    /**
     * Returns the S3 price of everything counted, in thousandths of a PUT.
     *
     * <p>A PUT and a listing cost one; a GET costs 0.08 of one ($0.0004 against $0.005 per thousand); a register write
     * is a GET for the version and a conditional PUT, so 1.08; a delete costs nothing.
     *
     * @return the cost, in thousandths of a PUT
     */
    public long costMilliUnits() {
        final long gets = get(Kind.REGISTER_READ) + get(Kind.BLOB_READ) + get(Kind.REGISTER_WRITE);
        final long puts = get(Kind.BLOB_WRITE) + get(Kind.LISTING) + get(Kind.REGISTER_WRITE);
        return puts + gets * 8L / 100L;
    }
}
