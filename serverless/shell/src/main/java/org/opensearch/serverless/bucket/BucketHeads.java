/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import java.io.IOException;
import java.util.Optional;

/**
 * Which head decides a shard's ownership: its own, or its bucket's.
 *
 * <p>Every member of a bucket is owned through the one bucket head, so taking the second member of a bucket a node
 * already holds costs no write, and a takeover moves the whole bucket with one compare-and-swap. The shard head store
 * asks this before every head operation. It must answer the same on every node, which is why it reads the index's
 * descriptor, and why buckets are turned on for a whole fleet before the first one is made.
 */
public interface BucketHeads {

    /** A member shard: the bucket that owns it, and the member index's uuid. */
    record Member(String bucketId, String indexUuid) {
        /**
         * Creates a member reference.
         *
         * @param bucketId the bucket
         * @param indexUuid the member index's uuid
         */
        public Member {
            BucketIds.validate(bucketId);
        }
    }

    /**
     * Returns the bucket a shard belongs to.
     *
     * @param indexName the index
     * @param shardId the shard
     * @return its bucket, or empty for a shard owned through its own head
     * @throws IOException if the index's descriptor cannot be read
     */
    Optional<Member> bucketOf(String indexName, int shardId) throws IOException;

    /**
     * Returns the bucket a shard of one index incarnation belongs to.
     *
     * @param indexName the index
     * @param shardId the shard
     * @param indexUuid the incarnation, or null when the caller does not know it
     * @return its bucket, or empty for a shard owned through its own head or of another incarnation
     * @throws IOException if the index's descriptor cannot be read
     */
    default Optional<Member> bucketOf(String indexName, int shardId, String indexUuid) throws IOException {
        return bucketOf(indexName, shardId).filter(member -> indexUuid == null || indexUuid.equals(member.indexUuid()));
    }

    /**
     * Returns whether a node holds any other member of a bucket open, in which case giving one member back keeps the
     * bucket head: the head is released only with the bucket's last open member.
     *
     * @param bucketId the bucket
     * @param nodeId the node giving the member back
     * @param exceptIndexName the member being given back
     * @return true if another member is open on that node
     * @throws IOException if a member's descriptor cannot be read
     */
    boolean othersOpenOn(String bucketId, String nodeId, String exceptIndexName) throws IOException;
}
