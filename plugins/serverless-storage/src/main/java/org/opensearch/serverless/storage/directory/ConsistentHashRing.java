/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.directory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A classic consistent-hash ring over a fixed set of buckets {@code [0, bucketCount)}, each
 * represented by {@code virtualNodesPerBucket} points on the ring so load spreads roughly evenly
 * rather than depending on how luckily a single hash point lands. This is the routing primitive
 * {@link PartitionedShardDirectory} uses to assign a shard to one of several directory-node
 * instances the same way DynamoDB assigns a partition key to a storage node -- see
 * rfc-serverless-metadata-plane.md &sect;8/&sect;9/&sect;11's "tens of directory nodes,
 * DynamoDB-style" target design.
 *
 * <p>Not wired to anything dynamic yet: {@code bucketCount} is fixed at construction, there is no
 * support for adding/removing a bucket without rebuilding the whole ring (which would reshuffle
 * every key, defeating the point of consistent hashing over plain modulo). A real multi-node
 * deployment would need that -- this class only provides the deterministic, evenly-distributed
 * key-to-bucket mapping a resizable version would still need internally.
 */
final class ConsistentHashRing {

    private final NavigableMap<Long, Integer> ring = new TreeMap<>();

    ConsistentHashRing(int bucketCount, int virtualNodesPerBucket) {
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive, got " + bucketCount);
        }
        if (virtualNodesPerBucket <= 0) {
            throw new IllegalArgumentException("virtualNodesPerBucket must be positive, got " + virtualNodesPerBucket);
        }
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            for (int virtualNode = 0; virtualNode < virtualNodesPerBucket; virtualNode++) {
                ring.put(hash(bucket + "#" + virtualNode), bucket);
            }
        }
    }

    /** The bucket that owns {@code key}: the first ring point at or after {@code key}'s hash, wrapping around. */
    int bucketFor(String key) {
        long keyHash = hash(key);
        var entry = ring.ceilingEntry(keyHash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    private static long hash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            long result = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                result = (result << 8) | (bytes[i] & 0xFF);
            }
            return result;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 must always be available", e);
        }
    }
}
