/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.serverless.cluster.IndexDescriptor;

/**
 * Keeps {@link ServerlessNode}'s copies of index descriptors current: refreshes the open indices' descriptors and
 * applies a newer one to the shards here.
 */
final class DescriptorRefresh {

    private final ServerlessNode node;

    DescriptorRefresh(ServerlessNode node) {
        this.node = node;
    }

    /**
     * Applies mapping and settings changes made elsewhere to the shards this node holds open.
     *
     * <p>A mapping or settings update reached only the node that served it; every other node's open
     * shard kept the old one until it was reopened. One descriptor read per open index per heartbeat --
     * the heartbeat already reads a head per shard -- and a refresh only when a version moved.
     */
    java.util.Map<String, java.util.Optional<IndexDescriptor>> refreshDescriptorsOfOpenIndices(
        org.opensearch.serverless.metadata.MetadataPlane plane
    ) {
        final java.util.Set<String> indices = new java.util.LinkedHashSet<>();
        for (org.opensearch.core.index.shard.ShardId shardId : node.reconciler.openShards()) {
            indices.add(shardId.getIndexName());
        }
        // What was read, for the caller: present, or definitely absent. An index whose read failed is
        // left out, so a transient error is never mistaken for a deletion.
        final java.util.Map<String, java.util.Optional<IndexDescriptor>> read = new java.util.LinkedHashMap<>();
        final long intervalNanos = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(node.descriptorRefreshIntervalMillis);
        final long nowNanos = System.nanoTime();
        for (String indexName : indices) {
            final Long lastRead = node.descriptorReadAtNanos.get(indexName);
            if (intervalNanos > 0L && lastRead != null && nowNanos - lastRead < intervalNanos) {
                // Read within the interval -- by an earlier pass, or by the incarnation fence answering a
                // request -- and applied then. Left out of the result, which the caller reads as "not
                // known" rather than "deleted": a deletion is refused at the fence meanwhile.
                continue;
            }
            try {
                final long startedAt = System.nanoTime();
                final var descriptor = plane.describe(indexName);
                read.put(indexName, descriptor);
                node.descriptorReadAtNanos.put(indexName, startedAt);
                if (descriptor.isEmpty()) {
                    continue;
                }
                applyDescriptor(indexName, descriptor.get());
            } catch (Exception e) {
                ServerlessNode.logger.warn("could not refresh the descriptor of " + indexName + " on this node's open shards", e);
            }
        }
        node.descriptorReadAtNanos.keySet().retainAll(indices);
        return read;
    }

    /**
     * Brings this node's open shards of an index up to a descriptor just read.
     *
     * <p>From the per-pass refresh and from the incarnation fence, which reads the same register for an
     * index being used and so keeps its mapping and settings current at no further cost.
     */
    void applyDescriptor(String indexName, IndexDescriptor descriptor) throws java.io.IOException {
        // The copies the next projection is built from follow what was read, or a reader opened after a
        // mapping update would be projected with the mapping from before it.
        node.hosted.computeIfPresent(indexName, (k, v) -> descriptor);
        node.served.computeIfPresent(indexName, (k, v) -> descriptor);
        final long[] applied = node.appliedDescriptorVersions.computeIfAbsent(indexName, k -> new long[] { -1L, -1L });
        synchronized (applied) {
            if (applied[0] != descriptor.mappingVersion()) {
                node.reconciler.refresh().refreshMapping(indexName, descriptor);
                applied[0] = descriptor.mappingVersion();
            }
            if (applied[1] != descriptor.settingsVersion()) {
                node.reconciler.refresh().refreshSettings(indexName, descriptor);
                applied[1] = descriptor.settingsVersion();
            }
        }
    }

    /**
     * Applies a descriptor some other read found -- one routing a request, say -- if it is newer than what this node's
     * shards carry. Newer, not merely different: such a read can be older than the one the fence last applied.
     */
    void applyIfNewer(String indexName, IndexDescriptor descriptor, long readStartedNanos) {
        final long[] applied = node.appliedDescriptorVersions.get(indexName);
        if (applied != null) {
            synchronized (applied) {
                if (descriptor.mappingVersion() <= applied[0] && descriptor.settingsVersion() <= applied[1]) {
                    return;
                }
            }
        }
        try {
            applyDescriptor(indexName, descriptor);
            node.descriptorReadAtNanos.merge(indexName, readStartedNanos, Math::max);
        } catch (Exception e) {
            ServerlessNode.logger.warn("could not apply the descriptor of " + indexName + " read while routing", e);
        }
    }
}
