/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Which manifest shard an index's entry lives in: {@code murmurhash3(index UUID) mod shardCount}.
 *
 * <p>The index UUID, not the name, is the partition key -- an index cannot be renamed, so the two are
 * equally stable in practice, but the UUID is fixed-width and uniformly distributed where names are
 * neither, and it is already what {@code IndexId} keys on elsewhere in this codebase. A pure function
 * of the UUID and the declared shard count, nothing else: no cluster state, no ordering, so the same
 * index always lands in the same shard for a given shard count regardless of when or in what order the
 * manifest is built, which is what makes "carry forward every shard but the ones that changed" sound at
 * all.
 *
 * <h2>Alignment with the coordinator/descriptor-cache affinity hash</h2>
 *
 * The plugin's {@code CoordinatorAffinityRouting} already rendezvous-
 * hashes indices onto coordinator nodes for a different reason (spreading resolver-cache warmth). If
 * the manifest's own partition function matched that one, a coordinator's warm
 * set and its manifest-shard read set would coincide -- a free win from two independent designs lining
 * up. This class deliberately does not attempt that coupling: {@code CoordinatorAffinityRouting}
 * partitions over the current, size-varying node set (rendezvous hashing is defined precisely so adding
 * or removing a node reshuffles as little as possible), while this partitions over a shard count fixed
 * in the manifest and never derived from cluster size (see this class's own {@code shardCount} contract;
 * an earlier design that derived it from cluster size caused codec flapping). Those are two different
 * hash domains; forcing them to coincide is a real design decision that needs to be
 * built deliberately rather than discovered, not something this function can default its way into
 * without deciding, on its own authority, what a fixed manifest shard count should even mean relative to
 * a live node count. What this class does instead is the safe half: reuse the same underlying primitive
 * ({@link MurmurHash3}, the one hash implementation already vendored in this codebase) rather than
 * inventing a second one, so a future alignment decision has a shared foundation to build on rather than
 * two incompatible hashes to reconcile.
 */
public final class ManifestShardFunction {

    private ManifestShardFunction() {}

    /**
     * @param indexUUID the index's stable UUID (see this class's own javadoc for why the UUID and not
     *                  the name).
     * @param shardCount the manifest's declared shard count; must be positive. This is always the value
     *                   found in the manifest doing the partitioning, not a locally configured one:
     *                   readers use the value they find
     *                   there, not the value they are configured with.
     * @return which manifest shard, in {@code [0, shardCount)}, {@code indexUUID}'s entry belongs in.
     */
    public static int shardFor(String indexUUID, int shardCount) {
        Objects.requireNonNull(indexUUID, "indexUUID must not be null");
        if (shardCount <= 0) {
            throw new IllegalArgumentException("manifest shard count must be positive, got [" + shardCount + "]");
        }
        byte[] bytes = indexUUID.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0L, new MurmurHash3.Hash128());
        // floorMod, not %: h1 is a signed long and can be negative, and a negative shard id is not a
        // partition, it is a bug -- Math.floorMod is the direct way to say "wrap into [0, shardCount)"
        // rather than relying on the sign of shardCount (always positive here) to make % behave.
        return Math.floorMod(hash.h1, shardCount);
    }
}
