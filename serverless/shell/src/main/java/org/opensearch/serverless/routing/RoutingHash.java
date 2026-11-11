/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.routing;

import org.opensearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;

/**
 * The 64-bit point in the hash space a routing value maps to.
 *
 * <p>Murmur3, as core's routing uses, but the 128-bit variant's first half, so that the hash space is
 * wide enough to be cut anywhere: an adaptive index halves a unit's range for as long as it keeps
 * growing, and 32 bits would run out of room to halve long before the data did. The value is read as
 * <b>unsigned</b>; every comparison against it goes through {@link Long#compareUnsigned}.
 *
 * <p>This function is part of the stored format. A document's unit is a function of it, so changing it
 * would leave every existing document in a unit its routing no longer points at.
 */
public final class RoutingHash {

    private RoutingHash() {}

    /**
     * Returns the hash of a routing value: the document's routing when it has one, its id otherwise.
     *
     * @param routing the routing value; must not be null
     * @return the point in the unsigned 64-bit hash space
     */
    public static long of(String routing) {
        final byte[] bytes = routing.getBytes(StandardCharsets.UTF_8);
        return MurmurHash3.hash128(bytes, 0, bytes.length, 0L, new MurmurHash3.Hash128()).h1;
    }

    /**
     * Returns the declared shard a hash falls in: the {@code i} with {@code i * 2^64 / N <= hash < (i + 1) * 2^64 / N}.
     *
     * @param hash an unsigned hash
     * @param declaredShards the index's declared shard count
     * @return the declared shard, in {@code [0, declaredShards)}
     */
    public static int declaredShard(long hash, int declaredShards) {
        return (int) Math.unsignedMultiplyHigh(hash, declaredShards);
    }

    /**
     * Returns the first hash of a declared shard: {@code ceil(shard * 2^64 / N)}, the smallest hash for
     * which {@link #declaredShard} answers {@code shard}.
     *
     * @param shard the declared shard, in {@code [0, declaredShards]}; {@code declaredShards} itself
     *     names the end of the space and has no first hash
     * @param declaredShards the index's declared shard count
     * @return the unsigned first hash
     */
    public static long firstHashOf(int shard, int declaredShards) {
        if (shard < 0 || shard >= declaredShards) {
            throw new IllegalArgumentException("declared shard " + shard + " is outside [0, " + declaredShards + ")");
        }
        if (shard == 0) {
            return 0L;
        }
        // ceil(shard * 2^64 / n) = floor((shard * 2^64 - 1) / n) + 1, computed in 128 bits.
        final java.math.BigInteger numerator = java.math.BigInteger.valueOf(shard).shiftLeft(64).subtract(java.math.BigInteger.ONE);
        return numerator.divide(java.math.BigInteger.valueOf(declaredShards)).add(java.math.BigInteger.ONE).longValue();
    }

    /**
     * Returns the last hash of a declared shard, inclusive.
     *
     * @param shard the declared shard, in {@code [0, declaredShards)}
     * @param declaredShards the index's declared shard count
     * @return the unsigned last hash
     */
    public static long lastHashOf(int shard, int declaredShards) {
        return shard == declaredShards - 1 ? -1L : firstHashOf(shard + 1, declaredShards) - 1L;
    }
}
