/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.serverless.cluster.IndexDescriptor;

/**
 * Which shard a document id belongs to.
 *
 * <p>This mirrors {@code OperationRouting.calculateScaledShardId}, which is private, and it must keep
 * mirroring it: a serverless node that hashed documents differently from the data plane would put a
 * document in one shard and look for it in another, and the symptom would be a search that returns
 * nothing rather than an error.
 *
 * <p><b>Split indices are not supported here.</b> The real function consults
 * {@code getSplitShardsMetadata()} after the modulo; this does not, because nothing in this shell
 * splits an index. If splitting ever lands, this is one of the places that has to learn about it, and
 * failing to would be silent.
 */
public final class DocumentRouting {

    private DocumentRouting() {}

    /**
     * Returns the shard a document id routes to.
     *
     * @param descriptor the index
     * @param id the document id, which is also the routing value
     * @return the shard number
     */
    public static int shardFor(IndexDescriptor descriptor, String id) {
        return Math.floorMod(Murmur3HashFunction.hash(id), descriptor.numberOfShards());
    }
}
