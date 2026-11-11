/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.core.index.shard.ShardId;

/** Which shard a document belongs to, and what this node can do about it. */
public final class Placement {

    private final ShardId local;
    private final int shard;
    private final String owner;
    private final String uuid;

    Placement(ShardId local, int shard, String owner, String uuid) {
        this.local = local;
        this.shard = shard;
        this.owner = owner;
        this.uuid = uuid;
    }

    /**
     * Returns the uuid of the index incarnation this placement was resolved against.
     *
     * <p>Travels with every forward, so the owner can refuse a request for an index that has since
     * been deleted and recreated under the same name.
     *
     * @return the uuid
     */
    public String uuid() {
        return uuid;
    }

    /**
     * Returns the shard if it is open here as a writer, or null.
     *
     * @return the local shard, or null
     */
    public ShardId local() {
        return local;
    }

    /**
     * Returns the shard number the document hashes to.
     *
     * @return the shard number
     */
    public int shard() {
        return shard;
    }

    /**
     * Returns the node the shard-head names, or null if nobody owns it.
     *
     * @return the owner, or null
     */
    public String owner() {
        return owner;
    }
}
