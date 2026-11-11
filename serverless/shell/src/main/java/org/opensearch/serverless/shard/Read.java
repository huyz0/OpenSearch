/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.shell.Document;

/** A document, and which copy answered for it. */
public final class Read {

    private final Document document;
    private final String servedBy;
    private final boolean realtime;
    private final ShardId shardId;

    Read(Document document, String servedBy, boolean realtime, ShardId shardId) {
        this.document = document;
        this.servedBy = servedBy;
        this.realtime = realtime;
        this.shardId = shardId;
    }

    /**
     * Returns what was found, which may be nothing.
     *
     * @return the document
     */
    public Document document() {
        return document;
    }

    /**
     * Returns the node that actually read it.
     *
     * @return the node id
     */
    public String servedBy() {
        return servedBy;
    }

    /**
     * Returns the resolved shard, or null when a filter supplied the response without reading.
     *
     * @return the resolved shard
     */
    public ShardId shardId() {
        return shardId;
    }

    /**
     * Reports whether the answer includes writes that are acknowledged but not yet published.
     *
     * @return true when the owner answered
     */
    public boolean realtime() {
        return realtime;
    }
}
