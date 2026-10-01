/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import java.io.IOException;

/**
 * A reader refused to serve: nobody is writing the shard, and its published commit is behind acknowledged writes
 * that are only in the write-ahead log.
 *
 * <p>Retryable. The refusal raised the doubt that makes a node take the shard, which replays the log and publishes;
 * a reader opened after that serves everything acknowledged.
 */
public final class ShardBehindLogException extends IOException {

    private final String index;
    private final int shard;

    /**
     * Creates the refusal.
     *
     * @param index the index
     * @param shard the shard number
     */
    public ShardBehindLogException(String index, int shard) {
        super(
            "shard "
                + shard
                + " of "
                + index
                + " has acknowledged writes its published commit does not hold, and nobody is writing it; "
                + "a node is being asked to take it and replay its log -- retry"
        );
        this.index = index;
        this.shard = shard;
    }

    /**
     * Returns the index.
     *
     * @return the index name
     */
    public String index() {
        return index;
    }

    /**
     * Returns the shard number.
     *
     * @return the shard
     */
    public int shard() {
        return shard;
    }
}
