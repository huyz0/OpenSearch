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
 *
 * <p>Or, {@link #superseded()}: the copy refusing is open on a commit a later writer has replaced. The log cannot say
 * so -- the publish that replaced it trimmed what it covered -- so the published manifest does. Nothing is wrong
 * with the shard, and a reader opened now serves the current commit.
 */
public final class ShardBehindLogException extends IOException {

    private final String index;
    private final int shard;
    private final boolean superseded;

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
        this.superseded = false;
    }

    private ShardBehindLogException(String index, int shard, String message) {
        super(message);
        this.index = index;
        this.shard = shard;
        this.superseded = true;
    }

    /**
     * Creates the refusal of a copy whose commit a later publish has replaced.
     *
     * @param index the index
     * @param shard the shard number
     * @return the refusal
     */
    public static ShardBehindLogException superseded(String index, int shard) {
        return new ShardBehindLogException(
            index,
            shard,
            "this copy of shard " + shard + " of " + index + " serves a commit a later publish has replaced -- retry"
        );
    }

    /**
     * Whether the refusing copy was open on a replaced commit, rather than the shard's commit being behind its log.
     *
     * @return true if superseded
     */
    public boolean superseded() {
        return superseded;
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
