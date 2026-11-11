/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import java.io.IOException;

/**
 * This node cannot serve the shard, and says what it knows about who can.
 *
 * <p>Carrying the owner is the point: a caller that is told only "wrong node" has to guess, and
 * guessing at ownership is how two writers come to believe the same thing.
 */
public final class NotHereException extends IOException {

    private final String owner;
    private final boolean retryable;
    private final boolean behindLog;

    NotHereException(String message, String owner, boolean retryable) {
        this(message, owner, retryable, null);
    }

    NotHereException(String message, String owner, boolean retryable, Throwable cause) {
        this(message, owner, retryable, cause, false);
    }

    private NotHereException(String message, String owner, boolean retryable, Throwable cause, boolean behindLog) {
        super(message, cause);
        this.owner = owner;
        this.retryable = retryable;
        this.behindLog = behindLog;
    }

    /**
     * A shard nobody is writing whose published commit is behind acknowledged writes, which nobody took over
     * in time to answer from. Retryable: the refusal asked for the takeover that makes it current.
     */
    static NotHereException behindLog(org.opensearch.serverless.shell.ShardBehindLogException cause) {
        return new NotHereException(cause.getMessage(), null, true, cause, true);
    }

    /**
     * Returns the node the shard-head names, if any.
     *
     * @return the owner, or null
     */
    public String owner() {
        return owner;
    }

    /**
     * Reports whether asking again shortly is likely to work.
     *
     * <p>True for a shard mid-activation or an owner that has gone quiet — states that resolve
     * themselves. False when no node owns the shard at all, which needs someone to activate it.
     *
     * @return true when the caller should retry
     */
    public boolean retryable() {
        return retryable;
    }

    /**
     * The HTTP status this refusal renders as, the same on every endpoint.
     *
     * <p>"Nobody owns the shard" used to be a 421 from {@code PUT} and {@code _bulk} and a 503
     * {@code owner_unreachable} from {@code _update}, {@code _get} and {@code _mget} -- a state the code
     * itself classifies as not retryable, reported to the client as "retry". The one place that knows
     * what was thrown decides what it is.
     *
     * @return 421 when no node owns the shard, 503 for every self-resolving state
     */
    public org.opensearch.core.rest.RestStatus restStatus() {
        if (behindLog) {
            return org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;
        }
        return owner == null
            ? org.opensearch.core.rest.RestStatus.MISDIRECTED_REQUEST
            : org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;
    }

    /**
     * The error type this refusal renders as, in the vocabulary the single-document endpoints use.
     *
     * @param localNodeId this node's id, to recognise the activation window
     * @return the type
     */
    public String restType(String localNodeId) {
        if (behindLog) {
            return "shard_behind_log";
        }
        if (owner == null) {
            return "not_the_writer";
        }
        if (owner.equals(localNodeId)) {
            return "activation_in_progress";
        }
        final String message = getMessage() == null ? "" : getMessage();
        return message.contains("could not forward") || message.contains("may or may not have been applied")
            ? "forward_failed"
            : "owner_unreachable";
    }
}
