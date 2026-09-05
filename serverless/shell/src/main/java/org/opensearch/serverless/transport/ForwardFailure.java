/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

/**
 * What a failed forward means, sorted by the one question every caller has to answer: did the owner
 * apply the request, and is it safe to send it somewhere else?
 *
 * <p>Every forwarding path — the single-document write, the batch, the get, the update, the explain — used
 * to answer that question with its own chain of {@code unwrap} calls and its own substring checks, and
 * they disagreed. One of them rendered an owner's 429 as "stale routing, retry"; another treated an
 * action filter's refusal as a lost shard. This is the one table they all read now.
 */
public enum ForwardFailure {

    /** The far side does not hold the shard as its writer. Nothing was applied; the hint was stale. */
    NOT_OWNER,

    /** The owner could not be reached at all: no live lease, or the connection could not be made. Nothing was sent. */
    UNREACHABLE,

    /** The request went out and no answer came back within the deadline. It may or may not have been applied. */
    DEADLINE,

    /** The connection dropped while the request was in flight. It may or may not have been applied. */
    INTERRUPTED,

    /** The owner answered: a compare-and-swap the caller asked for did not hold. */
    CONFLICT,

    /** The owner answered: it is shedding load, and says so with core's own 429. */
    REJECTED,

    /** The owner answered with some other failure of its own. Its answer stands; ownership is not in doubt. */
    REMOTE;

    /**
     * Sorts a forward's failure.
     *
     * @param failure what the forward threw, wrappers and all
     * @return the kind
     */
    public static ForwardFailure classify(Throwable failure) {
        if (NotShardOwnerException.describes(failure)) {
            return NOT_OWNER;
        }
        if (org.opensearch.ExceptionsHelper.unwrap(failure, org.opensearch.index.engine.VersionConflictEngineException.class) != null) {
            return CONFLICT;
        }
        if (org.opensearch.ExceptionsHelper.unwrap(
            failure,
            org.opensearch.core.concurrency.OpenSearchRejectedExecutionException.class
        ) != null) {
            return REJECTED;
        }
        // The future's own deadline and the transport's are the same event, and ShardRouter reports the
        // first in the second's terms; both are listed so a caller that bypassed the router still agrees.
        if (org.opensearch.ExceptionsHelper.unwrap(
            failure,
            org.opensearch.transport.ReceiveTimeoutTransportException.class,
            org.opensearch.OpenSearchTimeoutException.class
        ) != null) {
            return DEADLINE;
        }
        // Before ConnectTransportException, which it extends: a node that disconnects with a request in
        // flight is not a node that could not be reached, it is one whose answer was lost.
        if (org.opensearch.ExceptionsHelper.unwrap(failure, org.opensearch.transport.NodeDisconnectedException.class) != null) {
            return INTERRUPTED;
        }
        if (org.opensearch.ExceptionsHelper.unwrap(
            failure,
            OwnerUnreachableException.class,
            org.opensearch.transport.ConnectTransportException.class,
            org.opensearch.transport.SendRequestTransportException.class,
            java.net.ConnectException.class
        ) != null) {
            return UNREACHABLE;
        }
        return REMOTE;
    }

    /**
     * Whether this failure is reason to doubt that the register's owner is really serving.
     *
     * <p>An owner that answered, however unhappily, is serving. Only a refusal, a silence or an absence
     * should cost an activation pass.
     *
     * @return true when the coordinator should signal doubt
     */
    public boolean castsDoubtOnOwnership() {
        return this == NOT_OWNER || this == UNREACHABLE || this == DEADLINE || this == INTERRUPTED;
    }

    /**
     * Whether the request is known not to have been applied, so it may be sent to another node.
     *
     * <p>A deadline or a dropped connection is not: the owner may have applied an auto-id write and been
     * slow to say so, and forwarding it again writes the document twice.
     *
     * @return true when a second forward cannot duplicate the request
     */
    public boolean mayRetryElsewhere() {
        return this == NOT_OWNER || this == UNREACHABLE;
    }

    /**
     * The owner's own answer, unwrapped from the transport, for the failures where it gave one.
     *
     * @param failure what the forward threw
     * @return the owner's exception
     */
    public static Throwable answer(Throwable failure) {
        return org.opensearch.ExceptionsHelper.unwrapCause(failure);
    }

    /**
     * Whether the owner's answer carries a status a client should see as it is — a 4xx it can act on
     * rather than a server-side failure that reads as "retry".
     *
     * @param failure what the forward threw
     * @return true for a client-side status
     */
    public static boolean isClientStatus(Throwable failure) {
        return org.opensearch.ExceptionsHelper.status(answer(failure)).getStatus() < 500;
    }

    /**
     * Words a failed forward for the caller, telling a deadline apart from a refusal.
     *
     * <p>A forward that timed out is not "stale routing, retry": the owner may have applied the write and
     * been slow to answer, and a client that retries an auto-id write on that message writes the document
     * twice. The deadline case says so, so the client can read before it writes again. The same for a
     * connection that dropped with the request in flight, which used to be worded as a routing failure.
     *
     * @param owner the node the request was forwarded to
     * @param failure what went wrong
     * @return the message
     */
    public static String describe(String owner, Throwable failure) {
        switch (classify(failure)) {
            case DEADLINE:
                return "the owner "
                    + owner
                    + " did not answer within the deadline; the operation may or may not have been applied there, so read before retrying a write";
            case INTERRUPTED:
                return "the connection to the owner "
                    + owner
                    + " dropped while the request was in flight; the operation may or may not have been applied there, so read before retrying a write";
            default:
                return "could not forward to " + owner + ", which the shard-head named as owner: " + failure.getMessage();
        }
    }
}
