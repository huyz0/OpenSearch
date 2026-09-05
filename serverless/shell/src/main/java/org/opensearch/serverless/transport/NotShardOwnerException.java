/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

/**
 * The node a request was forwarded to does not hold the shard as its writer.
 *
 * <p><b>Typed, and marked, because the sender acts on it.</b> This refusal is the one signal that tells a
 * coordinator its routing hint is stale: on seeing it the coordinator forgets the hint, reads the register
 * once and forwards again. It used to be recognised by searching every cause's message for the phrase
 * "does not own", which any other exception could contain — a security plugin refusing with "user does
 * not own index alpha" re-routed the write and reported a routing failure instead of the plugin's 403.
 *
 * <p>The class itself does not survive the wire — core serialises an {@code IllegalStateException} by
 * message — so the message carries {@link #MARKER}, a token nothing else produces, and
 * {@link #describes(Throwable)} checks for either the type or the token. The phrase "does not own" is
 * kept in the text for the humans and logs that already look for it.
 */
public final class NotShardOwnerException extends IllegalStateException {

    /** A token that appears in no other message, so the check on the sending side is exact. */
    public static final String MARKER = "[serverless:not_owner]";

    /**
     * Creates the refusal.
     *
     * @param index the index the request named
     * @param shard the shard it named
     * @param why what this node holds instead, or null when it holds nothing
     */
    public NotShardOwnerException(String index, int shard, String why) {
        super("this node does not own " + index + "[" + shard + "]" + (why == null || why.isEmpty() ? "" : ": " + why) + " " + MARKER);
    }

    /**
     * Reports whether a failure, anywhere in its cause chain, is this refusal.
     *
     * @param failure what a forward threw
     * @return true when the far side refused as not the owner
     */
    public static boolean describes(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof NotShardOwnerException) {
                return true;
            }
            if (cause.getMessage() != null && cause.getMessage().contains(MARKER)) {
                return true;
            }
            if (cause.getCause() == cause) {
                break;
            }
        }
        return false;
    }
}
