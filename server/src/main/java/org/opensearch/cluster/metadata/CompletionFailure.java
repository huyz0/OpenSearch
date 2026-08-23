/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.OpenSearchException;

import java.util.concurrent.CompletionException;

/**
 * Strips the wrapper a {@link java.util.concurrent.CompletionStage} adds to a failure, so a client sees the
 * cause rather than the plumbing.
 *
 * <p>One copy rather than the four this replaces. Every metadata service that defers an acknowledgement on a
 * {@link ClaimedIndexLifecycle} stage needs exactly this, and each had grown its own {@code unwrapCompletion}
 * -- three of them identical, the fourth written out inline. That is not merely repetition: the four
 * disagreed on what to do with a {@link Throwable} that is not an {@link Exception}, which is the one case
 * the method exists to handle, and the difference was invisible at every call site.
 */
final class CompletionFailure {

    private CompletionFailure() {}

    /**
     * @param failure whatever {@code whenComplete} handed over, wrapped or not
     * @return the cause as an {@link Exception}, wrapping anything that is not one so a listener that can
     *         only report exceptions still reports something rather than dropping the failure
     */
    static Exception unwrap(Throwable failure) {
        Throwable cause = failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
        return cause instanceof Exception e ? e : new OpenSearchException(cause);
    }
}
