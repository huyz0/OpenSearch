/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.OpenSearchException;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * The plumbing every core caller of {@link ClaimedIndexLifecycle} needs to turn a stage into an
 * acknowledgement, in one place.
 *
 * <p>One copy rather than the five this replaces. Each metadata service that defers an acknowledgement on a
 * lifecycle stage had grown its own {@code unwrapCompletion} -- three of them identical, two written out
 * inline. That is not merely repetition: they disagreed on what to do with a {@link Throwable} that is not
 * an {@link Exception}, which is the one case the method exists to handle, and the difference was invisible
 * at every call site.
 */
final class ClaimedIndexWrites {

    private ClaimedIndexWrites() {}

    /**
     * Strips the wrapper a completion stage adds to a failure, so a client sees the cause rather than the
     * plumbing.
     *
     * @param failure whatever {@code whenComplete} handed over, wrapped or not
     * @return the cause as an {@link Exception}, wrapping anything that is not one so a listener that can
     *         only report exceptions still reports something rather than dropping the failure
     */
    static Exception unwrap(Throwable failure) {
        Throwable cause = failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
        return cause instanceof Exception e ? e : new OpenSearchException(cause);
    }

    /**
     * Reports a claimed-index write to a listener, without blocking on it.
     *
     * <p>Nothing waits: the listener is completed from whichever thread completes the write. That is the
     * same arrangement a deletion's acknowledgement uses, and it is the only way a caller on a thread that
     * must not block can still refuse to acknowledge a change that did not happen.
     *
     * <p><b>A {@code false} outcome is a failure here rather than a quiet success.</b> A plane answers false
     * only when the change could not be applied at all, and a request told "acknowledged" for that is the
     * silent-success failure this area has produced repeatedly.
     *
     * @param indexName named in the failure, since the listener has no other way to say which write it was
     */
    static void reportTo(CompletionStage<Boolean> write, String indexName, ActionListener<Void> listener) {
        write.whenComplete((applied, failure) -> {
            if (failure != null) {
                listener.onFailure(unwrap(failure));
            } else if (Boolean.TRUE.equals(applied) == false) {
                listener.onFailure(new IllegalStateException("the record for [" + indexName + "] could not be updated"));
            } else {
                listener.onResponse(null);
            }
        });
    }
}
