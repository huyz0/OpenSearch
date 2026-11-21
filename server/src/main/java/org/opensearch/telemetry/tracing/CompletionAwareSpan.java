/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.InternalApi;

/** A span whose lifetime can also end independently of the transport exchange carrying it. */
@InternalApi
public interface CompletionAwareSpan extends Span {
    /** Runs the listener once when the span ends, or immediately if it has already ended. */
    void addCompletionListener(Runnable listener);
}
