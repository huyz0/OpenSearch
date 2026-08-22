/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;

/**
 * Thrown when whether an index exists could not be determined, as opposed to determined to be no.
 *
 * <p><b>Why this type exists.</b> A gated index has no cluster state entry, so the descriptor store is the
 * only thing that can answer whether its name is taken. That store swallowed every failure and returned
 * null, and null already meant "no such index". So a node that could not reach the descriptor store
 * reported every gated index in the cluster as non-existent, with nothing in the response to say otherwise.
 * A client acting on that answer could reasonably create an index that already exists.
 *
 * <p>That is this area's recurring failure aimed at availability rather than at latency: a computed index
 * returning a confident empty answer where an error would have been kinder. It is worth a distinct type
 * rather than a boolean or a null because the two answers have opposite safe responses. "Absent" invites
 * the caller to create; "unknown" must not.
 *
 * <p><b>What this deliberately does not change.</b> {@code AbsentIndexDescriptorSuppliers} treats a
 * supplier that throws as having no answer, which was settled on the grounds that resolution is already
 * a degradation path and turning a plugin bug into a request failure makes the absence worse. That
 * reasoning holds for a bug and fails for unreachable data, so this one type propagates and everything
 * else is still swallowed.
 *
 * <p>Reported as {@link RestStatus#SERVICE_UNAVAILABLE} rather than as a not-found, because retrying is
 * the correct client response and creating is not.
 */
public class DescriptorUnavailableException extends OpenSearchStatusException {

    public DescriptorUnavailableException(String indexName, Throwable cause) {
        super("could not determine whether index [" + indexName + "] exists", RestStatus.SERVICE_UNAVAILABLE, cause);
    }

    /**
     * Read from a stream, which every {@link org.opensearch.OpenSearchException} subclass must support.
     *
     * <p>Missing when this type was first introduced, which broke
     * {@code ExceptionSerializationTests.testExceptionRegistration} and went unnoticed because that run only
     * exercised the descriptor tests. The whole point of the type is that a caller can tell "unknown" from
     * "absent", and an unregistered exception loses its identity crossing the wire, so the distinction
     * survived on one node and was erased between two.
     */
    public DescriptorUnavailableException(org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
        super(in);
    }
}
