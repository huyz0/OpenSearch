/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.transport;

import org.opensearch.node.NodeClosedException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.SendRequestTransportException;

import java.io.IOException;

public class ForwardFailureTests extends OpenSearchTestCase {
    public void testShutdownOfAnOutstandingRequestIsNotSafeToRetry() {
        // TransportService.doStop wraps every pending response handler this way, including writes
        // that already reached their owner. The wrapper alone cannot prove nothing was sent.
        final var failure = new SendRequestTransportException(
            null,
            "write",
            new NodeClosedException((org.opensearch.cluster.node.DiscoveryNode) null)
        );
        assertEquals(ForwardFailure.INTERRUPTED, ForwardFailure.classify(failure));
        assertFalse(ForwardFailure.classify(failure).mayRetryElsewhere());
        assertTrue(ForwardFailure.describe("owner", failure).contains("may or may not have been applied"));
    }

    public void testAnUnspecifiedSendFailureIsAmbiguous() {
        final var failure = new SendRequestTransportException(null, "write", new IOException("send failed"));
        assertEquals(ForwardFailure.INTERRUPTED, ForwardFailure.classify(failure));
        assertFalse(ForwardFailure.classify(failure).mayRetryElsewhere());
    }

    public void testAFailedConnectionBeforeSendingRemainsRetryable() {
        final var failure = new SendRequestTransportException(null, "write", new ConnectTransportException(null, "not connected"));
        assertEquals(ForwardFailure.UNREACHABLE, ForwardFailure.classify(failure));
        assertTrue(ForwardFailure.classify(failure).mayRetryElsewhere());
    }
}
