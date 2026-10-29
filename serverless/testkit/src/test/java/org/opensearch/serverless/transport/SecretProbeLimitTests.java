/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.transport;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SecretProbeLimitTests extends OpenSearchTestCase {
    public void testAnOverdueProbeDoesNotOverlapAnInFlightRead() {
        final AtomicLong nanos = new AtomicLong();
        final AtomicInteger reads = new AtomicInteger();
        final java.util.concurrent.atomic.AtomicReference<TransportAuthenticator> reference =
            new java.util.concurrent.atomic.AtomicReference<>();
        final var secrets = new MetadataPlane.TransportSecrets(1L, "ab".repeat(32), null, 0L);
        final var request = new GetRequest("index", "id");
        final Runnable slowRead = () -> {
            reads.incrementAndGet();
            nanos.set(2_000_000_000L);
            expectThrows(
                org.opensearch.core.concurrency.OpenSearchRejectedExecutionException.class,
                () -> reference.get().verify("action", request, "999:sender:1000:nonce:00")
            );
        };
        final var auth = new TransportAuthenticator(() -> secrets, slowRead, () -> "node", () -> 1_000L, () -> 30_000L, 16, nanos::get);
        reference.set(auth);
        expectThrows(OpenSearchSecurityException.class, () -> auth.verify("action", request, "999:sender:1000:nonce:00"));
        assertEquals("the nested request must not start a second store read", 1, reads.get());
    }

    public void testUnauthenticatedGenerationClaimsCannotReadTheStorePerRequest() {
        final AtomicLong nanos = new AtomicLong();
        final AtomicInteger reads = new AtomicInteger();
        final var secrets = new MetadataPlane.TransportSecrets(1L, "ab".repeat(32), null, 0L);
        final var auth = new TransportAuthenticator(
            () -> secrets,
            reads::incrementAndGet,
            () -> "node",
            () -> 1_000L,
            () -> 30_000L,
            16,
            nanos::get
        );
        final var request = new GetRequest("index", "id");
        for (int i = 0; i < 100; i++) {
            final String unverified = (100L + i) + ":sender:1000:nonce:00";
            if (i == 0) {
                expectThrows(OpenSearchSecurityException.class, () -> auth.verify("action", request, unverified));
            } else {
                expectThrows(
                    org.opensearch.core.concurrency.OpenSearchRejectedExecutionException.class,
                    () -> auth.verify("action", request, unverified)
                );
            }
        }
        assertEquals(1, reads.get());
        nanos.set(999_999_999L);
        expectThrows(
            org.opensearch.core.concurrency.OpenSearchRejectedExecutionException.class,
            () -> auth.verify("action", request, "999:sender:1000:nonce:00")
        );
        assertEquals(1, reads.get());
        nanos.incrementAndGet();
        expectThrows(OpenSearchSecurityException.class, () -> auth.verify("action", request, "999:sender:1000:nonce:00"));
        assertEquals("a later probe is allowed even though the wall clock is frozen", 2, reads.get());
    }
}
