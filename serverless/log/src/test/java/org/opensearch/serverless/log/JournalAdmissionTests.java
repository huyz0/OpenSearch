/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class JournalAdmissionTests extends OpenSearchTestCase {
    public void testCountAndByteBudgetsAndIdempotentRelease() throws Exception {
        final var limits = new JournalConfiguration.Limits(1, 100, 2, 6L * 1024, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        final var admission = new JournalAdmission(limits);
        final var first = admission.acquire(100);
        assertEquals(6L * 612, admission.pendingBytes());
        expectThrows(IOException.class, () -> admission.acquire(0));
        first.close();
        first.close();
        assertEquals(0, admission.pendingRequests());
        assertEquals(0, admission.pendingBytes());
        final var small = admission.acquire(0);
        final var second = admission.acquire(0);
        expectThrows(IOException.class, () -> admission.acquire(0));
        admission.close();
        small.close();
        expectThrows(IOException.class, () -> admission.acquire(0));
        second.close();
        assertEquals(0, admission.pendingBytes());
    }
}
