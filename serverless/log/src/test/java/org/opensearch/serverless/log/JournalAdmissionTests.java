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

    public void testRequestReservationsSurviveBorrowingReleaseAndClose() throws Exception {
        final var firstGroup = java.util.UUID.randomUUID();
        final var secondGroup = java.util.UUID.randomUUID();
        final var limits = new JournalConfiguration.Limits(2, 100, 4, 100_000, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        final var admission = new JournalAdmission(limits, java.util.Set.of(firstGroup, secondGroup));
        final var permits = new java.util.ArrayList<JournalAdmission.Permit>();
        try {
            for (int i = 0; i < 3; i++) {
                permits.add(admission.acquire(firstGroup, 0));
            }
            expectThrows(IOException.class, () -> admission.acquire(firstGroup, 0));
            final var reserved = admission.acquire(secondGroup, 100);
            assertEquals(4, admission.pendingRequests());
            reserved.close();
            reserved.close();
            expectThrows(IOException.class, () -> admission.acquire(firstGroup, 0));
            permits.getFirst().close();
            permits.add(admission.acquire(firstGroup, 0));
            permits.add(admission.acquire(secondGroup, 100));
            admission.close();
            expectThrows(IOException.class, () -> admission.acquire(secondGroup, 0));
        } finally {
            permits.forEach(JournalAdmission.Permit::close);
        }
        assertEquals(0, admission.pendingRequests());
        assertEquals(0, admission.pendingBytes());
    }

    public void testByteReservationAllowsMaximumPayloadWhileAnotherGroupBorrowsCapacity() throws Exception {
        final var firstGroup = java.util.UUID.randomUUID();
        final var secondGroup = java.util.UUID.randomUUID();
        final long charge = 6L * (1000 + 512);
        final var limits = new JournalConfiguration.Limits(2, 1000, 100, 3 * charge, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        final var admission = new JournalAdmission(limits, java.util.Set.of(firstGroup, secondGroup));
        final var permits = new java.util.ArrayList<JournalAdmission.Permit>();
        try {
            permits.add(admission.acquire(firstGroup, 1000));
            permits.add(admission.acquire(firstGroup, 1000));
            expectThrows(IOException.class, () -> admission.acquire(firstGroup, 0));
            permits.add(admission.acquire(secondGroup, 1000));
            assertEquals(3 * charge, admission.pendingBytes());
            permits.getFirst().close();
            permits.add(admission.acquire(secondGroup, 1000));
            permits.get(1).close();
            expectThrows(IOException.class, () -> admission.acquire(secondGroup, 0));
            permits.add(admission.acquire(firstGroup, 1000));
            assertEquals(3 * charge, admission.pendingBytes());
        } finally {
            permits.forEach(JournalAdmission.Permit::close);
        }
        assertEquals(0, admission.pendingRequests());
        assertEquals(0, admission.pendingBytes());
    }

    public void testUnserviceableReservationsAndInvalidRequestsAreRefused() throws Exception {
        final var group = java.util.UUID.randomUUID();
        final var groups = java.util.Set.of(group, java.util.UUID.randomUUID());
        final var countLimited = new JournalConfiguration.Limits(2, 100, 1, 100_000, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        final var byteLimited = new JournalConfiguration.Limits(2, 100, 10, 6L * 612, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        expectThrows(IllegalArgumentException.class, () -> new JournalAdmission(countLimited, groups));
        expectThrows(IllegalArgumentException.class, () -> new JournalAdmission(byteLimited, groups));
        final var admission = new JournalAdmission(countLimited, java.util.Set.of(group));
        expectThrows(IOException.class, () -> admission.acquire(java.util.UUID.randomUUID(), 0));
        expectThrows(IOException.class, () -> admission.acquire(group, -1));
        expectThrows(IOException.class, () -> admission.acquire(group, 101));
        assertEquals(0, admission.pendingRequests());
        assertEquals(0, admission.pendingBytes());
    }

}
