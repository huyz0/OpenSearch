/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.UUID;

public class JournalConfigurationTests extends OpenSearchTestCase {
    public void testPlacementRejectsSharedFailureDomainsAndDuplicateAddresses() {
        final var first = new JournalConfiguration.Peer(UUID.randomUUID(), "localhost", 10001, "az-a");
        final var second = new JournalConfiguration.Peer(UUID.randomUUID(), "localhost", 10002, "az-b");
        final var sameZone = new JournalConfiguration.Peer(UUID.randomUUID(), "localhost", 10003, "az-a");
        final var sameAddress = new JournalConfiguration.Peer(UUID.randomUUID(), "localhost", 10001, "az-c");
        expectThrows(
            IllegalArgumentException.class,
            () -> new JournalConfiguration.Group(UUID.randomUUID(), List.of(first, second, sameZone))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new JournalConfiguration.Group(UUID.randomUUID(), List.of(first, second, sameAddress))
        );
    }

    public void testRatisQueueMinimumIsValidatedBeforeStartingThreads() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new JournalConfiguration.Limits(1, 1024, 32, 1024 * 1024, 65536, 2 * 1024 * 1024, 0, 10_000, 1)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new JournalConfiguration.Limits(1, 1024, 32, 1024, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new JournalConfiguration.Limits(1, 1024, 32, 1024 * 1024, 1024 * 1024, 2 * 1024 * 1024, 0, 0, 1)
        );
    }
}
