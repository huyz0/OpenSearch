/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.transport;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

public class TransportNonceCacheTests extends OpenSearchTestCase {
    public void testTimestampSubtractionCannotOverflowTheWindowCheck() {
        final var auth = new TransportAuthenticator(() -> null, () -> {}, () -> "node", () -> 0L, () -> 10L);
        final var request = new org.opensearch.action.get.GetRequest("index", "id");
        final var error = expectThrows(
            org.opensearch.OpenSearchSecurityException.class,
            () -> auth.verify("action", request, "0:sender:" + Long.MIN_VALUE + ":nonce:00")
        );
        assertTrue(error.getMessage().contains("outside the time window"));
    }

    public void testSaturationDoesNotAllowAReplay() {
        final AtomicLong now = new AtomicLong(100L);
        final var cache = new TransportAuthenticator.NonceCache(2, () -> 10L, now::get);
        assertTrue(cache.remember("first", 100L));
        assertTrue(cache.remember("second", 100L));
        assertFalse(cache.remember("third", 100L));
        assertFalse(cache.remember("first", 100L));
        now.set(110L);
        assertFalse("valid at the inclusive deadline", cache.remember("first", 100L));
        now.set(111L);
        assertTrue("expired entries free capacity", cache.remember("third", 111L));
    }

    public void testFutureTimestampRemainsProtectedForItsEntireWindow() {
        final AtomicLong now = new AtomicLong(100L);
        final var cache = new TransportAuthenticator.NonceCache(2, () -> 10L, now::get);
        assertTrue(cache.remember("future", 110L));
        assertTrue(cache.remember("earlier", 100L));
        now.set(111L);
        assertTrue(cache.remember("fresh", 111L));
        assertFalse(cache.remember("future", 110L));
        now.set(120L);
        assertFalse(cache.remember("future", 110L));
        now.set(121L);
        assertTrue(cache.remember("another", 121L));
    }
}
