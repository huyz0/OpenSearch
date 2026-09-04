/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.auth;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Consecutive failed logins, counted per account and per remote address, and the wait each run earns.
 *
 * <p><b>Why it exists.</b> The derivation is slow by design and the checker pool is a budget on how much
 * of that slowness a node does at once. Neither stops a guess: an attacker with an account name and a
 * password list gets one answer per derivation, for as long as they care to wait. Refusing at the door
 * after a run of failures makes each further guess cost a wait rather than a derivation -- for the
 * attacker, who cannot make the clock run faster, and for this node, which does no work at all to say
 * "not yet".
 *
 * <p><b>Two keys, because either alone has a hole.</b> Keyed only by account, a flood against a thousand
 * names from one address is never noticed; keyed only by address, a flood against one name from a
 * thousand addresses is a thousand fresh counters. So a failure counts against both, and an attempt is
 * refused if either has earned a wait. What that costs is written down rather than hidden: somebody who
 * knows an account name can keep that account waiting by feeding it wrong passwords. That is a lockout
 * and not a break-in, and the cap on the wait bounds how long it lasts.
 *
 * <p><b>Exponential, capped, and reset by a success.</b> Up to the threshold, failures are free. At the
 * threshold the wait is one second, and each further failure doubles it up to the cap. A refused attempt
 * does not count, because nothing was checked. A success clears both counters, since the wait exists to
 * slow guessing and a correct password is not a guess.
 *
 * <p><b>Bounded.</b> The table is an LRU of fixed size, so a flood of distinct names or addresses evicts
 * old counters instead of growing the heap. An attacker can reset the counters of others by spreading
 * out, at the price of spreading out, and that is a better bargain than an unbounded map on a path an
 * unauthenticated caller drives.
 */
final class LoginThrottle {

    /** How many counters are kept before the least recently touched is dropped. */
    static final int CAPACITY = 10_000;

    /** The wait earned at the threshold, before any doubling. */
    static final long BASE_DELAY_MILLIS = 1_000L;

    private static final class Counter {
        int failures;
        long refusedUntil;
    }

    private final LongSupplier clock;
    private final int threshold;
    private final long maxDelayMillis;
    private final Map<String, Counter> table;

    /**
     * Creates the throttle.
     *
     * @param threshold the consecutive failures that earn the first wait
     * @param maxDelayMillis the longest wait, in milliseconds; zero switches the throttle off
     * @param clock the millisecond clock
     */
    LoginThrottle(int threshold, long maxDelayMillis, LongSupplier clock) {
        this.threshold = threshold;
        this.maxDelayMillis = maxDelayMillis;
        this.clock = clock;
        this.table = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Counter> eldest) {
                return size() > CAPACITY;
            }
        };
    }

    /**
     * Reports how long a caller must wait before an attempt will be checked.
     *
     * @param user the username offered
     * @param address the remote address, or null when the channel has none
     * @return whole seconds to wait, or 0 when the attempt may be checked now
     */
    long retryAfterSeconds(String user, String address) {
        if (maxDelayMillis == 0) {
            return 0;
        }
        final long now = clock.getAsLong();
        long until = now;
        synchronized (table) {
            for (String key : keys(user, address)) {
                final Counter entry = table.get(key);
                if (entry != null && entry.refusedUntil > until) {
                    until = entry.refusedUntil;
                }
            }
        }
        if (until <= now) {
            return 0;
        }
        // Rounded up: a Retry-After of 0 would invite an immediate retry that is refused again.
        return (until - now + 999) / 1_000;
    }

    /**
     * Counts a failure against both keys.
     *
     * @param user the username offered
     * @param address the remote address, or null
     */
    void failed(String user, String address) {
        if (maxDelayMillis == 0) {
            return;
        }
        final long now = clock.getAsLong();
        synchronized (table) {
            for (String key : keys(user, address)) {
                final Counter entry = table.computeIfAbsent(key, ignored -> new Counter());
                entry.failures++;
                if (entry.failures >= threshold) {
                    // Shift capped well below 63 so the doubling cannot wrap negative before the cap
                    // applies; by then the cap has applied many times over anyway.
                    final int doublings = Math.min(entry.failures - threshold, 30);
                    entry.refusedUntil = now + Math.min(maxDelayMillis, BASE_DELAY_MILLIS << doublings);
                }
            }
        }
    }

    /**
     * Clears both counters.
     *
     * @param user the username that authenticated
     * @param address the remote address, or null
     */
    void succeeded(String user, String address) {
        if (maxDelayMillis == 0) {
            return;
        }
        synchronized (table) {
            for (String key : keys(user, address)) {
                table.remove(key);
            }
        }
    }

    private static String[] keys(String user, String address) {
        // Different first characters, so a username can never spell an address key or the reverse.
        return address == null ? new String[] { "u:" + user } : new String[] { "u:" + user, "a:" + address };
    }
}
