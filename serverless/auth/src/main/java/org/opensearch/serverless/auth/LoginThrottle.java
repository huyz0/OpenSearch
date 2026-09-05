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
 * Failed logins, counted three ways, and what each run earns: a wait answered at the door, or a wait
 * served before the check.
 *
 * <p><b>Why it exists.</b> The derivation is slow by design and the checker pool is a budget on how much
 * of that slowness a node does at once. Neither stops a guess: an attacker with an account name and a
 * password list gets one answer per derivation, for as long as they care to wait. Making each further
 * guess cost a wait rather than a derivation prices the guessing in time -- which the attacker cannot
 * make run faster -- and for this node a wait costs nothing at all.
 *
 * <p><b>Three keys, and what each one may do.</b> The first version of this counted per account and per
 * address and refused if either had earned a wait. That is two lockouts, and both were wrong in the same
 * way: whoever could make a name or an address fail could make it refuse a <em>correct</em> password. An
 * address is shared -- a NAT, a load balancer, an ingress -- so one bad account behind it locked every
 * good one behind it; and an account name is public, so five wrong guesses at the configured account, sent
 * from anywhere, locked the one credential that exists for the day everything else is broken.
 *
 * <ul>
 *   <li><b>The pair {@code (address, account)}</b> is the only thing that earns a refusal. Consecutive
 *       failures from one address against one name earn an exponential wait during which that pair is
 *       answered 429 at the door with no work done. The only caller this refuses is one who has been
 *       failing that account from that address, which is what a guesser looks like; a different account
 *       behind the same address, and the same account from a different address, each have their own
 *       counter.</li>
 *   <li><b>The account</b>, across every address, earns a <em>delay</em> and never a refusal: an uncached
 *       check of that account is scheduled after the wait rather than run now. A guesser rotating
 *       addresses is slowed to one check per wait, and nobody can lock an account by knowing its name.</li>
 *   <li><b>The address</b> has a failure budget per window rather than a counter that doubles. A single
 *       bad account cannot reach it: once its pair is refused, its attempts are answered at the door and
 *       are not failures. Only a flood across many names gets there, and when it does, uncached attempts
 *       from that address are refused until the window turns. A caller this node has already verified is
 *       never asked: the request path consults the cache before it consults this.</li>
 * </ul>
 *
 * <p><b>The recovery account is never refused, only slowed.</b> {@link #decide} takes a flag for the
 * configured account, and for it every wait -- pair, account or address -- becomes a delay bounded by the
 * cap. A correct password always gets checked. An attacker who wants to keep the operator out has to keep
 * the node's bounded delay budget full continuously, rather than send one wrong password a minute.
 *
 * <p><b>Exponential, capped, reset by a success, decayed by quiet.</b> Up to the threshold, failures are
 * free. At the threshold the wait is one second, and each further failure doubles it up to the cap. A
 * refused attempt does not count, because nothing was checked. A success clears the pair and the account,
 * since the wait exists to slow guessing and a correct password is not a guess. And a failure that arrives
 * more than a cap after the last wait expired starts its count again, so a counter left at the cap does
 * not stay there for the life of the process.
 *
 * <p><b>Per node, and bounded.</b> Nothing shares this table between nodes: a fleet of N nodes gives a
 * guesser N times this budget, and a wait earned on one node says nothing about another. Sharing it would
 * cost a read of the account index per failure, on a path an unauthenticated caller drives, and that is
 * not obviously worth it. The table is an LRU of fixed size, so a flood of distinct names or addresses
 * evicts old counters instead of growing the heap. An attacker can reset the counters of others by
 * spreading out, at the price of spreading out, and that is a better bargain than an unbounded map.
 */
final class LoginThrottle {

    /** How many counters are kept before the least recently touched is dropped. */
    static final int CAPACITY = 10_000;

    /** The wait earned at the threshold, before any doubling. */
    static final long BASE_DELAY_MILLIS = 1_000L;

    /** How long an address's failure budget lasts before it is counted afresh. */
    static final long ADDRESS_WINDOW_MILLIS = 60_000L;

    /** What the throttle says about one attempt. */
    static final class Decision {

        /** The attempt may be checked now. */
        static final Decision NOW = new Decision(0L, false, null);

        private final long waitMillis;
        private final boolean refuse;
        private final String because;

        private Decision(long waitMillis, boolean refuse, String because) {
            this.waitMillis = waitMillis;
            this.refuse = refuse;
            this.because = because;
        }

        /** True if the attempt may be checked without waiting. */
        boolean immediate() {
            return waitMillis <= 0;
        }

        /** How long before the attempt may be checked; zero when it may be checked now. */
        long waitMillis() {
            return waitMillis;
        }

        /** True if the attempt is to be answered at the door rather than delayed and then checked. */
        boolean refuse() {
            return refuse;
        }

        /** The wait in whole seconds, rounded up: a Retry-After of 0 would invite an immediate retry. */
        long retryAfterSeconds() {
            return (waitMillis + 999) / 1_000;
        }

        /** Which key earned the wait, for the message. */
        String because() {
            return because;
        }
    }

    private static final class Counter {
        int failures;
        long refusedUntil;
        long windowStart;
    }

    private final LongSupplier clock;
    private final int threshold;
    private final int addressBudget;
    private final long maxDelayMillis;
    private final Map<String, Counter> table;

    /**
     * Creates the throttle.
     *
     * @param threshold the consecutive failures a pair or an account may make before the first wait
     * @param addressBudget the failures an address may make within {@link #ADDRESS_WINDOW_MILLIS}
     * @param maxDelayMillis the longest wait, in milliseconds; zero switches the throttle off
     * @param clock the millisecond clock
     */
    LoginThrottle(int threshold, int addressBudget, long maxDelayMillis, LongSupplier clock) {
        this.threshold = threshold;
        this.addressBudget = addressBudget;
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
     * Decides whether an uncached attempt may be checked now, must wait, or is refused.
     *
     * @param user the username offered
     * @param address the remote address, or null when the channel has none
     * @param recovery true for the configured account, which is delayed and never refused
     * @return the decision
     */
    Decision decide(String user, String address, boolean recovery) {
        if (maxDelayMillis == 0) {
            return Decision.NOW;
        }
        final long now = clock.getAsLong();
        long pairWait = 0L;
        long addressWait = 0L;
        final long accountWait;
        synchronized (table) {
            if (address != null) {
                pairWait = waitOf(table.get(pairKey(address, user)), now);
                final Counter budget = table.get(addressKey(address));
                if (budget != null && now - budget.windowStart < ADDRESS_WINDOW_MILLIS && budget.failures >= addressBudget) {
                    addressWait = budget.windowStart + ADDRESS_WINDOW_MILLIS - now;
                }
            }
            accountWait = waitOf(table.get(accountKey(user)), now);
        }
        final long longest = Math.max(pairWait, Math.max(accountWait, addressWait));
        if (longest <= 0) {
            return Decision.NOW;
        }
        final String because;
        if (addressWait >= pairWait && addressWait >= accountWait) {
            because = "from this address";
        } else if (pairWait >= accountWait) {
            because = "for this account from this address";
        } else {
            because = "for this account";
        }
        if (recovery) {
            // Never at the door. The delay is capped even when the address budget would have refused
            // for the rest of its window: the operator waits at most one cap, whoever else is failing.
            return new Decision(Math.min(longest, maxDelayMillis), false, because);
        }
        if (pairWait > 0 || addressWait > 0) {
            return new Decision(Math.max(pairWait, addressWait), true, because);
        }
        return new Decision(Math.min(accountWait, maxDelayMillis), false, because);
    }

    /**
     * Reports how long a caller must wait before an attempt will be checked, for a caller that cannot
     * schedule a delay and has to refuse instead.
     *
     * @param user the username offered
     * @param address the remote address, or null when there is none
     * @return whole seconds to wait, or 0 when the attempt may be checked now
     */
    long retryAfterSeconds(String user, String address) {
        return decide(user, address, false).retryAfterSeconds();
    }

    /**
     * Counts a failure against the pair, the account and the address's budget.
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
            if (address != null) {
                escalate(pairKey(address, user), now);
                spend(addressKey(address), now);
            }
            escalate(accountKey(user), now);
        }
    }

    /**
     * Clears the pair and the account. The address's budget is left alone: a success says nothing about
     * how many failures that address has produced, and clearing it would let a guesser reset the budget
     * with one account they do hold.
     *
     * @param user the username that authenticated
     * @param address the remote address, or null
     */
    void succeeded(String user, String address) {
        if (maxDelayMillis == 0) {
            return;
        }
        synchronized (table) {
            if (address != null) {
                table.remove(pairKey(address, user));
            }
            table.remove(accountKey(user));
        }
    }

    private void escalate(String key, long now) {
        final Counter entry = table.computeIfAbsent(key, ignored -> new Counter());
        if (entry.refusedUntil != 0L && now >= entry.refusedUntil + maxDelayMillis) {
            // Quiet for a whole cap after the last wait ended: the run is over, and this failure is the
            // start of a new one rather than the continuation of one from an hour ago.
            entry.failures = 0;
            entry.refusedUntil = 0L;
        }
        entry.failures++;
        if (entry.failures >= threshold) {
            // Shift capped well below 63 so the doubling cannot wrap negative before the cap applies;
            // by then the cap has applied many times over anyway.
            final int doublings = Math.min(entry.failures - threshold, 30);
            entry.refusedUntil = now + Math.min(maxDelayMillis, BASE_DELAY_MILLIS << doublings);
        }
    }

    private void spend(String key, long now) {
        final Counter entry = table.computeIfAbsent(key, ignored -> new Counter());
        if (now - entry.windowStart >= ADDRESS_WINDOW_MILLIS) {
            entry.windowStart = now;
            entry.failures = 0;
        }
        entry.failures++;
    }

    private static long waitOf(Counter entry, long now) {
        return entry == null || entry.refusedUntil <= now ? 0L : entry.refusedUntil - now;
    }

    // Different first characters, so no key of one kind can spell a key of another; the pair key puts
    // the address first and separates with a character neither a formatted address nor a username can
    // contain, so two different pairs cannot spell the same key either.
    private static String pairKey(String address, String user) {
        return "p:" + address + '\0' + user;
    }

    private static String accountKey(String user) {
        return "u:" + user;
    }

    private static String addressKey(String address) {
        return "a:" + address;
    }
}
