/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.auth;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.transport.client.Client;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Who the accounts are, kept in an index, read through the plugin {@link Client}.
 *
 * <p><b>Credentials live in an index because that is the thing being proved.</b> The standing goal is a
 * shell that can host plugins which keep their state in a system index, and OpenSearch Security is
 * exactly such a plugin. The honest way to find out whether the host can carry one is to write one and
 * put it on the critical path of every request. Nothing here uses a shell class; it is all {@code Client}.
 *
 * <p><b>The configured account is checked before the index, and that ordering is the recovery path.</b>
 * The store is an index, an index is shards, and shards can be unowned, mid-activation, or on a node that
 * cannot be reached. If the only credentials were in there, then the failure that takes out the store also
 * takes away the ability to log in and fix it. One account, configured, checked first, and working while
 * the object store is unreachable, is what keeps a bad day from being a locked door.
 *
 * <p><b>Verified credentials are cached, and the cost is stated.</b> A derivation is a few hundred
 * milliseconds by design, and a lookup is a get that may cross the network; paying both per request would
 * put authentication an order of magnitude above the request it protects. The cache holds a fingerprint of
 * the credential, never the password and never the stored record, for a short TTL. The node that makes a
 * change drops its own entries immediately; every other node learns of it through the marker below, and
 * the TTL is the bound that still holds when the marker is bypassed.
 *
 * <p><b>A marker, so that a change made on one node reaches the others in a second rather than a TTL.</b>
 * There is no cluster state to carry "account X changed", and having every node re-read the account on
 * every request is the cost the cache exists to avoid. So the index carries one reserved document, moved
 * on every account change after the change itself is written, and a node consults it at most once a
 * second when it is about to trust its cache: one small read, and when the marker has moved the whole
 * cache is dropped and every cached credential is checked again on its next use. It is a random value
 * rather than a counter because the plugin {@link Client} reports sequence numbers as unassigned and does
 * not honour a compare-and-swap, so a read-increment-write from two nodes could lose an increment, and a
 * lost increment is a change nobody would ever see; two random values cannot collapse into one that way.
 * The read never runs on the thread that read the request: {@link #isCached} declines when the check is
 * due, which sends the request to the checker pool, and {@link #verify} pays for the read there.
 *
 * <p><b>Every failure is a denial.</b> An unreadable store, an unparseable record, a missing index: none
 * of them authenticate anybody. The distinction the caller does get is <em>why</em>, because "no such
 * account" and "cannot reach the accounts" are a 401 and a 503, and an operator needs to tell them apart.
 */
public final class CredentialStore {

    /** What checking a credential concluded. */
    public static final class Verdict {

        private final String principal;
        private final String unavailable;

        private Verdict(String principal, String unavailable) {
            this.principal = principal;
            this.unavailable = unavailable;
        }

        /**
         * Returns who the credential belongs to.
         *
         * @return the principal name, or null when it was not accepted
         */
        public String principal() {
            return principal;
        }

        /**
         * Reports why the accounts could not be consulted, if they could not be.
         *
         * @return the reason, or null when the store answered
         */
        public String unavailable() {
            return unavailable;
        }

        /**
         * Reports whether the credential was accepted.
         *
         * @return true if authenticated
         */
        public boolean authenticated() {
            return principal != null;
        }
    }

    private static final Verdict REJECTED = new Verdict(null, null);

    /**
     * The marker's document id, which no account may have.
     *
     * <p>The same name {@code StoredScriptStore} gives its register, because it is the same idea. It is
     * a legal username by the character class, so {@link #validateUsername} refuses it by name.
     */
    static final String MARKER = "_version";

    /** How long a node trusts its cache before it looks at the marker again. */
    static final long MARKER_INTERVAL_MILLIS = 1_000L;

    private static final SecureRandom RANDOM = new SecureRandom();

    private final Supplier<Client> client;
    private final LongSupplier clock;
    private final String index;
    private final String bootstrapUser;
    private final String bootstrapRecord;
    private final String decoy;
    private final int iterations;
    private final long cacheTtlMillis;
    private final TimeValue timeout;

    private final Map<String, Long> cache;

    /** When the marker was last consulted, so the check costs one read a second and not one a request. */
    private final AtomicLong markerCheckedAt;

    /** Set when {@link #isCached} claimed a check and sent the request on; {@link #verify} pays for it. */
    private final AtomicBoolean markerPending = new AtomicBoolean();

    /** The marker as this node last saw it; 0 before any read and when there is none. */
    private volatile long markerSeen;

    /**
     * Builds the store.
     *
     * @param settings the node settings
     * @param client supplies the client, which does not exist until the node has started
     * @param clock the millisecond clock, injectable so a test can age the cache without sleeping
     */
    public CredentialStore(Settings settings, Supplier<Client> client, LongSupplier clock) {
        this.client = client;
        this.clock = clock;
        this.index = ServerlessAuthPlugin.INDEX.get(settings);
        this.bootstrapUser = ServerlessAuthPlugin.BOOTSTRAP_USER.get(settings);
        this.iterations = ServerlessAuthPlugin.ITERATIONS.get(settings);
        this.cacheTtlMillis = ServerlessAuthPlugin.CACHE_TTL.get(settings).millis();
        this.timeout = ServerlessAuthPlugin.LOOKUP_TIMEOUT.get(settings);

        try (org.opensearch.core.common.settings.SecureString password = ServerlessAuthPlugin.BOOTSTRAP_PASSWORD.get(settings)) {
            if (password == null || password.length() == 0) {
                // Loud, at construction, and therefore at node start. The alternative is a node with
                // authentication installed and no credential that could ever create the first account,
                // which is not a guarded door but a door locked with the key thrown away.
                throw new IllegalArgumentException(
                    "["
                        + ServerlessAuthPlugin.BOOTSTRAP_PASSWORD.getKey()
                        + "] must be set in the keystore: the authentication plugin is installed, so without "
                        + "a configured account there is no credential that could ever create the first one"
                );
            }
            // Derived once, here, so that the only lasting copy of this credential is a salted hash. The
            // SecureString is closed on the way out of this block, which wipes the characters the keystore
            // handed over.
            this.bootstrapRecord = PasswordHash.encode(password.getChars(), iterations);
        }
        this.decoy = PasswordHash.decoy(iterations);
        // Due at once, so the first check after start reads the marker rather than trusting a value it
        // never saw.
        this.markerCheckedAt = new AtomicLong(clock.getAsLong() - MARKER_INTERVAL_MILLIS);

        final int capacity = ServerlessAuthPlugin.CACHE_SIZE.get(settings);
        this.cache = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > capacity;
            }
        };
    }

    /**
     * The names an account may have.
     *
     * <p>Narrow on purpose. A username is a document id, part of a cache key and something an operator
     * types; a character class that excludes whitespace, quotes and slashes removes a whole family of
     * questions about what happens when one of those appears in any of those places.
     */
    private static final java.util.regex.Pattern USERNAME = java.util.regex.Pattern.compile("[A-Za-z0-9_.@-]{1,64}");

    /**
     * Refuses a username this system will not store.
     *
     * @param user the proposed name
     * @throws IllegalArgumentException if the name is not allowed
     */
    public static void validateUsername(String user) {
        if (user == null || USERNAME.matcher(user).matches() == false) {
            throw new IllegalArgumentException(
                "a username must be 1-64 characters of letters, digits, underscore, dot, at-sign or hyphen, but was [" + user + "]"
            );
        }
        if (MARKER.equals(user)) {
            throw new IllegalArgumentException("[" + user + "] is reserved");
        }
    }

    /**
     * Returns the configured account's name.
     *
     * @return the bootstrap username
     */
    public String bootstrapUser() {
        return bootstrapUser;
    }

    /**
     * Returns the index the accounts are kept in.
     *
     * @return the index name
     */
    public String index() {
        return index;
    }

    /**
     * Reports whether this exact credential was checked recently enough to be trusted again without work.
     *
     * <p>Separate from {@link #verify} so that a caller can find out, without blocking and without a
     * derivation, whether checking would block. The request path uses it to stay on the thread that read
     * the request in the overwhelming majority of cases: a cache hit is a hash and a map lookup, while a
     * miss is hundreds of milliseconds of key derivation and possibly a read across the network, and those
     * two do not belong on the same thread.
     *
     * @param user the username offered
     * @param password the password offered
     * @return true if the credential is already known good
     */
    public boolean isCached(String user, char[] password) {
        if (user == null || user.isEmpty() || password == null || password.length == 0) {
            return false;
        }
        if (claimMarkerCheck()) {
            // The marker is due, and reading it is a client call that does not belong on this thread.
            // Declining sends this one request the slow way, where verify() reads the marker and then
            // finds the cache entry still there if the marker has not moved; every other request in the
            // same second stays on the fast path.
            markerPending.set(true);
            return false;
        }
        return cachedAndFresh(user + " " + PasswordHash.fingerprint(user, password));
    }

    /**
     * Checks a username and password.
     *
     * <p><b>May block, and may take hundreds of milliseconds.</b> Both are deliberate: the derivation is
     * slow by design and the account may have to be read from the object store. Call it from somewhere
     * that can afford both.
     *
     * @param user the username offered
     * @param password the password offered
     * @return what the check concluded
     */
    public Verdict verify(String user, char[] password) {
        if (user == null || user.isEmpty() || password == null || password.length == 0) {
            return REJECTED;
        }
        if (markerPending.compareAndSet(true, false) || claimMarkerCheck()) {
            refreshMarker();
        }
        final String key = user + " " + PasswordHash.fingerprint(user, password);
        if (cachedAndFresh(key)) {
            return new Verdict(user, null);
        }
        boolean derived = false;
        if (bootstrapUser.equals(user)) {
            derived = true;
            if (PasswordHash.verify(password, bootstrapRecord)) {
                remember(key);
                return new Verdict(user, null);
            }
        }
        // Falls through when the configured name is offered with a different password, rather than
        // refusing: the same name may also exist in the index, and short-circuiting here would make the
        // configured account impossible to override.
        String record;
        try {
            record = lookup(user);
        } catch (IndexNotFoundException e) {
            // No account has ever been created. That is "no such user", not "broken": a fresh deployment
            // is in this state until the configured account creates the first one.
            record = null;
        } catch (Exception e) {
            return new Verdict(null, "the account store could not be read: " + rootMessage(e));
        }
        if (record != null) {
            if (PasswordHash.verify(password, record)) {
                remember(key);
                return new Verdict(user, null);
            }
            return REJECTED;
        }
        if (derived == false) {
            // No account, so nothing to check against -- and answering now would answer faster than a
            // wrong password does, which tells the caller the name is not real. The decoy costs the same
            // derivation a real record would; the configured name skips it because it already paid one.
            PasswordHash.verify(password, decoy);
        }
        return REJECTED;
    }

    /**
     * Creates or replaces an account.
     *
     * @param user the username
     * @param password the password
     * @throws Exception if the account cannot be written
     */
    public void put(String user, char[] password) throws Exception {
        validateUsername(user);
        final String record = PasswordHash.encode(password, iterations);
        final org.opensearch.core.xcontent.XContentBuilder source = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()
            .startObject()
            .field("user", user)
            .field("hash", record)
            .endObject();
        final IndexRequest request = new IndexRequest(index).id(user).source(source).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        try {
            require().index(request).actionGet(timeout);
        } catch (IndexNotFoundException e) {
            createIndex();
            require().index(request).actionGet(timeout);
        }
        forget(user);
        // After the record, so a node that sees the new marker sees the new record. A bump that fails
        // leaves the account written and the caller with an error; retrying rewrites the same record and
        // moves the marker, which is the right way for that to resolve.
        bump();
    }

    /**
     * Removes an account.
     *
     * @param user the username
     * @return true if there was one to remove
     * @throws Exception if the removal fails
     */
    public boolean remove(String user) throws Exception {
        final boolean deleted;
        try {
            deleted = require().delete(new DeleteRequest(index, user).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .actionGet(timeout)
                .getResult() == org.opensearch.action.DocWriteResponse.Result.DELETED;
        } catch (IndexNotFoundException e) {
            return false;
        } finally {
            // Unconditional, and deliberately not only on the success path: a delete that failed after
            // the document was already gone would otherwise leave this node authenticating a removed
            // account until the entry aged out.
            forget(user);
        }
        if (deleted) {
            bump();
        }
        return deleted;
    }

    /**
     * Reads the marker.
     *
     * <p>One small read, and the whole of what another node's change costs this one until the marker
     * has moved. A random value rather than a count, for the reason the class comment gives.
     *
     * @return the marker, or 0 when no account has ever been changed
     * @throws Exception if the store cannot be read
     */
    public long version() throws Exception {
        final GetResponse response;
        try {
            response = require().get(new GetRequest(index, MARKER)).actionGet(timeout);
        } catch (IndexNotFoundException e) {
            return 0L;
        }
        if (response.isExists() == false) {
            return 0L;
        }
        final Object marker = response.getSource().get("marker");
        return marker instanceof Number ? ((Number) marker).longValue() : 0L;
    }

    /**
     * Reports whether an account exists, without revealing anything about its password.
     *
     * @param user the username
     * @return true if the account is there
     * @throws Exception if the store cannot be read
     */
    public boolean exists(String user) throws Exception {
        try {
            return lookup(user) != null;
        } catch (IndexNotFoundException e) {
            return false;
        }
    }

    private String lookup(String user) throws Exception {
        if (MARKER.equals(user)) {
            // The marker is a document in the account index and is not an account.
            return null;
        }
        final GetResponse response = require().get(new GetRequest(index, user)).actionGet(timeout);
        if (response.isExists() == false) {
            return null;
        }
        final Object hash = response.getSource().get("hash");
        return hash == null ? null : hash.toString();
    }

    private void createIndex() throws Exception {
        try {
            require().admin()
                .indices()
                .create(
                    new CreateIndexRequest(index).settings(Settings.builder().put("index.number_of_shards", 1))
                        // The hash is not indexed: nothing searches for one, and a field that is not
                        // indexed cannot be matched against by anyone who reaches the search API.
                        .mapping(
                            "{\"properties\":{\"user\":{\"type\":\"keyword\"},\"hash\":{\"type\":\"keyword\",\"index\":false},"
                                + "\"marker\":{\"type\":\"long\",\"index\":false}}}"
                        )
                )
                .actionGet(timeout);
        } catch (org.opensearch.ResourceAlreadyExistsException e) {
            // Two nodes creating the first account at once. One wins and the other proceeds.
        }
    }

    private Client require() {
        final Client available = client.get();
        if (available == null) {
            // The HTTP transport binds before plugins build their components, so there is a window at
            // startup where requests arrive and this does not exist yet. Saying so is a 503; the
            // configured account still works, because it is checked before anything reaches here.
            throw new IllegalStateException("this node has not finished starting");
        }
        return available;
    }

    private void bump() throws Exception {
        final org.opensearch.core.xcontent.XContentBuilder source = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()
            .startObject()
            .field("marker", RANDOM.nextLong())
            .endObject();
        require().index(new IndexRequest(index).id(MARKER).source(source).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
            .actionGet(timeout);
        // Deliberately not recorded as seen: this node's own next check will find it moved and drop a
        // cache it just cleaned itself, which is one spurious drop per change. Recording it would hide
        // a change another node made in the same second, and a hidden change is the one this exists
        // to prevent.
    }

    /** Claims the once-a-second slot for reading the marker; true for exactly one caller per interval. */
    private boolean claimMarkerCheck() {
        final long now = clock.getAsLong();
        final long last = markerCheckedAt.get();
        return now - last >= MARKER_INTERVAL_MILLIS && markerCheckedAt.compareAndSet(last, now);
    }

    private void refreshMarker() {
        final long current;
        try {
            current = version();
        } catch (Exception e) {
            // An unreadable store is the bad day the cache and the configured account exist for. Going
            // on trusting what was verified, until it ages out, is the TTL doing its job; turning a store
            // outage into a logout for every cached caller would be the opposite of the recovery path.
            return;
        }
        if (current != markerSeen) {
            markerSeen = current;
            synchronized (cache) {
                cache.clear();
            }
        }
    }

    private boolean cachedAndFresh(String key) {
        synchronized (cache) {
            final Long expiry = cache.get(key);
            if (expiry == null) {
                return false;
            }
            if (expiry <= clock.getAsLong()) {
                cache.remove(key);
                return false;
            }
            return true;
        }
    }

    private void remember(String key) {
        synchronized (cache) {
            cache.put(key, clock.getAsLong() + cacheTtlMillis);
        }
    }

    private void forget(String user) {
        final String prefix = user + " ";
        synchronized (cache) {
            final Iterator<String> keys = cache.keySet().iterator();
            while (keys.hasNext()) {
                if (keys.next().startsWith(prefix)) {
                    keys.remove();
                }
            }
        }
    }

    private static String rootMessage(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getMessage() == null ? cause.toString() : cause.getMessage();
    }
}
