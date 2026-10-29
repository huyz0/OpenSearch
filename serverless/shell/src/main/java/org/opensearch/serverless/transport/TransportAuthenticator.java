/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.hash.MessageDigests;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.TransportRequest;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Authenticates each forwarded request on its own, rather than by a secret that rides every frame.
 *
 * <p><b>The static token was a password on the wire.</b> One value, presented verbatim on every forward,
 * readable by anything that could see the transport, replayable as-is, and unrotatable without restarting
 * the fleet. This replaces it with an HMAC over the action, the sender, a timestamp, a nonce and a digest
 * of the request itself, keyed by a secret the register can rotate: a captured frame authenticates
 * nothing else, cannot be re-sent past the window or twice within it, and the key it was signed under
 * stops working one window after the next rotation.
 *
 * <p>Header value: {@code generation:sender:timestamp:nonce:mac}, all ASCII, nonce and mac in hex. The
 * receiving side accepts the current generation, or the previous one for one window after a rotation, so
 * a node that has not yet re-read the register keeps working while the fleet moves over.
 */
public final class TransportAuthenticator {

    /** The header a forwarded request carries its authentication in. */
    public static final String MAC_HEADER = "x-serverless-transport-mac";

    private static final String ALGORITHM = "HmacSHA256";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Supplier<MetadataPlane.TransportSecrets> secrets;
    private final Runnable refresh;
    private final Supplier<String> localNodeId;
    private final LongSupplier clock;
    private final LongSupplier windowMillis;
    private final NonceCache seen;
    private final LongSupplier probeClock;
    private boolean secretProbeRunning;
    private long lastSecretProbe = Long.MIN_VALUE;
    private static final long SECRET_PROBE_INTERVAL_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(1L);

    /**
     * Creates the authenticator.
     *
     * @param secrets the secrets as this node last read them, or null before a plane is attached
     * @param refresh re-reads the secrets from the register, for a generation this node has not seen
     * @param localNodeId this process's identity, which travels as the sender
     * @param clock the plane's clock
     * @param windowMillis how far a timestamp may be from now, and how long a nonce is remembered: one lease
     */
    public TransportAuthenticator(
        Supplier<MetadataPlane.TransportSecrets> secrets,
        Runnable refresh,
        Supplier<String> localNodeId,
        LongSupplier clock,
        LongSupplier windowMillis
    ) {
        this(secrets, refresh, localNodeId, clock, windowMillis, 65_536);
    }

    /**
     * Creates an authenticator with a bounded replay cache.
     *
     * @param secrets current transport secrets
     * @param refresh reloads secrets after rotation
     * @param localNodeId this sender's identity
     * @param clock wall clock in milliseconds
     * @param windowMillis permitted clock skew
     * @param capacity maximum live nonces; size for peak forwarded traffic over twice the window
     */
    public TransportAuthenticator(
        Supplier<MetadataPlane.TransportSecrets> secrets,
        Runnable refresh,
        Supplier<String> localNodeId,
        LongSupplier clock,
        LongSupplier windowMillis,
        int capacity
    ) {
        this(secrets, refresh, localNodeId, clock, windowMillis, capacity, System::nanoTime);
    }

    TransportAuthenticator(
        Supplier<MetadataPlane.TransportSecrets> secrets,
        Runnable refresh,
        Supplier<String> localNodeId,
        LongSupplier clock,
        LongSupplier windowMillis,
        int capacity,
        LongSupplier probeClock
    ) {
        if (capacity < 1) {
            throw new IllegalArgumentException("transport replay cache capacity must be positive");
        }
        this.probeClock = probeClock;
        this.secrets = secrets;
        this.refresh = refresh;
        this.localNodeId = localNodeId;
        this.clock = clock;
        this.windowMillis = windowMillis;
        this.seen = new NonceCache(capacity, windowMillis, clock);
    }

    /**
     * Signs a request for the wire.
     *
     * @param action the transport action it is sent under
     * @param request the request as it will be written
     * @return the header value, or null when this node has no secret yet
     * @throws IOException if the request cannot be serialised for its digest
     */
    public String sign(String action, TransportRequest request) throws IOException {
        return sign(action, request, localNodeId.get(), java.util.Map.of());
    }

    /**
     * Signs a request for one receiver and its exact propagated caller context.
     *
     * @param action action name
     * @param request request payload
     * @param recipient intended receiver's process identity
     * @param headers headers as they will be transmitted
     * @return authentication header, or null before secrets are available
     * @throws IOException if serialization fails
     */
    public String sign(String action, TransportRequest request, String recipient, java.util.Map<String, String> headers)
        throws IOException {
        final MetadataPlane.TransportSecrets current = secrets.get();
        if (current == null) {
            return null;
        }
        final long now = clock.getAsLong();
        final byte[] nonce = new byte[16];
        RANDOM.nextBytes(nonce);
        final String nonceHex = MessageDigests.toHexString(nonce);
        final String sender = localNodeId.get();
        final byte[] mac = mac(current.current(), action, sender, recipient, now, nonceHex, digest(request), digestHeaders(headers));
        return current.generation() + ":" + sender + ":" + now + ":" + nonceHex + ":" + MessageDigests.toHexString(mac);
    }

    /**
     * Refuses unless the header authenticates this exact request, now, once.
     *
     * @param action the transport action the request arrived under
     * @param request the request as it was read
     * @param header the presented header value, or null
     * @throws IOException if the request cannot be serialised for its digest
     * @throws OpenSearchSecurityException with 403 for anything that does not verify
     */
    public void verify(String action, TransportRequest request, String header) throws IOException {
        verify(action, request, header, java.util.Map.of());
    }

    /**
     * Verifies a request and its caller context for this node only.
     *
     * @param action action name
     * @param request received payload
     * @param header authentication header
     * @param headers request headers received from the wire
     * @throws IOException if serialization fails
     * @throws OpenSearchSecurityException when authentication fails
     */
    public void verify(String action, TransportRequest request, String header, java.util.Map<String, String> headers) throws IOException {
        if (header == null) {
            throw refuse("forwarded request without transport authentication");
        }
        final String[] parts = header.split(":");
        if (parts.length != 5) {
            throw refuse("malformed transport authentication header");
        }
        final long generation;
        final long timestamp;
        final byte[] presented;
        try {
            generation = Long.parseLong(parts[0]);
            timestamp = Long.parseLong(parts[2]);
            presented = hexToBytes(parts[4]);
        } catch (IllegalArgumentException e) {
            throw refuse("malformed transport authentication header");
        }
        final String sender = parts[1];
        final String nonceHex = parts[3];
        final long now = clock.getAsLong();
        final long window = windowMillis.getAsLong();
        final long age;
        try {
            age = Math.subtractExact(now, timestamp);
        } catch (ArithmeticException e) {
            throw refuse("transport authentication outside the time window");
        }
        if (age < -window || age > window) {
            throw refuse("transport authentication outside the time window");
        }
        MetadataPlane.TransportSecrets current = secrets.get();
        if (current == null) {
            throw refuse("this node has no transport secret yet");
        }
        if (generation > current.generation()) {
            // The claimed generation has not been authenticated yet. Bound probes independently of the
            // wall clock so arbitrary headers cannot buy one object-store read each, even with frozen clocks.
            if (beginSecretProbe()) {
                try {
                    refresh.run();
                } finally {
                    endSecretProbe();
                }
            } else {
                final MetadataPlane.TransportSecrets latest = secrets.get();
                if (latest != null && generation > latest.generation()) {
                    throw new org.opensearch.core.concurrency.OpenSearchRejectedExecutionException(
                        "transport secret refresh is pending or rate limited; retry after one second"
                    );
                }
            }
            current = secrets.get();
            if (current == null) {
                throw refuse("this node has no transport secret yet");
            }
        }
        final String key;
        if (generation == current.generation()) {
            key = current.current();
        } else if (generation == current.generation() - 1 && current.previous() != null && now - current.rotatedAtMillis() <= window) {
            key = current.previous();
        } else {
            throw refuse("transport authentication under an unknown secret generation");
        }
        final byte[] expected = mac(key, action, sender, localNodeId.get(), timestamp, nonceHex, digest(request), digestHeaders(headers));
        if (MessageDigest.isEqual(expected, presented) == false) {
            // The same words the token check used, so a test that pins the refusal still holds.
            throw refuse("forwarded request without a valid transport token");
        }
        if (seen.remember(sender + ":" + nonceHex, timestamp) == false) {
            throw refuse("replayed forwarded request or transport replay cache full");
        }
    }

    /** The identity of a transport process, including the incarnation whose replay cache is in memory. */
    static String identity(org.opensearch.cluster.node.DiscoveryNode node) {
        return node.getId() + "/" + node.getEphemeralId();
    }

    private synchronized boolean beginSecretProbe() {
        final long now = probeClock.getAsLong();
        if (secretProbeRunning || (lastSecretProbe != Long.MIN_VALUE && now - lastSecretProbe < SECRET_PROBE_INTERVAL_NANOS)) {
            return false;
        }
        lastSecretProbe = now;
        secretProbeRunning = true;
        return true;
    }

    private synchronized void endSecretProbe() {
        secretProbeRunning = false;
    }

    private static byte[] mac(
        String keyHex,
        String action,
        String sender,
        String recipient,
        long timestamp,
        String nonceHex,
        byte[] digest,
        byte[] headersDigest
    ) {
        try {
            final Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(hexToBytes(keyHex), ALGORITHM));
            for (String field : new String[] { "serverless-transport-v2", action, sender, recipient, Long.toString(timestamp), nonceHex }) {
                mac.update(field.getBytes(StandardCharsets.UTF_8));
                mac.update((byte) 0);
            }
            mac.update(digest);
            mac.update(headersDigest);
            return mac.doFinal();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("this JVM has no " + ALGORITHM, e);
        }
    }

    /** SHA-256 of canonical serialization, streamed without retaining or copying the full request. */
    private static byte[] digest(Writeable value) throws IOException {
        final MessageDigest digest = MessageDigests.sha256();
        try (var out = new OutputStreamStreamOutput(new DigestOutputStream(OutputStream.nullOutputStream(), digest))) {
            value.writeTo(out);
        }
        return digest.digest();
    }

    private static byte[] digestHeaders(java.util.Map<String, String> headers) throws IOException {
        final java.util.Map<String, String> authenticated = new java.util.TreeMap<>(headers);
        authenticated.remove(MAC_HEADER);
        authenticated.remove(ShardRouter.TOKEN_HEADER);
        return digest(
            out -> out.writeMap(
                authenticated,
                org.opensearch.core.common.io.stream.StreamOutput::writeString,
                org.opensearch.core.common.io.stream.StreamOutput::writeString
            )
        );
    }

    private static byte[] hexToBytes(String hex) {
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException("odd-length hex");
        }
        final byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            final int high = Character.digit(hex.charAt(2 * i), 16);
            final int low = Character.digit(hex.charAt(2 * i + 1), 16);
            if (high < 0 || low < 0) {
                throw new IllegalArgumentException("not hex");
            }
            out[i] = (byte) ((high << 4) | low);
        }
        return out;
    }

    private static OpenSearchSecurityException refuse(String why) {
        return new OpenSearchSecurityException(why, RestStatus.FORBIDDEN);
    }

    /**
     * A bounded record of accepted nonces, retained until the signed timestamp leaves the validity window.
     * Saturation refuses new frames instead of evicting a nonce that could still be replayed.
     */
    static final class NonceCache {

        private record Entry(String nonce, long expiresAt) {
        }

        private final Set<String> table = new HashSet<>();
        private final PriorityQueue<Entry> expirations = new PriorityQueue<>(java.util.Comparator.comparingLong(Entry::expiresAt));
        private final int capacity;
        private final LongSupplier windowMillis;
        private final LongSupplier clock;

        NonceCache(int capacity, LongSupplier windowMillis, LongSupplier clock) {
            this.capacity = capacity;
            this.windowMillis = windowMillis;
            this.clock = clock;
        }

        /** Records a nonce through the signed timestamp's validity; refuses duplicates and saturation. */
        synchronized boolean remember(String nonce, long timestamp) {
            final long now = clock.getAsLong();
            while (expirations.isEmpty() == false && expirations.peek().expiresAt() < now) {
                table.remove(expirations.remove().nonce());
            }
            final long expiresAt;
            try {
                expiresAt = Math.addExact(timestamp, windowMillis.getAsLong());
            } catch (ArithmeticException e) {
                return false;
            }
            if (expiresAt < now || table.contains(nonce) || table.size() >= capacity) {
                return false;
            }
            table.add(nonce);
            expirations.add(new Entry(nonce, expiresAt));
            return true;
        }
    }
}
