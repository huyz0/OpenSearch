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
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.TransportRequest;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;
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

    /**
     * Creates the authenticator.
     *
     * @param secrets the secrets as this node last read them, or null before a plane is attached
     * @param refresh re-reads the secrets from the register, for a generation this node has not seen
     * @param localNodeId this node's id, which travels as the sender
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
        this.secrets = secrets;
        this.refresh = refresh;
        this.localNodeId = localNodeId;
        this.clock = clock;
        this.windowMillis = windowMillis;
        this.seen = new NonceCache(65_536, windowMillis, clock);
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
        final MetadataPlane.TransportSecrets current = secrets.get();
        if (current == null) {
            return null;
        }
        final long now = clock.getAsLong();
        final byte[] nonce = new byte[16];
        RANDOM.nextBytes(nonce);
        final String nonceHex = MessageDigests.toHexString(nonce);
        final String sender = localNodeId.get();
        final byte[] mac = mac(current.current(), action, sender, now, nonceHex, digest(request));
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
        if (Math.abs(now - timestamp) > window) {
            throw refuse("transport authentication outside the time window");
        }
        MetadataPlane.TransportSecrets current = secrets.get();
        if (current == null) {
            throw refuse("this node has no transport secret yet");
        }
        if (generation > current.generation()) {
            // A newer generation exists that this node has not read: the register, not the cache, answers.
            refresh.run();
            current = secrets.get();
        }
        final String key;
        if (generation == current.generation()) {
            key = current.current();
        } else if (generation == current.generation() - 1 && current.previous() != null && now - current.rotatedAtMillis() <= window) {
            key = current.previous();
        } else {
            throw refuse("transport authentication under an unknown secret generation");
        }
        final byte[] expected = mac(key, action, sender, timestamp, nonceHex, digest(request));
        if (MessageDigest.isEqual(expected, presented) == false) {
            // The same words the token check used, so a test that pins the refusal still holds.
            throw refuse("forwarded request without a valid transport token");
        }
        if (seen.remember(sender + ":" + nonceHex, now) == false) {
            throw refuse("replayed forwarded request");
        }
    }

    private static byte[] mac(String keyHex, String action, String sender, long timestamp, String nonceHex, byte[] digest) {
        try {
            final Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(hexToBytes(keyHex), ALGORITHM));
            for (String field : new String[] { action, sender, Long.toString(timestamp), nonceHex }) {
                mac.update(field.getBytes(StandardCharsets.UTF_8));
                mac.update((byte) 0);
            }
            mac.update(digest);
            return mac.doFinal();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("this JVM has no " + ALGORITHM, e);
        }
    }

    /** SHA-256 of the request as it is written; ties the MAC to the payload, not only the envelope. */
    private static byte[] digest(TransportRequest request) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            return MessageDigests.sha256().digest(BytesReference.toBytes(out.bytes()));
        }
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
     * A bounded record of (sender, nonce) pairs seen, with expiry: a nonce older than the window can never
     * verify again anyway, so it need not be remembered past it.
     */
    static final class NonceCache {

        private final Map<String, Long> table;
        private final LongSupplier windowMillis;
        private final LongSupplier clock;

        NonceCache(int capacity, LongSupplier windowMillis, LongSupplier clock) {
            this.windowMillis = windowMillis;
            this.clock = clock;
            this.table = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                    return size() > capacity || eldest.getValue() < clock.getAsLong() - windowMillis.getAsLong();
                }
            };
        }

        /** Records the nonce; false if it was already there. */
        synchronized boolean remember(String nonce, long now) {
            return table.putIfAbsent(nonce, now) == null;
        }
    }
}
