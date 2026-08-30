/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.auth;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Locale;

/**
 * How a password is stored, and how one is checked against what was stored.
 *
 * <p><b>Salted PBKDF2-HMAC-SHA512, and the alternatives are worse.</b> A plain digest of a password —
 * even SHA-512 — is computed at billions of guesses per second on commodity hardware, so a stolen copy of
 * the credential index would be a stolen copy of the passwords. A deliberately slow, salted derivation is
 * what turns "the attacker has the file" into "the attacker has a bill". bcrypt or Argon2 would be at
 * least as good and arguably better, and both would mean a new third-party dependency in a project whose
 * whole shape is about not adding any; PBKDF2 is in the JDK, is the one FIPS-approved option of the three,
 * and is not the weak link here.
 *
 * <p><b>The iteration count is stored in the record, not compiled in.</b> The right number goes up with
 * hardware, and a system that hard-codes it cannot raise it without invalidating every stored password.
 * Reading it back from the record means a raised default applies to new and changed passwords while old
 * ones keep verifying, which is the only way this is ever actually raised in production.
 *
 * <p><b>Comparison is constant-time</b>, via {@link MessageDigest#isEqual}. A byte-by-byte comparison that
 * returns early leaks, through timing, how much of a derived key a guess got right. That is a narrow
 * channel and a real one, and avoiding it costs a method call.
 *
 * <p><b>What this does not defend against.</b> Verifying a name that does not exist returns quickly, while
 * verifying one that does spends the full derivation, so the response time tells an attacker whether a
 * username is real. Closing that means deriving against a dummy record for unknown users, which
 * {@link CredentialStore} does not do; the exposure is that account names are enumerable, and it is
 * written down rather than papered over.
 */
public final class PasswordHash {

    /** Names the scheme in the stored record, so a later one can be added without ambiguity. */
    static final String SCHEME = "pbkdf2_sha512";

    private static final String ALGORITHM = "PBKDF2WithHmacSHA512";
    private static final int SALT_BYTES = 16;
    private static final int KEY_BITS = 512;

    /**
     * OWASP's current floor for PBKDF2-HMAC-SHA512. Costs a few hundred milliseconds, which is why
     * {@link CredentialStore} caches a verified credential rather than paying it per request.
     */
    public static final int DEFAULT_ITERATIONS = 210_000;

    private static final SecureRandom RANDOM = new SecureRandom();

    private PasswordHash() {}

    /**
     * Derives a storable record from a password.
     *
     * @param password the password, which is not retained
     * @param iterations the work factor
     * @return the record to store
     */
    public static String encode(char[] password, int iterations) {
        final byte[] salt = new byte[SALT_BYTES];
        RANDOM.nextBytes(salt);
        final byte[] derived = derive(password, salt, iterations);
        final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
        return SCHEME + "$" + iterations + "$" + encoder.encodeToString(salt) + "$" + encoder.encodeToString(derived);
    }

    /**
     * Checks a password against a stored record.
     *
     * <p>A record this does not understand is a failure, not a pass: an unreadable credential must never
     * be treated as one that matched.
     *
     * @param password the password offered
     * @param stored the record to check against
     * @return true if they match
     */
    public static boolean verify(char[] password, String stored) {
        if (stored == null) {
            return false;
        }
        final String[] parts = stored.split("\\$");
        if (parts.length != 4 || SCHEME.equals(parts[0]) == false) {
            return false;
        }
        final int iterations;
        final byte[] salt;
        final byte[] expected;
        try {
            iterations = Integer.parseInt(parts[1]);
            salt = Base64.getDecoder().decode(parts[2]);
            expected = Base64.getDecoder().decode(parts[3]);
        } catch (IllegalArgumentException e) {
            return false;
        }
        if (iterations <= 0 || salt.length == 0) {
            return false;
        }
        return MessageDigest.isEqual(expected, derive(password, salt, iterations));
    }

    private static byte[] derive(char[] password, byte[] salt, int iterations) {
        final PBEKeySpec spec = new PBEKeySpec(password, salt, iterations, KEY_BITS);
        try {
            return SecretKeyFactory.getInstance(ALGORITHM).generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            // PBKDF2-HMAC-SHA512 is mandatory in every JDK this runs on, so this is not a condition an
            // operator can be in -- but failing closed matters more than the odds, and a thrown error
            // denies where a caught one would have to decide what "could not check" means.
            throw new IllegalStateException("this JVM cannot derive " + ALGORITHM.toLowerCase(Locale.ROOT) + " keys", e);
        } finally {
            spec.clearPassword();
        }
    }

    /**
     * A short, non-reversible fingerprint of a credential, for use as a cache key.
     *
     * <p>Not for storage: this is a plain digest with no work factor, and a stored one would be
     * brute-forceable. It exists so that {@link CredentialStore}'s cache can recognise the same password
     * again without keeping the password itself in a long-lived map.
     *
     * @param user the username
     * @param password the password
     * @return the fingerprint
     */
    static String fingerprint(String user, char[] password) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(user.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            final java.nio.ByteBuffer buffer = StandardCharsets.UTF_8.encode(java.nio.CharBuffer.wrap(password));
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            digest.update(bytes);
            java.util.Arrays.fill(bytes, (byte) 0);
            return Base64.getEncoder().withoutPadding().encodeToString(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("this JVM has no SHA-256", e);
        }
    }
}
