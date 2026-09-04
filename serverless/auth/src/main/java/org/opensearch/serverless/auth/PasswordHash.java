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
 * <p><b>A miss costs the same as a wrong password.</b> Verifying a name that does not exist would
 * otherwise return as soon as the lookup did, while verifying one that does spends the full derivation,
 * and the difference in response time would tell an attacker which usernames are real. So
 * {@link CredentialStore} derives against a {@link #decoy decoy record} when there is no account to
 * check: a record with a fixed salt and a digest nothing derives to, verified with exactly the code path
 * a real record takes. The answer is the same and so is the bill.
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

    /**
     * The most iterations a record may ask for before it is refused unread.
     *
     * <p>The count is read from the record, and a record is a document in an index: whoever can write one
     * -- another plugin through the client, anyone with write access to the bucket -- can write one that
     * asks for two billion iterations, and a checker thread would spend hours on it. Four such records
     * and every uncached login is a 503. A record above this is treated as unreadable, which denies.
     * {@link CredentialStore} passes a tighter bound derived from what it is configured to write.
     */
    public static final int MAX_ITERATIONS = 10_000_000;

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
        return verify(password, stored, MAX_ITERATIONS);
    }

    /**
     * Checks a password against a stored record, refusing a record whose work factor is above a bound.
     *
     * @param password the password offered
     * @param stored the record to check against
     * @param maxIterations the most iterations a record may ask for; above it the record is unreadable
     * @return true if they match
     */
    public static boolean verify(char[] password, String stored, int maxIterations) {
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
        if (iterations <= 0 || iterations > Math.min(maxIterations, MAX_ITERATIONS) || salt.length == 0) {
            return false;
        }
        return MessageDigest.isEqual(expected, derive(password, salt, iterations));
    }

    /**
     * A record that nothing verifies against, for spending a derivation on an account that is not there.
     *
     * <p>The salt is fixed and the digest is all zeros. {@link #verify} parses it, derives at the given
     * work factor and compares -- the same work a real record costs -- and the comparison fails unless a
     * password derives to sixty-four zero bytes, which is not a thing that happens. Built once by
     * {@link CredentialStore} at the work factor it stores new records with, so the decoy keeps pace with
     * the setting rather than with a compiled-in number.
     *
     * @param iterations the work factor, matching what stored records cost
     * @return the record
     */
    static String decoy(int iterations) {
        final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
        return SCHEME
            + "$"
            + iterations
            + "$"
            + encoder.encodeToString(new byte[SALT_BYTES])
            + "$"
            + encoder.encodeToString(new byte[KEY_BITS / 8]);
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
     * <p><b>Keyed, so that it means nothing outside the process that made it.</b> A plain digest has no
     * work factor, and the cache holds it for a TTL: a heap dump would hand an attacker every recently
     * used password as a hash crackable at GPU speed -- exactly what the salted derivation was chosen to
     * deny. An HMAC under a key generated at construction and never persisted is the same size, costs the
     * same, and is worthless without a key that exists only in this node's memory.
     *
     * @param key the process-local key, which is never stored
     * @param user the username
     * @param password the password
     * @return the fingerprint
     */
    static String fingerprint(byte[] key, String user, char[] password) {
        try {
            final javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(key, "HmacSHA256"));
            mac.update(user.getBytes(StandardCharsets.UTF_8));
            mac.update((byte) 0);
            final java.nio.ByteBuffer buffer = StandardCharsets.UTF_8.encode(java.nio.CharBuffer.wrap(password));
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            mac.update(bytes);
            java.util.Arrays.fill(bytes, (byte) 0);
            return Base64.getEncoder().withoutPadding().encodeToString(mac.doFinal());
        } catch (NoSuchAlgorithmException | java.security.InvalidKeyException e) {
            throw new IllegalStateException("this JVM has no HmacSHA256", e);
        }
    }
}
