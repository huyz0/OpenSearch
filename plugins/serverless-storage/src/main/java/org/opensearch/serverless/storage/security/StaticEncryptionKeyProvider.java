/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.util.Set;

/**
 * The simplest possible {@link EncryptionKeyProvider}: one fixed key for the lifetime of the
 * provider, no rotation. Not what a production deployment should use (no KMS integration, no
 * rotation, the key must be supplied by the caller through some other secure channel this class
 * has no opinion on) -- useful for tests and for a first, honest "encryption works end to end"
 * deployment before a real KMS-backed provider is built.
 *
 * <p><b>This is also the only key provider any node configuration can actually build</b>
 * ({@code ServerlessStoragePlugin#createComponents} constructs one from
 * {@code serverless_storage.encryption_key} and nothing else constructs any provider anywhere), so
 * it is worth stating plainly: one key, node-wide, shared by every index on the node. Whatever
 * {@link EncryptionKeyProvider#currentKey(String)} implies elsewhere, in a deployable configuration
 * it returns the same key for every argument. See {@link PerIndexEncryptionKeyProvider} for the
 * per-index provider that exists but has no settings path.
 */
public final class StaticEncryptionKeyProvider implements EncryptionKeyProvider {

    private final SecretKey key;

    /**
     * Wraps a fixed key.
     *
     * @param key the key to return from every {@link #currentKey()} call.
     */
    public StaticEncryptionKeyProvider(SecretKey key) {
        this.key = key;
    }

    /**
     * The three key lengths AES is defined for. {@link SecretKeySpec} accepts <em>any</em> byte
     * count without complaint -- it is a dumb container, not a validator -- and the rejection only
     * happens much later, inside {@code Cipher#init}.
     */
    private static final Set<Integer> VALID_AES_KEY_LENGTHS_BYTES = Set.of(16, 24, 32);

    /**
     * Builds a provider from raw AES key bytes, rejecting a length AES cannot use.
     *
     * <p><b>Why the check is here and not left to the cipher.</b> Without it, a five-byte key
     * started the node perfectly cleanly: {@code SecretKeySpec} took it, this provider handed it
     * out, and the first failure was {@code Cipher.init} throwing on the first write -- surfacing as
     * {@code IOException("failed to encrypt")} from {@link AesGcmCipher}, on every write, forever.
     * A node that starts green and then cannot make a single durable write is the worst shape a
     * configuration error can take: it looks like a storage outage rather than a typo in the
     * keystore, and it is discovered by an indexing client rather than by the operator who made the
     * change. Failing at construction turns the same mistake into a startup failure naming the
     * setting.
     *
     * @param rawKeyBytes the raw AES key bytes (16/24/32 bytes for AES-128/192/256).
     * @return a provider wrapping the decoded key.
     * @throws IllegalArgumentException if {@code rawKeyBytes} is not a valid AES key length.
     */
    public static StaticEncryptionKeyProvider fromRawKeyBytes(byte[] rawKeyBytes) {
        if (VALID_AES_KEY_LENGTHS_BYTES.contains(rawKeyBytes.length) == false) {
            throw new IllegalArgumentException(
                "AES key must be 16, 24 or 32 bytes (AES-128/192/256), but the configured key decoded to "
                    + rawKeyBytes.length
                    + " bytes; check that serverless_storage.encryption_key holds the base64 encoding of raw key "
                    + "bytes, not of a passphrase or a hex string"
            );
        }
        // SecretKeySpec copies the array, so the caller is free to -- and does -- zero its own copy
        // immediately after this returns. Nothing here retains rawKeyBytes.
        return new StaticEncryptionKeyProvider(new SecretKeySpec(rawKeyBytes, "AES"));
    }

    @Override
    public SecretKey currentKey() {
        return key;
    }
}
