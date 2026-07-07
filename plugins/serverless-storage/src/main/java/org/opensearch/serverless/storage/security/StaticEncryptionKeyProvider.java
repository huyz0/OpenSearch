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

/**
 * The simplest possible {@link EncryptionKeyProvider}: one fixed key for the lifetime of the
 * provider, no rotation. Not what a production deployment should use (no KMS integration, no
 * rotation, the key must be supplied by the caller through some other secure channel this class
 * has no opinion on) -- useful for tests and for a first, honest "encryption works end to end"
 * deployment before a real KMS-backed provider is built.
 */
public final class StaticEncryptionKeyProvider implements EncryptionKeyProvider {

    private final SecretKey key;

    public StaticEncryptionKeyProvider(SecretKey key) {
        this.key = key;
    }

    public static StaticEncryptionKeyProvider fromRawKeyBytes(byte[] rawKeyBytes) {
        return new StaticEncryptionKeyProvider(new SecretKeySpec(rawKeyBytes, "AES"));
    }

    @Override
    public SecretKey currentKey() {
        return key;
    }
}
