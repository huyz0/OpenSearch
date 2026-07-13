/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import javax.crypto.SecretKey;

import java.util.Map;

/**
 * The first real per-index-aware {@link EncryptionKeyProvider}: a fixed key per {@code indexUuid},
 * no rotation -- {@link StaticEncryptionKeyProvider}'s own "simplest possible provider" reasoning,
 * generalized to actually distinguish by index rather than returning one key regardless. Not what a
 * production deployment should use long-term (no KMS integration, no rotation, keys must be
 * supplied by the caller through some other secure channel this class has no opinion on) -- exists
 * so {@code WalRecordCrypto}'s own per-record encryption (rfc-serverless-opensearch.md &sect;12
 * bullet 1), which already carries {@code indexUuid} alongside each payload, can buy real per-index
 * key isolation for the shared node-level WAL today, the way {@link EncryptingBlobContainer}
 * already has since bundles/manifests are single-index by construction.
 */
public final class PerIndexEncryptionKeyProvider implements EncryptionKeyProvider {

    private final Map<String, SecretKey> keysByIndex;
    private final SecretKey defaultKey;

    /**
     * Wraps a fixed per-index key map, plus an optional default for indices not present in it.
     *
     * @param keysByIndex the key to use for each {@code indexUuid} that has one configured.
     * @param defaultKey the key to fall back to for both {@link #currentKey()} (which has no index
     *                    to look up) and {@link #currentKey(String)} calls naming an index absent
     *                    from {@code keysByIndex}; {@code null} to instead fail loudly on either
     *                    case, for a deployment that wants every index's key to be explicit.
     */
    public PerIndexEncryptionKeyProvider(Map<String, SecretKey> keysByIndex, SecretKey defaultKey) {
        this.keysByIndex = Map.copyOf(keysByIndex);
        this.defaultKey = defaultKey;
    }

    @Override
    public SecretKey currentKey() {
        if (defaultKey == null) {
            throw new IllegalStateException(
                "no default key configured on this per-index provider -- currentKey(indexUuid) must be used instead"
            );
        }
        return defaultKey;
    }

    @Override
    public SecretKey currentKey(String indexUuid) {
        SecretKey key = keysByIndex.get(indexUuid);
        if (key != null) {
            return key;
        }
        if (defaultKey != null) {
            return defaultKey;
        }
        throw new IllegalStateException("no key configured for index [" + indexUuid + "] and no default key to fall back to");
    }
}
