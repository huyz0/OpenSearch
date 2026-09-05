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
 *
 * <h2>Nothing constructs this outside tests, and the RFC should not be read as though something does</h2>
 *
 * <p>Stated plainly because the alternative is worse than the gap itself: <b>no node configuration
 * can produce an instance of this class.</b> The only production construction of any
 * {@link EncryptionKeyProvider} is {@code ServerlessStoragePlugin#createComponents} building a
 * {@link StaticEncryptionKeyProvider} from the single node-level {@code
 * serverless_storage.encryption_key} secure setting. This class is referenced from javadoc and from
 * two test classes, and from nowhere else. Every deployable configuration therefore has exactly one
 * key for the whole node, and rfc-serverless-opensearch.md &sect;12's per-index key domain -- "a
 * compromised chunk yields nothing without per-index keys" -- <b>is not achieved</b>.
 *
 * <p>This matters more than an ordinary unfinished feature, because the shape of the code implies
 * the opposite. {@link EncryptionKeyProvider#currentKey(String)} exists, this class exists, both are
 * tested, and {@code WalRecordCrypto} and {@link EncryptingBlobContainer} both call the per-index
 * overload correctly -- so a reader can follow every call site, find them all correct, and conclude
 * per-index isolation works. It does not; the provider at the end of every one of those chains
 * ignores the argument. The call sites are correct so that wiring this becomes configuration rather
 * than code, which is worth having, but it is not the same as having it.
 *
 * <p><b>What wiring it for real would take.</b> An affix secure setting --
 * {@code serverless_storage.encryption_key.<index-uuid>} in the node keystore, built with
 * {@code Setting.affixKeySetting} the way {@code repository-s3}'s per-client credentials already
 * are -- read at {@code createComponents} into the map this constructor takes, with the existing
 * single-key setting becoming {@code defaultKey}. That is a contained change. What makes it not
 * merely contained is the operational half: keys keyed by index UUID cannot be written to the
 * keystore until the index exists and its UUID is known, which makes key provisioning a
 * post-creation step on every node, and index deletion a key-cleanup step. Until that lifecycle is
 * designed, adding the setting would produce a per-index key domain that silently falls back to the
 * default key for every index nobody remembered to provision -- which reads as isolation and is
 * not, exactly the failure mode this note exists to stop.
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
