/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import javax.crypto.SecretKey;

/**
 * Supplies the symmetric key {@link EncryptingBlobContainer} encrypts and decrypts blob content
 * with. Deliberately minimal and backend-agnostic: a real deployment would back this with a KMS
 * (AWS KMS, GCP KMS, Azure Key Vault, ...), none of which this interface commits to -- it exists
 * so {@link EncryptingBlobContainer} never depends on a specific key-management backend, the same
 * seam-not-implementation split used for {@code BundleFileReader} and the per-cloud {@code
 * compareAndSwapRegister} implementations elsewhere in this module.
 *
 * <p><b>Key rotation is not supported by this interface or by anything that reads {@link
 * #currentKey()}, and changing the key an implementation returns is destructive, not additive.</b>
 * Neither the block-encrypted bundle format ({@link BlockLayout}) nor the WAL record envelope
 * (see {@code WalRecordCrypto}) carries any key identifier alongside the ciphertext they write --
 * there is nothing recorded anywhere that says "this content was encrypted under key X," so there
 * is no way, at read time, to know which of several keys to try. Decryption always uses whatever
 * {@link #currentKey()} (or {@link #currentKey(String)}) returns <em>right now</em>. Concretely:
 * if an implementation of this interface is ever changed to return a different key -- a real KMS
 * rotation, a config change, a credential refresh that happens to also rotate the underlying key
 * material -- every block/record encrypted under the previous key becomes permanently unreadable
 * from that moment on (AES-GCM's authentication tag check fails loudly on the wrong key, so this
 * surfaces as a hard decryption failure on every affected read, not silent corruption, but there is
 * no supported way to recover: the old key is gone and nothing durable records which key was used
 * for which content). Do not implement key rotation against this interface as it stands; doing so
 * requires a wire-format change (a key identifier recorded alongside each block/record) plus this
 * interface gaining a way to look up a specific past key by that identifier, neither of which exists
 * today.
 */
public interface EncryptionKeyProvider {

    /**
     * The key to use for encrypting new content and decrypting content encrypted with it.
     *
     * <p>See this interface's own javadoc: the value returned here must never change once any
     * content has been encrypted under it, other than by discarding this deployment's data
     * entirely -- there is no key-identifier plumbed anywhere that would let a reader distinguish
     * old ciphertext from new, or recover an old key to decrypt it.
     */
    SecretKey currentKey();

    /**
     * The key to use for content belonging to a specific index. Defaults to {@link #currentKey()}
     * for providers (like {@link StaticEncryptionKeyProvider}) that only ever have one key
     * regardless of index -- {@link PerIndexEncryptionKeyProvider} is the one that actually
     * distinguishes by {@code indexUuid}. Exists so callers that multiplex several indices' data
     * through one shared object ({@code WalRecordCrypto}, whose own per-record encryption already
     * carries {@code indexUuid} alongside each payload -- see its own javadoc) can get real
     * per-index key isolation the moment a per-index-aware provider is configured, without any
     * change to the caller or the wire format it produces.
     *
     * @param indexUuid the index the content being encrypted/decrypted belongs to.
     */
    default SecretKey currentKey(String indexUuid) {
        return currentKey();
    }
}
