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
 * (AWS KMS, GCP KMS, Azure Key Vault, ...) and key rotation, none of which this interface commits
 * to -- it exists so {@link EncryptingBlobContainer} never depends on a specific key-management
 * backend, the same seam-not-implementation split used for {@code BundleFileReader} and the
 * per-cloud {@code compareAndSwapRegister} implementations elsewhere in this module.
 */
public interface EncryptionKeyProvider {

    /** The key to use for encrypting new content and decrypting content encrypted with it. */
    SecretKey currentKey();
}
