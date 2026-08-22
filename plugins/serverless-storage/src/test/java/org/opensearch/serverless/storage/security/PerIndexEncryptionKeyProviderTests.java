/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.test.OpenSearchTestCase;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.util.Map;

public class PerIndexEncryptionKeyProviderTests extends OpenSearchTestCase {

    private static SecretKey newAesKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }

    public void testDifferentIndicesGetGenuinelyDifferentConfiguredKeys() throws Exception {
        SecretKey keyA = newAesKey();
        SecretKey keyB = newAesKey();
        PerIndexEncryptionKeyProvider provider = new PerIndexEncryptionKeyProvider(Map.of("index-a", keyA, "index-b", keyB), null);

        assertEquals(keyA, provider.currentKey("index-a"));
        assertEquals(keyB, provider.currentKey("index-b"));
        assertNotEquals(provider.currentKey("index-a"), provider.currentKey("index-b"));
    }

    public void testUnconfiguredIndexFallsBackToTheDefaultKeyWhenOneExists() throws Exception {
        SecretKey keyA = newAesKey();
        SecretKey defaultKey = newAesKey();
        PerIndexEncryptionKeyProvider provider = new PerIndexEncryptionKeyProvider(Map.of("index-a", keyA), defaultKey);

        assertEquals(keyA, provider.currentKey("index-a"));
        assertEquals(defaultKey, provider.currentKey("index-unconfigured"));
        assertEquals("currentKey() itself must also return the default", defaultKey, provider.currentKey());
    }

    public void testUnconfiguredIndexFailsLoudlyWithNoDefaultKeyRatherThanSilentlyPickingOne() throws Exception {
        SecretKey keyA = newAesKey();
        PerIndexEncryptionKeyProvider provider = new PerIndexEncryptionKeyProvider(Map.of("index-a", keyA), null);

        expectThrows(IllegalStateException.class, () -> provider.currentKey("index-unconfigured"));
        // currentKey() itself has no index to look up, so it must also fail loudly with no default.
        expectThrows(IllegalStateException.class, provider::currentKey);
    }
}
