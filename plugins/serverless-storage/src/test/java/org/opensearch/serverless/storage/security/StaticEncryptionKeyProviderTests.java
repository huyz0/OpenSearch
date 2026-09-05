/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.test.OpenSearchTestCase;

import javax.crypto.SecretKey;

import java.nio.charset.StandardCharsets;

/**
 * F-7: key-length validation at construction.
 *
 * <p>The point of these tests is <em>when</em> the failure happens, not that it happens. Before this
 * change a bad key was accepted by {@code SecretKeySpec} without complaint, the node started green,
 * and the first symptom was {@code IOException("failed to encrypt")} on every write forever -- an
 * error that names neither the setting nor the mistake, arrives at an indexing client rather than at
 * the operator who made the change, and looks exactly like a storage outage. Rejecting at
 * construction is what turns that into a startup failure naming the setting.
 */
public class StaticEncryptionKeyProviderTests extends OpenSearchTestCase {

    public void testAcceptsTheThreeValidAesKeyLengths() {
        for (int length : new int[] { 16, 24, 32 }) {
            SecretKey key = StaticEncryptionKeyProvider.fromRawKeyBytes(new byte[length]).currentKey();
            assertEquals("AES", key.getAlgorithm());
            assertEquals(length, key.getEncoded().length);
        }
    }

    /**
     * Five bytes: the case from the report. {@code SecretKeySpec} takes it, and every write then
     * fails at {@code Cipher.init}.
     */
    public void testRejectsAKeyThatIsTooShort() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> StaticEncryptionKeyProvider.fromRawKeyBytes(new byte[5])
        );
        assertTrue("the message must say what a valid length is: " + e.getMessage(), e.getMessage().contains("16, 24 or 32 bytes"));
        assertTrue("the message must name the setting: " + e.getMessage(), e.getMessage().contains("serverless_storage.encryption_key"));
    }

    /** An empty key, which is what an operator gets from a keystore entry set to the empty string. */
    public void testRejectsAnEmptyKey() {
        expectThrows(IllegalArgumentException.class, () -> StaticEncryptionKeyProvider.fromRawKeyBytes(new byte[0]));
    }

    /**
     * The realistic mistake: base64-encoding a passphrase instead of raw key bytes. "hunter2hunter2!"
     * is 15 characters, so it decodes to a length AES cannot use -- and before this check it was
     * accepted, which is how a typo became a durability outage discovered by a client.
     */
    public void testRejectsAPassphraseMistakenForKeyBytes() {
        byte[] passphrase = "hunter2hunter2!".getBytes(StandardCharsets.UTF_8);
        assertEquals("this test only means anything if the passphrase is not a valid AES length", 15, passphrase.length);
        expectThrows(IllegalArgumentException.class, () -> StaticEncryptionKeyProvider.fromRawKeyBytes(passphrase));
    }

    /** 33 bytes: one over AES-256, the off-by-one an operator gets from a stray newline in a key file. */
    public void testRejectsAKeyThatIsTooLong() {
        expectThrows(IllegalArgumentException.class, () -> StaticEncryptionKeyProvider.fromRawKeyBytes(new byte[33]));
    }

    /**
     * The provider must not retain the caller's array, because the caller zeroes it immediately --
     * {@code ServerlessStoragePlugin#createComponents} wipes {@code rawKeyBytes} in a {@code finally}
     * right after this call. If {@code SecretKeySpec} did not copy, the node would come up holding a
     * key of zeroes and every read of previously-written data would fail authentication.
     */
    public void testTheProviderCopiesTheKeyRatherThanRetainingTheCallersArray() {
        byte[] raw = new byte[32];
        for (int i = 0; i < raw.length; i++) {
            raw[i] = (byte) (i + 1);
        }
        byte[] expected = raw.clone();

        StaticEncryptionKeyProvider provider = StaticEncryptionKeyProvider.fromRawKeyBytes(raw);
        java.util.Arrays.fill(raw, (byte) 0);

        assertArrayEquals("wiping the caller's array must not wipe the key", expected, provider.currentKey().getEncoded());
    }

    /**
     * The honesty check for &sect;12: this provider ignores the index it is asked about, so the
     * per-index overload is not per-index in any deployable configuration. Asserted rather than only
     * documented, so that if a real per-index provider is ever wired in, this test is the thing that
     * says the old statement is no longer true.
     */
    public void testTheIndexAwareOverloadReturnsTheSameKeyForEveryIndex() {
        StaticEncryptionKeyProvider provider = StaticEncryptionKeyProvider.fromRawKeyBytes(new byte[32]);
        assertSame(provider.currentKey(), provider.currentKey("index-a"));
        assertSame(provider.currentKey(), provider.currentKey("index-b"));
    }
}
