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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * F-5 at the primitive: the cipher had no way to say where an envelope belonged, so every envelope
 * it produced was portable under one key.
 *
 * <p>These tests exist separately from {@code EncryptingBlobContainerTests} because the two prove
 * different things. There, the argument is that a specific attack on a specific wire format fails.
 * Here, it is that the primitive underneath has the property those arguments rest on -- and that the
 * legacy no-AAD overloads, which two other callers still use, behave exactly as they always did, so
 * their formats are unchanged.
 */
public class AesGcmCipherTests extends OpenSearchTestCase {

    private static SecretKey newAesKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public void testRoundTripsWithAssociatedData() throws Exception {
        SecretKey key = newAesKey();
        byte[] plaintext = utf8("the quick brown fox");
        byte[] aad = utf8("index-a|shard-0|bundle-1|2|65536|19|0");

        byte[] envelope = AesGcmCipher.encrypt(plaintext, key, aad);
        assertArrayEquals(plaintext, AesGcmCipher.decrypt(envelope, key, aad));
    }

    /**
     * The whole point. An envelope encrypted under one context must not decrypt under another, even
     * though the key is identical -- that is what stops a block from moving between blobs.
     */
    public void testAnEnvelopeDoesNotDecryptUnderDifferentAssociatedData() throws Exception {
        SecretKey key = newAesKey();
        byte[] envelope = AesGcmCipher.encrypt(utf8("secret"), key, utf8("index-a|shard-0|bundle-1|2|65536|6|0"));

        IOException e = expectThrows(
            IOException.class,
            () -> AesGcmCipher.decrypt(envelope, key, utf8("index-b|shard-0|bundle-1|2|65536|6|0"))
        );
        assertTrue(e.getMessage().contains("failed authentication"));
    }

    /** A single differing byte in the associated data is enough; there is no partial match. */
    public void testASingleByteChangeInAssociatedDataIsEnoughToFail() throws Exception {
        SecretKey key = newAesKey();
        byte[] aad = utf8("index-a|shard-0|bundle-1|2|65536|6|0");
        byte[] envelope = AesGcmCipher.encrypt(utf8("secret"), key, aad);

        byte[] tampered = aad.clone();
        tampered[tampered.length - 1] ^= 0x01;
        expectThrows(IOException.class, () -> AesGcmCipher.decrypt(envelope, key, tampered));
    }

    /**
     * Associated data is authenticated, not encrypted, so binding an envelope to its position must
     * cost nothing on the wire. If this ever stopped being true, {@code BlockLayout}'s stride
     * arithmetic -- which assumes a fixed IV+tag overhead per block and nothing else -- would
     * silently start computing wrong offsets.
     */
    public void testAssociatedDataAddsNoBytesToTheEnvelope() throws Exception {
        SecretKey key = newAesKey();
        byte[] plaintext = utf8("0123456789");

        int withoutAad = AesGcmCipher.encrypt(plaintext, key).length;
        int withAad = AesGcmCipher.encrypt(
            plaintext,
            key,
            utf8("a very long associated data string indeed, far longer than the plaintext")
        ).length;

        assertEquals("AAD is authenticated, not stored", withoutAad, withAad);
        assertEquals("12-byte IV plus 16-byte tag", plaintext.length + 12 + 16, withAad);
    }

    /**
     * The legacy overloads must stay bit-compatible with an empty AAD, because that equivalence is
     * what lets {@code EncryptingBlobContainer} read version 1 blobs by passing an empty array
     * rather than by keeping a second code path.
     */
    public void testTheNoArgOverloadsAreEquivalentToAnEmptyAssociatedData() throws Exception {
        SecretKey key = newAesKey();
        byte[] plaintext = utf8("compatibility matters");

        byte[] legacyEnvelope = AesGcmCipher.encrypt(plaintext, key);
        assertArrayEquals(plaintext, AesGcmCipher.decrypt(legacyEnvelope, key, new byte[0]));

        byte[] emptyAadEnvelope = AesGcmCipher.encrypt(plaintext, key, new byte[0]);
        assertArrayEquals(plaintext, AesGcmCipher.decrypt(emptyAadEnvelope, key));
    }

    /** A legacy envelope must not decrypt under a non-empty context, or the version gate would be pointless. */
    public void testALegacyEnvelopeDoesNotDecryptUnderRealAssociatedData() throws Exception {
        SecretKey key = newAesKey();
        byte[] legacyEnvelope = AesGcmCipher.encrypt(utf8("written by an older build"), key);

        expectThrows(IOException.class, () -> AesGcmCipher.decrypt(legacyEnvelope, key, utf8("index-a|shard-0|bundle-1|2|65536|25|0")));
    }

    /** Unchanged behaviour, restated so the AAD work cannot quietly weaken it. */
    public void testTamperedCiphertextStillFails() throws Exception {
        SecretKey key = newAesKey();
        byte[] aad = utf8("index-a|shard-0|bundle-1|2|65536|6|0");
        byte[] envelope = AesGcmCipher.encrypt(utf8("secret"), key, aad);
        envelope[envelope.length - 1] ^= 0x01;

        expectThrows(IOException.class, () -> AesGcmCipher.decrypt(envelope, key, aad));
    }

    public void testAnEnvelopeTooShortToHoldAnIvIsRejected() throws Exception {
        SecretKey key = newAesKey();
        IOException e = expectThrows(IOException.class, () -> AesGcmCipher.decrypt(new byte[4], key, new byte[0]));
        assertTrue(e.getMessage().contains("too short to contain an IV"));
    }
}
