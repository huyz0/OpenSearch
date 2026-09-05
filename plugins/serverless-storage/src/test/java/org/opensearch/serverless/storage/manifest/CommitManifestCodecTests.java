/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * The manifest was the one persisted format in this plugin with neither a magic header, nor a
 * format version, nor a checksum -- while the bundle and WAL chunk formats both have all three.
 * Every test here fails against the bare {@code CommitManifest#writeTo} body the store used to
 * write and read.
 */
public class CommitManifestCodecTests extends OpenSearchTestCase {

    private static CommitManifest manifest() {
        return new CommitManifest(
            "AbCdEfGhIjKlMnOpQrStUv",
            3,
            7L,
            42L,
            "segments_5",
            Map.of("segments_5", new FileReference("bundle-a", 0L, 155L, 1234L), "_0.si", new FileReference("bundle-a", 155L, 400L, 5678L)),
            99L,
            98L,
            null,
            1L,
            PruningStats.empty(),
            1_700_000_000_000L
        );
    }

    public void testRoundTripsThroughTheEnvelope() throws Exception {
        CommitManifest original = manifest();
        CommitManifest read = CommitManifestCodec.fromBytes(CommitManifestCodec.toBytes(original), "manifest-7-42");
        assertEquals(original.indexUuid(), read.indexUuid());
        assertEquals(original.shardId(), read.shardId());
        assertEquals(original.primaryTerm(), read.primaryTerm());
        assertEquals(original.generation(), read.generation());
        assertEquals(original.segmentsFileName(), read.segmentsFileName());
        assertEquals(original.files(), read.files());
        assertEquals(original.maxSeqNo(), read.maxSeqNo());
        assertEquals(original.localCheckpoint(), read.localCheckpoint());
    }

    /** Magic and version are actually present, so a future format change has something to key on. */
    public void testTheBlobCarriesMagicAndAFormatVersion() throws Exception {
        byte[] blob = CommitManifestCodec.toBytes(manifest());
        assertTrue("a written manifest must be enveloped", CommitManifestCodec.isEnveloped(blob));
        assertEquals('S', blob[0]);
        assertEquals('C', blob[1]);
        assertEquals('M', blob[2]);
        assertEquals('1', blob[3]);
        // formatVersion is the next big-endian int.
        int version = ((blob[4] & 0xFF) << 24) | ((blob[5] & 0xFF) << 16) | ((blob[6] & 0xFF) << 8) | (blob[7] & 0xFF);
        assertEquals(CommitManifestCodec.FORMAT_VERSION, version);
    }

    /**
     * The finding this whole envelope exists for: a flipped bit anywhere in the body used to
     * produce either a huge allocation or a structurally valid manifest with wrong offsets,
     * lengths, or a wrong segments file name -- which was then used verbatim to build the shard's
     * file map, with nothing downstream able to catch it (every per-file checksum it would be
     * compared against came out of the same corrupted blob).
     */
    public void testASingleFlippedBitInTheBodyIsRejected() throws Exception {
        byte[] blob = CommitManifestCodec.toBytes(manifest());
        // Somewhere strictly inside the body, past the 12-byte header and before the 8-byte trailer.
        int corruptAt = 12 + randomIntBetween(0, blob.length - 12 - 8 - 1);
        blob[corruptAt] ^= (byte) (1 << randomIntBetween(0, 7));

        ManifestFormatException corrupt = expectThrows(
            ManifestFormatException.class,
            () -> CommitManifestCodec.fromBytes(blob, "manifest-7-42")
        );
        assertTrue(corrupt.getMessage(), corrupt.getMessage().contains("checksum mismatch"));
    }

    /** A future build's manifest must be refused loudly, not silently mis-parsed as this build's. */
    public void testAnUnknownFormatVersionIsRejected() throws Exception {
        byte[] blob = CommitManifestCodec.toBytes(manifest());
        blob[7] = (byte) (CommitManifestCodec.FORMAT_VERSION + 1);
        ManifestFormatException wrongVersion = expectThrows(
            ManifestFormatException.class,
            () -> CommitManifestCodec.fromBytes(blob, "manifest-7-42")
        );
        assertTrue(wrongVersion.getMessage(), wrongVersion.getMessage().contains("unsupported format version"));
    }

    /**
     * A corrupted body-length field must be rejected on the bound, before it is used to size or
     * slice anything -- the same fail-closed guard BundleReader applies to its entry count.
     */
    public void testAnImpossibleBodyLengthIsRejectedBeforeItIsUsed() throws Exception {
        byte[] blob = CommitManifestCodec.toBytes(manifest());
        blob[8] = 0x7F;
        blob[9] = (byte) 0xFF;
        blob[10] = (byte) 0xFF;
        blob[11] = (byte) 0xFF;
        ManifestFormatException impossible = expectThrows(
            ManifestFormatException.class,
            () -> CommitManifestCodec.fromBytes(blob, "manifest-7-42")
        );
        assertTrue(impossible.getMessage(), impossible.getMessage().contains("impossible for a blob"));
    }

    /**
     * Manifests written before the envelope existed are still sitting in every object store this
     * plugin has ever run against, and must keep being readable -- the read path sniffs the magic
     * and falls back to a bare body. The sniff cannot misfire because a bare body's first byte is
     * the vInt length of a 22-character index UUID, which is 0x16 and never 'S'.
     */
    public void testAPreEnvelopeManifestIsStillReadable() throws Exception {
        CommitManifest original = manifest();
        BytesStreamOutput bare = new BytesStreamOutput();
        original.writeTo(bare);
        byte[] legacy = BytesReference.toBytes(bare.bytes());

        assertFalse("a bare body must not look enveloped", CommitManifestCodec.isEnveloped(legacy));
        // A real OpenSearch index UUID is always 22 characters (UUIDs.randomBase64UUID), so a bare
        // body's first byte -- the vInt length of that string -- is always 0x16, and never 'S'.
        assertEquals(0x16, legacy[0] & 0xFF);

        CommitManifest read = CommitManifestCodec.fromBytes(legacy, "manifest-7-42");
        assertEquals(original.generation(), read.generation());
        assertEquals(original.files(), read.files());
        assertEquals(original.segmentsFileName(), read.segmentsFileName());
    }
}
