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
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * The on-disk envelope around a {@link CommitManifest}'s body, giving the manifest blob the same
 * three protections the plugin's two other persisted formats already have and it alone did not:
 * a magic header, an explicit {@code formatVersion}, and a trailing CRC32C over everything before
 * it. {@code BundleWriter} (magic + version + header checksum + per-file CRC32C) and {@code
 * WalChunkWriter} (magic + version + trailing CRC32C) are the two precedents; this class is
 * deliberately shaped like them rather than inventing a third convention.
 *
 * <p><b>Why a manifest needs this even though every file it names is individually checksummed.</b>
 * {@link FileReference#checksum()} protects the <em>bundle bytes</em> a manifest points at. It does
 * not protect the manifest itself: a flipped bit in the file map's entry count, or in a {@code
 * readString} length prefix, yields either a wildly oversized allocation or -- far worse -- a
 * structurally valid manifest carrying wrong offsets, wrong lengths, or a wrong
 * {@code segmentsFileName}, which is then used verbatim to build the shard's file map. Nothing
 * downstream can catch that, because every per-file checksum it compares against came out of the
 * same corrupted blob. Failing closed here is the only place it can be done.
 *
 * <p><b>Why a version, given manifests are written and read by the same build today.</b> They are
 * not: a manifest blob outlives the process that wrote it by design (that is the entire point of
 * &sect;6.3), and is read by other nodes and by later builds during a rolling upgrade. Adding,
 * reordering, or widening a single field in {@link CommitManifest#writeTo} silently mis-parses
 * every manifest already sitting in the object store -- {@code StreamInput.wrap} carries no {@code
 * Version}, so the usual transport-BWC machinery is not available to rescue it either. The WAL
 * chunk format hit exactly this and handled it by bumping its own {@code FORMAT_VERSION} and
 * rejecting unknown ones; this makes the same move available to the manifest before, rather than
 * after, an incompatible change is needed.
 *
 * <p><b>The read path is deliberately backward compatible.</b> Manifests written before this
 * envelope existed are bare {@link CommitManifest#writeTo} bodies with no header at all, and they
 * are still live in any cluster that has run this plugin. {@link #fromBytes} therefore sniffs the
 * magic and falls back to parsing a bare body when it is absent. That sniff cannot misfire on a
 * legacy blob: a bare body begins with {@code writeString(indexUuid)}, whose first byte is the
 * vInt length of an OpenSearch index UUID -- {@code UUIDs.randomBase64UUID} is always 22
 * characters, so that byte is always {@code 0x16}, and never {@code 'S'} (0x53). The fallback is
 * unchecksummed by construction, which is exactly the pre-existing behaviour for a pre-existing
 * blob; new writes are always enveloped.
 */
public final class CommitManifestCodec {

    /** Marks an enveloped manifest blob: 'S'(erverless) 'C'(ommit) 'M'(anifest) '1'. */
    static final byte[] MAGIC = { 'S', 'C', 'M', '1' };

    /** The only envelope version this build writes, and the only one {@link #fromBytes} accepts. */
    static final int FORMAT_VERSION = 1;

    /** magic(4) + formatVersion(4) + bodyLength(4). */
    private static final int HEADER_LENGTH = MAGIC.length + 4 + 4;

    /** Trailing CRC32C, written as a long so the field width matches the other two formats' checksums. */
    private static final int TRAILER_LENGTH = 8;

    private CommitManifestCodec() {}

    /**
     * Serializes {@code manifest} into its enveloped on-disk form.
     *
     * @param manifest the manifest to serialize.
     * @return magic, format version, body length, body, and a trailing CRC32C over all of the above.
     */
    public static byte[] toBytes(CommitManifest manifest) throws IOException {
        BytesStreamOutput bodyOut = new BytesStreamOutput();
        manifest.writeTo(bodyOut);
        byte[] body = BytesReference.toBytes(bodyOut.bytes());

        byte[] blob = new byte[HEADER_LENGTH + body.length + TRAILER_LENGTH];
        ByteBuffer buffer = ByteBuffer.wrap(blob);
        buffer.put(MAGIC);
        buffer.putInt(FORMAT_VERSION);
        buffer.putInt(body.length);
        buffer.put(body);
        buffer.putLong(checksum(blob, 0, HEADER_LENGTH + body.length));
        return blob;
    }

    /**
     * Parses a manifest blob written by {@link #toBytes}, or -- when the magic is absent -- a bare
     * pre-envelope body (see this class's own javadoc for why that sniff cannot misfire).
     *
     * @param blob the raw manifest blob bytes.
     * @param blobName the blob's name, used only to make a failure message locate itself.
     * @return the parsed manifest.
     * @throws ManifestFormatException if the blob is enveloped but truncated, carries an unknown
     *                                  format version, or fails its own checksum.
     * @throws IOException if the body itself cannot be deserialized.
     */
    public static CommitManifest fromBytes(byte[] blob, String blobName) throws IOException {
        if (isEnveloped(blob) == false) {
            // Pre-envelope blob: parse exactly as this plugin always did. Unchecksummed, because
            // there is no checksum in it to check -- see this class's own javadoc.
            return new CommitManifest(StreamInput.wrap(blob));
        }
        if (blob.length < HEADER_LENGTH + TRAILER_LENGTH) {
            throw new ManifestFormatException("manifest [" + blobName + "] is truncated: only " + blob.length + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.wrap(blob);
        buffer.position(MAGIC.length);
        int version = buffer.getInt();
        if (version != FORMAT_VERSION) {
            throw new ManifestFormatException(
                "manifest [" + blobName + "] has unsupported format version " + version + ", this build understands " + FORMAT_VERSION
            );
        }
        int bodyLength = buffer.getInt();
        // Bounded BEFORE it is used to size or slice anything: a single corrupted bit here (with
        // magic and version intact) would otherwise produce a huge or negative length and either an
        // OutOfMemoryError or a confusing IndexOutOfBoundsException, instead of the clean
        // fail-closed this class promises. The same guard, for the same reason, that
        // BundleReader bounds its entryCount and WalChunkReader bounds its record count with.
        if (bodyLength < 0 || bodyLength > blob.length - HEADER_LENGTH - TRAILER_LENGTH) {
            throw new ManifestFormatException(
                "manifest ["
                    + blobName
                    + "] declares a body of "
                    + bodyLength
                    + " bytes, impossible for a blob of "
                    + blob.length
                    + " bytes"
            );
        }
        long expected = checksum(blob, 0, HEADER_LENGTH + bodyLength);
        buffer.position(HEADER_LENGTH + bodyLength);
        long actual = buffer.getLong();
        // Verified BEFORE the body is deserialized, not after: the whole point is that a corrupted
        // body must never be turned into a structurally valid CommitManifest that a caller then
        // uses to build a shard's file map.
        if (actual != expected) {
            throw new ManifestFormatException(
                "manifest [" + blobName + "] checksum mismatch: expected " + expected + " but computed " + actual + " -- corrupt manifest"
            );
        }
        return new CommitManifest(StreamInput.wrap(blob, HEADER_LENGTH, bodyLength));
    }

    /** Whether {@code blob} starts with this format's magic, i.e. was written by {@link #toBytes}. */
    static boolean isEnveloped(byte[] blob) {
        if (blob.length < MAGIC.length) {
            return false;
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (blob[i] != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    private static long checksum(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return crc.getValue();
    }
}
