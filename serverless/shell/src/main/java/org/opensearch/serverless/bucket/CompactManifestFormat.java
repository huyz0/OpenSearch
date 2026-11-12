/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.PruningDigest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The compact encoding of a bucket manifest: binary, every repeated string written once, then compressed.
 *
 * <p>A member's JSON entry spent most of its bytes saying the same things again: every file repeated its pack's full
 * path (node, epoch, zero-padded ordinal), every file name was written twice, once more for its length, and the length
 * repeated what the location already held. Here the bucket's strings -- pack paths, file name stems and extensions,
 * writers -- are listed once, and a file is a stem, an extension, a pack and a range, each a varint.
 *
 * <p>A header names the format and the codec, so a reader decodes whatever a writer chose and the choice can change
 * without a migration; a manifest without the header is the JSON one, which every node still reads.
 *
 * <pre>
 * 'O' 'S' 'B' 'M' | format (1 byte) | codec id (1 byte) | uncompressed length (4 bytes) | compressed body
 * body: bucket id, term, string table, member index (uuid and entry length each), member entries
 * entry: index name, version, then for a commit: term, writer, wal ordinal + 1, stage sequence + 1, files,
 *         lengths no location gives, digest JSON
 * packs: the member's packs once each, a node pack as node, epoch and ordinal deltas, any other prefix as a string
 * file: stem, extension, tag, then a pack, offset and length, or a whole location; tag low bits say how its length
 *       is known
 * </pre>
 */
final class CompactManifestFormat {

    static final byte[] MAGIC = new byte[] { 'O', 'S', 'B', 'M' };
    static final int FORMAT = 2;
    private static final int HEADER = MAGIC.length + 2 + 4;

    /** The location is a prefix alone, such as a segment blob's term directory. */
    private static final int LOCATION_PREFIX = 0;
    /** The location is a prefix, then '#', then a canonical offset:length range. */
    private static final int LOCATION_RANGE = 1;
    /** The location is written whole: a shape this encoding does not take apart. */
    private static final int LOCATION_RAW = 2;

    /** No length is recorded for the file. */
    private static final int LENGTH_NONE = 0;
    /** The file's length is its range's length. */
    private static final int LENGTH_OF_RANGE = 1;
    /** The file's length follows. */
    private static final int LENGTH_GIVEN = 2;

    private CompactManifestFormat() {}

    static boolean isCompact(byte[] bytes) {
        if (bytes.length < MAGIC.length) {
            return false;
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (bytes[i] != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    /** Interns strings into the table a body is written with. */
    private static final class Strings {
        final Map<String, Integer> ids = new HashMap<>();
        final List<String> table = new ArrayList<>();

        int id(String s) {
            return ids.computeIfAbsent(s, k -> {
                table.add(k);
                return table.size() - 1;
            });
        }
    }

    /** A file's location taken apart. */
    private record Location(int tag, String prefix, long offset, long length) {
        static Location of(String encoded) {
            final int hash = encoded.lastIndexOf('#');
            if (hash < 0) {
                return encoded.indexOf(':') < 0 ? new Location(LOCATION_PREFIX, encoded, 0, 0) : raw(encoded);
            }
            final int colon = encoded.indexOf(':', hash);
            if (colon < 0) {
                return raw(encoded);
            }
            try {
                final String off = encoded.substring(hash + 1, colon);
                final String len = encoded.substring(colon + 1);
                final long offset = Long.parseLong(off);
                final long length = Long.parseLong(len);
                // Only a canonical range is taken apart, so writing it back gives the same string.
                if (offset < 0 || length < 0 || Long.toString(offset).equals(off) == false || Long.toString(length).equals(len) == false) {
                    return raw(encoded);
                }
                return new Location(LOCATION_RANGE, encoded.substring(0, hash), offset, length);
            } catch (NumberFormatException e) {
                return raw(encoded);
            }
        }

        private static Location raw(String encoded) {
            return new Location(LOCATION_RAW, encoded, 0, 0);
        }

        String encode() {
            return tag == LOCATION_RANGE ? prefix + "#" + offset + ":" + length : prefix;
        }
    }

    private static byte[] body(BucketManifest manifest) throws IOException {
        final Strings strings = new Strings();
        // Each member on its own first: the string table is complete only once every member has been through it.
        final List<byte[]> entries = new ArrayList<>(manifest.size());
        for (BucketManifest.Member member : manifest.members().values()) {
            final BytesStreamOutput entry = new BytesStreamOutput();
            entry.writeString(member.indexName());
            entry.writeVLong(member.version());
            entry.writeBoolean(member.commit() != null);
            if (member.commit() != null) {
                writeCommit(member.commit(), strings, entry);
            }
            entries.add(BytesReference.toBytes(entry.bytes()));
        }
        final BytesStreamOutput body = new BytesStreamOutput();
        body.writeString(manifest.bucketId());
        body.writeVLong(manifest.term());
        body.writeVInt(strings.table.size());
        for (String s : strings.table) {
            body.writeString(s);
        }
        // The index: every member's uuid and entry length, so a reader after one member skips the others unparsed.
        body.writeVInt(entries.size());
        int i = 0;
        for (String uuid : manifest.members().keySet()) {
            body.writeString(uuid);
            body.writeVInt(entries.get(i++).length);
        }
        for (byte[] entry : entries) {
            body.writeBytes(entry);
        }
        return BytesReference.toBytes(body.bytes());
    }

    /** Below this many bytes a manifest is written uncompressed: what a codec saves is less than its call costs. */
    static final int AUTO_MIN_BYTES = 32 * 1024;

    /** A compressed body is kept only if it is at most this fraction of the uncompressed one. */
    static final double AUTO_MAX_RATIO = 0.8;

    static BytesReference encode(BucketManifest manifest, ManifestCodec codec) throws IOException {
        final byte[] raw = body(manifest);
        return frame(codec, raw, codec.compress(raw));
    }

    /**
     * Encodes a manifest compressed only when that pays: from {@link #AUTO_MIN_BYTES} up, and only if the codec takes
     * off at least a fifth. Each GET of a member's manifest moves the whole register, so above that size the bytes saved
     * cost more time on the wire than decompressing them does; below it, or when the saving is small, the body is left
     * as it is, where a reader after one member reads it without copying anything.
     */
    static BytesReference encodeAuto(BucketManifest manifest, ManifestCodec codec) throws IOException {
        final byte[] raw = body(manifest);
        if (codec != ManifestCodec.NONE && raw.length >= AUTO_MIN_BYTES) {
            final byte[] packed = codec.compress(raw);
            if (packed.length <= raw.length * AUTO_MAX_RATIO) {
                return frame(codec, raw, packed);
            }
        }
        return frame(ManifestCodec.NONE, raw, raw);
    }

    private static BytesReference frame(ManifestCodec codec, byte[] raw, byte[] packed) {
        final byte[] out = new byte[HEADER + packed.length];
        System.arraycopy(MAGIC, 0, out, 0, MAGIC.length);
        out[MAGIC.length] = (byte) FORMAT;
        out[MAGIC.length + 1] = (byte) codec.id();
        final int length = raw.length;
        out[MAGIC.length + 2] = (byte) (length >>> 24);
        out[MAGIC.length + 3] = (byte) (length >>> 16);
        out[MAGIC.length + 4] = (byte) (length >>> 8);
        out[MAGIC.length + 5] = (byte) length;
        System.arraycopy(packed, 0, out, HEADER, packed.length);
        return new BytesArray(out);
    }

    /** A pack a member's files are in: a node pack taken apart, or any other location prefix as a string. */
    private record Pack(String node, long epoch, long ordinal, String prefix) {
        static Pack of(String prefix) {
            // @<node>/e=<epoch>/<ordinal>, kept apart only if putting it back together gives the same string.
            if (prefix.startsWith("@")) {
                final int nodeEnd = prefix.indexOf('/');
                final int slash = prefix.lastIndexOf('/');
                if (nodeEnd > 1 && slash > nodeEnd + 3 && prefix.startsWith("e=", nodeEnd + 1)) {
                    try {
                        final Pack pack = new Pack(
                            prefix.substring(1, nodeEnd),
                            Long.parseLong(prefix.substring(nodeEnd + 3, slash)),
                            Long.parseLong(prefix.substring(slash + 1)),
                            null
                        );
                        if (pack.encode().equals(prefix)) {
                            return pack;
                        }
                    } catch (IllegalArgumentException e) {
                        // not a node pack this encoding knows: kept whole
                    }
                }
            }
            return new Pack(null, 0, 0, prefix);
        }

        String encode() {
            if (prefix != null) {
                return prefix;
            }
            final String location = org.opensearch.serverless.store.FileLocation.nodePacked(node, epoch, ordinal, 0, 0).encode();
            return location.substring(0, location.lastIndexOf('#'));
        }
    }

    private static void writeCommit(CommitManifest commit, Strings strings, BytesStreamOutput out) throws IOException {
        out.writeVLong(commit.term());
        out.writeVInt(commit.writer() == null ? 0 : strings.id(commit.writer()) + 1);
        out.writeVLong(commit.walOrdinal() + 1);
        out.writeVLong(commit.stageSequence() + 1);
        final Map<String, String> files = new TreeMap<>(commit.files());
        final Map<String, Long> lengths = commit.lengths();
        final Map<String, Location> locations = new LinkedHashMap<>();
        final Map<String, Integer> packIds = new LinkedHashMap<>();
        for (Map.Entry<String, String> file : files.entrySet()) {
            final Location location = Location.of(file.getValue());
            locations.put(file.getKey(), location);
            if (location.tag() == LOCATION_RANGE) {
                packIds.putIfAbsent(location.prefix(), packIds.size());
            }
        }
        // The member's packs once each; node packs as node, epoch and ordinal, each a delta from the pack before.
        out.writeVInt(packIds.size());
        long epoch = 0;
        long ordinal = 0;
        for (String prefix : packIds.keySet()) {
            final Pack pack = Pack.of(prefix);
            if (pack.prefix() != null) {
                out.writeVInt(strings.id(pack.prefix()) << 1);
            } else {
                out.writeVInt(strings.id(pack.node()) << 1 | 1);
                out.writeZLong(pack.epoch() - epoch);
                out.writeZLong(pack.ordinal() - ordinal);
                epoch = pack.epoch();
                ordinal = pack.ordinal();
            }
        }
        out.writeVInt(files.size());
        for (Map.Entry<String, Location> file : locations.entrySet()) {
            final String name = file.getKey();
            final int dot = name.indexOf('.');
            out.writeVInt(strings.id(dot < 0 ? name : name.substring(0, dot)));
            out.writeVInt(strings.id(dot < 0 ? "" : name.substring(dot)));
            final Location location = file.getValue();
            final Long length = lengths.get(name);
            final int lengthTag = length == null ? LENGTH_NONE
                : location.tag() == LOCATION_RANGE && length == location.length() ? LENGTH_OF_RANGE
                : LENGTH_GIVEN;
            out.writeVInt(location.tag() << 2 | lengthTag);
            if (location.tag() == LOCATION_RANGE) {
                out.writeVInt(packIds.get(location.prefix()));
                out.writeVLong(location.offset());
                out.writeVLong(location.length());
            } else {
                out.writeVInt(strings.id(location.prefix()));
            }
            if (lengthTag == LENGTH_GIVEN) {
                out.writeVLong(length);
            }
        }
        // Lengths of files the commit does not name: none today, kept so nothing is lost if one ever is.
        final Map<String, Long> extra = new TreeMap<>();
        for (Map.Entry<String, Long> length : lengths.entrySet()) {
            if (files.containsKey(length.getKey()) == false) {
                extra.put(length.getKey(), length.getValue());
            }
        }
        out.writeVInt(extra.size());
        for (Map.Entry<String, Long> length : extra.entrySet()) {
            out.writeString(length.getKey());
            out.writeVLong(length.getValue());
        }
        if (commit.digest().isEmpty()) {
            out.writeVInt(0);
        } else {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                commit.digest().toXContent(builder);
                final BytesReference digest = BytesReference.bytes(builder);
                out.writeVInt(digest.length());
                digest.writeTo(out);
            }
        }
    }

    /** A decompressed body, read as far as its member index. */
    private record Body(String bucketId, long term, String[] strings, String[] uuids, int[] lengths, StreamInput entries) {
    }

    private static Body open(byte[] bytes) throws IOException {
        if (bytes.length < HEADER || isCompact(bytes) == false) {
            throw new IOException("not a compact bucket manifest");
        }
        final int format = bytes[MAGIC.length] & 0xFF;
        if (format != FORMAT) {
            throw new IOException("bucket manifest in unknown format " + format);
        }
        final ManifestCodec codec = ManifestCodec.ofId(bytes[MAGIC.length + 1] & 0xFF);
        final int rawLength = (bytes[MAGIC.length + 2] & 0xFF) << 24 | (bytes[MAGIC.length + 3] & 0xFF) << 16 | (bytes[MAGIC.length + 4]
            & 0xFF) << 8 | (bytes[MAGIC.length + 5] & 0xFF);
        if (rawLength < 0) {
            throw new IOException("malformed bucket manifest: negative length");
        }
        // Uncompressed, the body is read where it lies: a member's entry is then an offset away, nothing copied.
        final StreamInput in = codec == ManifestCodec.NONE
            ? new BytesArray(bytes, HEADER, bytes.length - HEADER).streamInput()
            : new BytesArray(codec.decompress(java.util.Arrays.copyOfRange(bytes, HEADER, bytes.length), rawLength)).streamInput();
        final String bucketId = in.readString();
        final long term = in.readVLong();
        final String[] strings = new String[in.readVInt()];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = in.readString();
        }
        final int count = in.readVInt();
        final String[] uuids = new String[count];
        final int[] lengths = new int[count];
        for (int i = 0; i < count; i++) {
            uuids[i] = in.readString();
            lengths[i] = in.readVInt();
        }
        return new Body(bucketId, term, strings, uuids, lengths, in);
    }

    private static BucketManifest.Member readMember(StreamInput in, String[] strings) throws IOException {
        final String indexName = in.readString();
        final long version = in.readVLong();
        final CommitManifest commit = in.readBoolean() ? readCommit(in, strings) : null;
        return new BucketManifest.Member(indexName, commit, version);
    }

    static BucketManifest decode(byte[] bytes) throws IOException {
        try {
            final Body body = open(bytes);
            final Map<String, BucketManifest.Member> members = new TreeMap<>();
            for (String uuid : body.uuids()) {
                members.put(uuid, readMember(body.entries(), body.strings()));
            }
            return new BucketManifest(body.bucketId(), body.term(), members);
        } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
            throw new IOException("malformed bucket manifest", e);
        }
    }

    /**
     * Decodes one member's entry, skipping every other member's bytes unparsed: what a reader opening one member needs.
     */
    static java.util.Optional<BucketManifest.Member> decodeMember(byte[] bytes, String indexUuid) throws IOException {
        try {
            final Body body = open(bytes);
            for (int i = 0; i < body.uuids().length; i++) {
                if (body.uuids()[i].equals(indexUuid)) {
                    return java.util.Optional.of(readMember(body.entries(), body.strings()));
                }
                body.entries().skipNBytes(body.lengths()[i]);
            }
            return java.util.Optional.empty();
        } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
            throw new IOException("malformed bucket manifest", e);
        }
    }

    private static CommitManifest readCommit(StreamInput in, String[] strings) throws IOException {
        final long term = in.readVLong();
        final int writerId = in.readVInt();
        final String writer = writerId == 0 ? null : strings[writerId - 1];
        final long walOrdinal = in.readVLong() - 1;
        final long stageSequence = in.readVLong() - 1;
        final String[] packs = new String[in.readVInt()];
        long epoch = 0;
        long ordinal = 0;
        for (int i = 0; i < packs.length; i++) {
            final int ref = in.readVInt();
            if ((ref & 1) == 0) {
                packs[i] = strings[ref >>> 1];
            } else {
                epoch += in.readZLong();
                ordinal += in.readZLong();
                packs[i] = new Pack(strings[ref >>> 1], epoch, ordinal, null).encode();
            }
        }
        final int fileCount = in.readVInt();
        final Map<String, String> files = new LinkedHashMap<>(fileCount * 2);
        final Map<String, Long> lengths = new LinkedHashMap<>(fileCount * 2);
        for (int i = 0; i < fileCount; i++) {
            final String name = strings[in.readVInt()] + strings[in.readVInt()];
            final int tag = in.readVInt();
            final int locationTag = tag >>> 2;
            final int lengthTag = tag & 3;
            final Location location = locationTag == LOCATION_RANGE
                ? new Location(LOCATION_RANGE, packs[in.readVInt()], in.readVLong(), in.readVLong())
                : new Location(locationTag, strings[in.readVInt()], 0, 0);
            files.put(name, location.encode());
            if (lengthTag == LENGTH_OF_RANGE) {
                lengths.put(name, location.length());
            } else if (lengthTag == LENGTH_GIVEN) {
                lengths.put(name, in.readVLong());
            }
        }
        final int extra = in.readVInt();
        for (int i = 0; i < extra; i++) {
            lengths.put(in.readString(), in.readVLong());
        }
        PruningDigest digest = PruningDigest.EMPTY;
        final int digestLength = in.readVInt();
        if (digestLength > 0) {
            final byte[] json = in.readNBytes(digestLength);
            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)
            ) {
                parser.nextToken();
                digest = PruningDigest.parse(parser);
            }
        }
        return new CommitManifest(term, files, writer, lengths, digest, walOrdinal).withStageSequence(stageSequence);
    }
}
