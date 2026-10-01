/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What a shard's published commit consists of: a term, and the blob each segment file lives at.
 *
 * <p>Per-file blob names rather than a single prefix, because after a failover the new writer's term
 * differs from the term that uploaded the files it inherited. Re-uploading unchanged segments just to
 * move them under a new prefix would make every failover proportional to shard size, which is the cost
 * this architecture exists to avoid. So a file keeps the blob it was written to, and the manifest
 * remembers where that is.
 */
public final class CommitManifest {

    private final long term;
    private final Map<String, String> files;
    private final String writer;
    private final Map<String, Long> lengths;
    private final PruningDigest digest;

    /**
     * Creates a manifest whose writer is not recorded.
     *
     * @param term the term of the writer that published it
     * @param files segment file name to the blob it lives at
     */
    public CommitManifest(long term, Map<String, String> files) {
        this(term, files, null);
    }

    /**
     * Creates a manifest.
     *
     * @param term the term of the writer that published it
     * @param files segment file name to the blob it lives at
     * @param writer the node that published it, or null if unrecorded
     */
    public CommitManifest(long term, Map<String, String> files, String writer) {
        this(term, files, writer, Map.of());
    }

    /**
     * Creates a manifest that also records how long each file is.
     *
     * <p>The lengths are the index that replaces a listing: a reader needs every file's length before
     * Lucene will read a byte, and without them it listed each term container on every open. The
     * publisher knows each length as it uploads, so recording them costs nothing beyond the register
     * write the manifest already is. Optional in the format: a manifest without them reads as before,
     * and a reader lists only for what it cannot find here.
     *
     * @param term the writer's term
     * @param files file name to the term container holding it
     * @param writer the writer's node id, or null
     * @param lengths file name to length, for whichever files are known
     */
    public CommitManifest(long term, Map<String, String> files, String writer, Map<String, Long> lengths) {
        this(term, files, writer, lengths, PruningDigest.EMPTY);
    }

    /**
     * Creates a manifest that also carries the commit's pruning digest.
     *
     * <p>The digest rides in the register the manifest already is, so a coordinator that reads it to route
     * a search learns, in the same read, whether the search can match anything here at all. Written last in
     * the JSON: a node from before digests reads term, files and lengths first and stops at the digest's
     * first closing brace, having read everything it knows.
     *
     * @param term the writer's term
     * @param files file name to the term container holding it
     * @param writer the writer's node id, or null
     * @param lengths file name to length, for whichever files are known
     * @param digest the commit's pruning digest, {@link PruningDigest#EMPTY} if none
     */
    public CommitManifest(long term, Map<String, String> files, String writer, Map<String, Long> lengths, PruningDigest digest) {
        this(term, files, writer, lengths, digest, -1L);
    }

    /**
     * Creates a manifest that records how far into the write-ahead log its commit reaches.
     *
     * @param term the publishing writer's term
     * @param files segment file name to blob name
     * @param writer the publishing node, or null if unknown
     * @param lengths file lengths, possibly fewer than files
     * @param digest the commit's pruning digest
     * @param walOrdinal the highest log ordinal of {@code term} the commit holds; negative for unknown
     */
    public CommitManifest(
        long term,
        Map<String, String> files,
        String writer,
        Map<String, Long> lengths,
        PruningDigest digest,
        long walOrdinal
    ) {
        this.term = term;
        this.files = Map.copyOf(files);
        this.writer = writer;
        this.lengths = Map.copyOf(lengths);
        this.digest = digest == null ? PruningDigest.EMPTY : digest;
        this.walOrdinal = walOrdinal;
    }

    /**
     * How far into the write-ahead log this commit reaches: the highest ordinal of its own term whose records it
     * holds, every older term included. Negative when unknown -- a manifest published before this was recorded,
     * or one carried on the wire -- which a reader treats as covering none of its term.
     */
    private final long walOrdinal;

    /**
     * Returns the highest log ordinal of this commit's term that the commit holds.
     *
     * @return the ordinal, or a negative number if this manifest does not record it
     */
    public long walOrdinal() {
        return walOrdinal;
    }

    /**
     * Returns the commit's pruning digest.
     *
     * @return the digest; empty for a manifest published without one
     */
    public PruningDigest digest() {
        return digest;
    }

    /**
     * Returns the recorded lengths, which may cover fewer files than {@link #files()}.
     *
     * @return file name to length
     */
    public Map<String, Long> lengths() {
        return lengths;
    }

    /**
     * Returns one file's length if recorded.
     *
     * @param fileName the file
     * @return the length, or null if this manifest does not record it
     */
    public Long lengthOf(String fileName) {
        return lengths.get(fileName);
    }

    /**
     * Returns the node that published this commit.
     *
     * <p><b>Recorded because a term is not an identity.</b> The publish fence refuses a <em>newer</em>
     * term, which is right for a zombie and says nothing about two writers holding the same term at once.
     * That cannot happen while the shard-head's compare-and-swap behaves — and if it ever does not, this is
     * what turns a commit silently assembled from two nodes' segments into a refusal.
     *
     * <p>Null for a manifest written before this field existed, which is treated as "cannot tell" rather
     * than as a mismatch: refusing every publish onto an older manifest would be a worse failure than the
     * one this guards against.
     *
     * @return the publishing node's id, or null
     */
    public String writer() {
        return writer;
    }

    /**
     * Returns the publishing writer's term.
     *
     * @return the term
     */
    public long term() {
        return term;
    }

    /**
     * Returns segment file name to blob name.
     *
     * @return the file map
     */
    public Map<String, String> files() {
        return files;
    }

    /**
     * Serializes this manifest.
     *
     * @return the register bytes
     * @throws IOException if serialization fails
     */
    public BytesReference toBytes() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("term", term);
            if (writer != null) {
                builder.field("writer", writer);
            }
            builder.startObject("files");
            for (Map.Entry<String, String> e : files.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
            if (lengths.isEmpty() == false) {
                builder.startObject("lengths");
                for (Map.Entry<String, Long> e : lengths.entrySet()) {
                    builder.field(e.getKey(), e.getValue());
                }
                builder.endObject();
            }
            if (walOrdinal >= 0L) {
                // Scalars, which every older parser skips: a field it does not know is ignored, not a failure.
                builder.field("wal_ordinal", walOrdinal);
            }
            if (digest.isEmpty() == false) {
                // Last, always: see the constructor.
                builder.field("digest");
                digest.toXContent(builder);
            }
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Parses a manifest.
     *
     * @param input the serialized manifest
     * @return the parsed manifest
     * @throws IOException if the bytes are malformed
     */
    public static CommitManifest fromStream(InputStream input) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)
        ) {
            long term = -1;
            String writer = null;
            final Map<String, String> files = new LinkedHashMap<>();
            final Map<String, Long> lengths = new LinkedHashMap<>();
            PruningDigest digest = PruningDigest.EMPTY;
            long walOrdinal = -1L;
            String field = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null && token != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && "lengths".equals(field)) {
                    String fileName = null;
                    XContentParser.Token inner;
                    while ((inner = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (inner == XContentParser.Token.FIELD_NAME) {
                            fileName = parser.currentName();
                        } else if (inner.isValue() && fileName != null) {
                            lengths.put(fileName, parser.longValue());
                        }
                    }
                } else if (token == XContentParser.Token.START_OBJECT && "files".equals(field)) {
                    String fileName = null;
                    XContentParser.Token inner;
                    while ((inner = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (inner == XContentParser.Token.FIELD_NAME) {
                            fileName = parser.currentName();
                        } else if (inner.isValue() && fileName != null) {
                            files.put(fileName, parser.text());
                        }
                    }
                } else if (token == XContentParser.Token.START_OBJECT && "digest".equals(field)) {
                    digest = PruningDigest.parse(parser);
                } else if (field != null && (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY)) {
                    // Something a later version added: skipped whole, so its closing brace is not mistaken
                    // for the manifest's own.
                    parser.skipChildren();
                } else if (token.isValue() && "term".equals(field)) {
                    term = parser.longValue();
                } else if (token.isValue() && "writer".equals(field)) {
                    writer = parser.text();
                } else if (token.isValue() && "wal_ordinal".equals(field)) {
                    walOrdinal = parser.longValue();
                }
            }
            if (term < 0) {
                throw new IOException("malformed commit manifest: no term");
            }
            return new CommitManifest(term, files, writer, lengths, digest, walOrdinal);
        }
    }

    /**
     * Reads a manifest off the transport wire.
     *
     * <p>A second, binary encoding beside {@link #toBytes()}'s JSON. The register format is JSON because a
     * register is read by hand sometimes and JSON survives an OpenSearch version change better than a
     * stream version does; the wire format exists only for {@link org.opensearch.serverless.transport}, to
     * carry a manifest to the node a frozen search is forwarded to, where neither concern applies.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public CommitManifest(StreamInput in) throws IOException {
        this.term = in.readVLong();
        this.writer = in.readOptionalString();
        final int count = in.readVInt();
        final Map<String, String> read = new LinkedHashMap<>(count);
        for (int i = 0; i < count; i++) {
            read.put(in.readString(), in.readString());
        }
        this.files = Map.copyOf(read);
        final int known = in.readVInt();
        final Map<String, Long> readLengths = new LinkedHashMap<>(known);
        for (int i = 0; i < known; i++) {
            readLengths.put(in.readString(), in.readVLong());
        }
        this.lengths = Map.copyOf(readLengths);
        // Not carried on the wire: a forwarded frozen search has already been routed.
        this.digest = PruningDigest.EMPTY;
        // Nor this: a frozen view serves the commit it froze on purpose, however far the log has moved since.
        this.walOrdinal = -1L;
    }

    /**
     * Writes this manifest to the transport wire.
     *
     * @param out the stream
     * @throws IOException if writing fails
     */
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(term);
        out.writeOptionalString(writer);
        out.writeVInt(files.size());
        // Sorted, so the same manifest serialises to the same bytes on every node: Map.copyOf iterates in
        // a salted order, and the per-request MAC over a forwarded frozen search digests these bytes on
        // both sides. An order that differed between sender and receiver read as a forged request.
        for (Map.Entry<String, String> file : new java.util.TreeMap<>(files).entrySet()) {
            out.writeString(file.getKey());
            out.writeString(file.getValue());
        }
        out.writeVInt(lengths.size());
        for (Map.Entry<String, Long> length : new java.util.TreeMap<>(lengths).entrySet()) {
            out.writeString(length.getKey());
            out.writeVLong(length.getValue());
        }
    }

    @Override
    public String toString() {
        return "CommitManifest[term=" + term + ", writer=" + writer + ", files=" + files.size() + "]";
    }
}
