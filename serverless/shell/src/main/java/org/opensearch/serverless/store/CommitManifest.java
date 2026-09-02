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
        this.term = term;
        this.files = Map.copyOf(files);
        this.writer = writer;
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
            String field = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null && token != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
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
                } else if (token.isValue() && "term".equals(field)) {
                    term = parser.longValue();
                } else if (token.isValue() && "writer".equals(field)) {
                    writer = parser.text();
                }
            }
            if (term < 0) {
                throw new IOException("malformed commit manifest: no term");
            }
            return new CommitManifest(term, files, writer);
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
        for (Map.Entry<String, String> file : files.entrySet()) {
            out.writeString(file.getKey());
            out.writeString(file.getValue());
        }
    }

    @Override
    public String toString() {
        return "CommitManifest[term=" + term + ", writer=" + writer + ", files=" + files.size() + "]";
    }
}
