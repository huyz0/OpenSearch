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

    /**
     * Creates a manifest.
     *
     * @param term the term of the writer that published it
     * @param files segment file name to the blob it lives at
     */
    public CommitManifest(long term, Map<String, String> files) {
        this.term = term;
        this.files = Map.copyOf(files);
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
                }
            }
            if (term < 0) {
                throw new IOException("malformed commit manifest: no term");
            }
            return new CommitManifest(term, files);
        }
    }

    @Override
    public String toString() {
        return "CommitManifest[term=" + term + ", files=" + files.size() + "]";
    }
}
