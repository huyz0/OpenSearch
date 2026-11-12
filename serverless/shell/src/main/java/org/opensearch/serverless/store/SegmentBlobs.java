/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** How a segment blob is laid out: its header, then its files, smallest first, so an open's first reads sit together. */
final class SegmentBlobs {

    private SegmentBlobs() {}

    /** A segment blob laid out but not yet written: its name, header, files in blob order and their sizes. */
    record Laid(String name, byte[] header, java.util.List<String> files, Map<String, Long> sizes, long length) {
    }

    static Laid prepare(Directory directory, String blob, java.util.List<String> fileNames) throws IOException {
        final Map<String, Long> sizes = new LinkedHashMap<>();
        for (String fileName : fileNames) {
            sizes.put(fileName, directory.fileLength(fileName));
        }
        final java.util.List<String> ordered = new java.util.ArrayList<>(fileNames);
        ordered.sort(java.util.Comparator.<String>comparingLong(sizes::get).thenComparing(java.util.Comparator.naturalOrder()));
        final byte[] header = header(ordered, sizes);
        long length = header.length;
        for (String fileName : ordered) {
            length += sizes.get(fileName);
        }
        return new Laid(blob, header, java.util.List.copyOf(ordered), sizes, length);
    }

    /** Opens a laid-out segment blob's bytes from the first, holding its files open until the stream is closed. */
    static java.io.InputStream open(Directory directory, Laid blob) throws IOException {
        final java.util.List<IndexInput> opened = new java.util.ArrayList<>();
        boolean success = false;
        try {
            final java.util.List<PackInputStream.Part> parts = new java.util.ArrayList<>();
            long offset = blob.header().length;
            for (String fileName : blob.files()) {
                final IndexInput input = directory.openInput(fileName, IOContext.READONCE);
                opened.add(input);
                if (input.length() != blob.sizes().get(fileName)) {
                    throw new IOException("[" + fileName + "] changed length while it was being published");
                }
                parts.add(new PackInputStream.Part(input, offset, input.length()));
                offset += input.length();
            }
            final PackInputStream stream = new PackInputStream(blob.header(), parts);
            success = true;
            return new java.io.FilterInputStream(stream) {
                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        org.opensearch.common.util.io.IOUtils.close(opened);
                    }
                }
            };
        } finally {
            if (success == false) {
                org.opensearch.common.util.io.IOUtils.closeWhileHandlingException(opened);
            }
        }
    }

    /** A blob name for a segment that cannot take its plain name: its files are already published, or the name is taken. */
    static String uniqueName(String segment) {
        return segment + "-" + org.opensearch.common.UUIDs.randomBase64UUID() + ".seg";
    }

    /**
     * A segment blob's header: {@code SEG1}, the length of what follows as four bytes, then JSON naming each file's
     * offset from the start of the blob and its length.
     */
    static byte[] header(java.util.List<String> ordered, Map<String, Long> sizes) throws IOException {
        // The offsets count the header itself, whose length depends on how many digits they take: settle it by
        // encoding until the length stops moving, which a second or third pass always does.
        byte[] encoded = new byte[0];
        for (int pass = 0; pass < 8; pass++) {
            long offset = 8L + encoded.length;
            try (org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                builder.startObject().field("version", 1).startObject("files");
                for (String fileName : ordered) {
                    builder.startArray(fileName).value(offset).value(sizes.get(fileName)).endArray();
                    offset += sizes.get(fileName);
                }
                builder.endObject().endObject();
                final byte[] next = org.opensearch.core.common.bytes.BytesReference.toBytes(
                    org.opensearch.core.common.bytes.BytesReference.bytes(builder)
                );
                if (next.length == encoded.length) {
                    final java.nio.ByteBuffer header = java.nio.ByteBuffer.allocate(8 + next.length);
                    header.put(SEGMENT_BLOB_MAGIC).putInt(next.length).put(next);
                    return header.array();
                }
                encoded = next;
            }
        }
        throw new IOException("a segment blob's header did not settle");
    }

    private static final byte[] SEGMENT_BLOB_MAGIC = { 'S', 'E', 'G', '1' };
}
