/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.text.Text;
import org.opensearch.search.SearchHit;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Estimates retained native payloads without allocating another serialized copy. */
final class ResponseMemory {
    private ResponseMemory() {}

    static long of(Writeable value) {
        final var output = new Counter();
        output.bytes = 256L;
        try {
            value.writeTo(output);
            return output.bytes;
        } catch (IOException e) {
            throw new IllegalStateException("could not account for a native response", e);
        }
    }

    /** Additive per-hit weights let a merge give discarded hits back without recounting retained hits. */
    static long hits(List<SearchHit> hits) {
        final var output = new Counter();
        try {
            for (SearchHit hit : hits) {
                output.bytes += 256L;
                hit.writeTo(output);
            }
            return output.bytes;
        } catch (IOException e) {
            throw new IllegalStateException("could not account for search hits", e);
        }
    }

    /** Walk native writers, allowing for strings, collections and object headers rather than only wire bytes. */
    private static final class Counter extends StreamOutput {
        private long bytes;

        @Override
        public void writeByte(byte value) {
            bytes++;
        }

        @Override
        public void writeBytes(byte[] value, int offset, int length) {
            bytes += length;
        }

        @Override
        public void writeString(String value) {
            bytes += 48L + 2L * value.length();
        }

        @Override
        public void writeBytesReference(BytesReference value) {
            bytes += value == null ? 1L : 64L + value.length();
        }

        @Override
        public void writeText(Text value) {
            bytes += 48L + (value.hasBytes() ? 2L * value.bytes().length() : 2L * value.string().length());
        }

        @Override
        public void writeGenericValue(Object value) throws IOException {
            if (value instanceof Map<?, ?> map) {
                bytes += 48L + 48L * map.size();
            } else if (value instanceof Collection<?> collection) {
                bytes += 32L + 16L * collection.size();
            } else if (value instanceof Object[] array) {
                bytes += 32L + 16L * array.length;
            } else if (value != null) {
                bytes += 32L;
            }
            super.writeGenericValue(value);
        }

        @Override
        public <K, V> void writeMap(Map<K, V> map, Writeable.Writer<K> keyWriter, Writeable.Writer<V> valueWriter) throws IOException {
            bytes += 48L + 48L * map.size();
            super.writeMap(map, keyWriter, valueWriter);
        }

        @Override
        public void writeMapWithConsistentOrder(Map<String, ? extends Object> map) throws IOException {
            bytes += map == null ? 1L : 48L + 48L * map.size();
            if (map != null) {
                // Ordering cannot change an estimate; avoid sorting or allocating another entry list.
                for (var entry : map.entrySet()) {
                    writeString(entry.getKey());
                    writeGenericValue(entry.getValue());
                }
            }
        }

        @Override
        public <T> void writeCollection(Collection<T> values, Writeable.Writer<T> writer) throws IOException {
            bytes += 32L + 16L * values.size();
            super.writeCollection(values, writer);
        }

        @Override
        public <T> void writeArray(Writeable.Writer<T> writer, T[] values) throws IOException {
            bytes += 32L + 16L * values.length;
            super.writeArray(writer, values);
        }

        @Override
        public void writeNamedWriteable(NamedWriteable value) throws IOException {
            bytes += 64L;
            super.writeNamedWriteable(value);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}

        @Override
        public void reset() {
            bytes = 0L;
        }
    }
}
