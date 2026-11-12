/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.compress.LZ4;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * How a bucket manifest's bytes are compressed. Its id is written in the manifest's header, so a reader decodes any of
 * them whatever its own setting, and the one a writer uses can change without a migration.
 *
 * <p>Every codec here ships with the server already: deflate with the JDK, zstd with the compress library the server
 * depends on, LZ4 with Lucene.
 */
public enum ManifestCodec {
    /** Uncompressed: the header and the compact JSON, for reading in a console after stripping the header. */
    NONE(0) {
        @Override
        byte[] compress(byte[] raw) {
            return raw;
        }

        @Override
        byte[] decompress(byte[] packed, int rawLength) {
            return packed;
        }
    },
    /** Deflate at its fastest level: gzip's algorithm without gzip's framing. */
    DEFLATE(1) {
        @Override
        byte[] compress(byte[] raw) {
            final Deflater deflater = new Deflater(Deflater.BEST_SPEED, true);
            try {
                deflater.setInput(raw);
                deflater.finish();
                final ByteArrayOutputStream out = new ByteArrayOutputStream(raw.length / 4 + 64);
                final byte[] buffer = new byte[16 * 1024];
                while (deflater.finished() == false) {
                    out.write(buffer, 0, deflater.deflate(buffer));
                }
                return out.toByteArray();
            } finally {
                deflater.end();
            }
        }

        @Override
        byte[] decompress(byte[] packed, int rawLength) throws IOException {
            final Inflater inflater = new Inflater(true);
            try {
                inflater.setInput(packed);
                final byte[] raw = new byte[rawLength];
                int at = 0;
                while (at < rawLength && inflater.finished() == false) {
                    final int n = inflater.inflate(raw, at, rawLength - at);
                    if (n == 0 && (inflater.needsInput() || inflater.needsDictionary())) {
                        throw new IOException("truncated deflate bucket manifest");
                    }
                    at += n;
                }
                return raw;
            } catch (DataFormatException e) {
                throw new IOException("malformed deflate bucket manifest", e);
            } finally {
                inflater.end();
            }
        }
    },
    /** Zstandard at level 1. */
    ZSTD(2) {
        @Override
        byte[] compress(byte[] raw) {
            return com.github.luben.zstd.Zstd.compress(raw, 1);
        }

        @Override
        byte[] decompress(byte[] packed, int rawLength) throws IOException {
            try {
                return com.github.luben.zstd.Zstd.decompress(packed, rawLength);
            } catch (RuntimeException e) {
                throw new IOException("malformed zstd bucket manifest", e);
            }
        }
    },
    /** Lucene's LZ4, fast table. */
    LZ4_FAST(3) {
        @Override
        byte[] compress(byte[] raw) throws IOException {
            final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
            LZ4.compress(raw, 0, raw.length, out, new LZ4.FastCompressionHashTable());
            return out.toArrayCopy();
        }

        @Override
        byte[] decompress(byte[] packed, int rawLength) throws IOException {
            final byte[] raw = new byte[rawLength];
            try {
                LZ4.decompress(new ByteArrayDataInput(packed), rawLength, raw, 0);
            } catch (RuntimeException e) {
                throw new IOException("malformed lz4 bucket manifest", e);
            }
            return raw;
        }
    };

    private final int id;

    ManifestCodec(int id) {
        this.id = id;
    }

    /** @return the id written in a manifest's header */
    public int id() {
        return id;
    }

    abstract byte[] compress(byte[] raw) throws IOException;

    abstract byte[] decompress(byte[] packed, int rawLength) throws IOException;

    /**
     * Returns the codec a header names.
     *
     * @param id the header's codec id
     * @return the codec
     * @throws IOException if no codec has that id: a manifest written by a newer node
     */
    static ManifestCodec ofId(int id) throws IOException {
        for (ManifestCodec codec : values()) {
            if (codec.id == id) {
                return codec;
            }
        }
        throw new IOException("bucket manifest compressed with unknown codec " + id);
    }

    /**
     * Returns the codec a setting names.
     *
     * @param name the setting's value, any case
     * @return the codec
     * @throws IllegalArgumentException if no codec has that name
     */
    public static ManifestCodec of(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }
}
