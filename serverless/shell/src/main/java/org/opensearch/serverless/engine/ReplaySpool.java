/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.engine;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OfflineSorter;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.store.WalRecord;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.zip.CRC32C;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * A disposable local copy of fenced history. Payloads are read from object storage once; replay,
 * collision validation and legacy ordering use local disk with a fixed sorting buffer. No durable
 * format changes: failure discards this directory and leaves the original WAL available for retry.
 */
public final class ReplaySpool implements AutoCloseable {
    @FunctionalInterface
    public interface Source {
        void read(CheckedConsumer<WalRecord, IOException> consumer) throws IOException;
    }

    private final Path path;
    private final Path records;
    private int count;
    private int modernCount;
    private long highest = -1L;
    private boolean legacy;
    private boolean mixed;
    private boolean closed;
    private boolean increasing = true;
    private long previousSeqNo = Long.MIN_VALUE;
    private long skippedChecksum;

    private ReplaySpool(Path parent) throws IOException {
        Files.createDirectories(parent);
        path = Files.createTempDirectory(parent, "wal-replay-");
        records = path.resolve("records");
    }

    /** Caller must hold the shard's exclusive engine lock; stale files belong to an interrupted open. */
    public static ReplaySpool captureExclusive(Path ownedDirectory, Source source) throws IOException {
        IOUtils.rm(ownedDirectory);
        return capture(ownedDirectory, source);
    }

    public static ReplaySpool capture(Path parent, Source source) throws IOException {
        final ReplaySpool spool = new ReplaySpool(parent);
        boolean success = false;
        try {
            spool.write(source);
            spool.validate();
            success = true;
            return spool;
        } finally {
            if (success == false) {
                spool.close();
            }
        }
    }

    private void write(Source source) throws IOException {
        final var checksum = new CRC32C();
        try (
            var directory = FSDirectory.open(path);
            var output = new DataOutputStream(new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(records)), checksum));
            IndexOutput sequenceOutput = directory.createOutput("sequences", IOContext.DEFAULT);
            var sequences = new OfflineSorter.ByteSequencesWriter(sequenceOutput)
        ) {
            final long[] offset = { 0L };
            source.read(record -> {
                if (count == Integer.MAX_VALUE) {
                    throw new IOException("too many operations in WAL recovery");
                }
                final byte[] body = record.isDeletion() ? new byte[0] : record.source().getBytes(StandardCharsets.UTF_8);
                final long size = 1L + 3L * Long.BYTES + Integer.BYTES + 2L * record.id().length() + 2L * Integer.BYTES + body.length;
                if (size > Integer.MAX_VALUE - Integer.BYTES) {
                    throw new IOException("WAL operation exceeds local replay frame size");
                }
                final int length = (int) size;
                output.writeInt(length);
                checksum.reset();
                output.writeByte((record.hasSequenceIdentity() ? 1 : 0) | (record.isDeletion() ? 2 : 0));
                output.writeLong(record.seqNo());
                output.writeLong(record.primaryTerm());
                output.writeLong(record.version());
                output.writeInt(record.id().length());
                output.writeChars(record.id());
                output.writeInt(body.length);
                output.write(body);
                output.writeInt((int) checksum.getValue());
                if (record.hasSequenceIdentity()) {
                    final byte[] key = new byte[20];
                    putLong(key, 0, record.seqNo());
                    putLong(key, 8, offset[0]);
                    putInt(key, 16, length + Integer.BYTES);
                    sequences.write(key);
                    increasing &= record.seqNo() > previousSeqNo;
                    previousSeqNo = record.seqNo();
                    modernCount++;
                    highest = Math.max(highest, record.seqNo());
                    mixed |= legacy;
                } else {
                    legacy = true;
                }
                count++;
                offset[0] += length + (long) Integer.BYTES;
            });
            CodecUtil.writeFooter(sequenceOutput);
        }
    }

    private void validate() throws IOException {
        try (var directory = FSDirectory.open(path); var data = directory.openInput("records", IOContext.READONCE)) {
            if (modernCount > 1 && increasing == false) {
                final String sorted = sorter(directory, Comparator.comparingLong(key -> getLong(key, 0))).sort("sequences");
                try (
                    var sortedInput = directory.openChecksumInput(sorted);
                    var reader = new OfflineSorter.ByteSequencesReader(sortedInput, sorted)
                ) {
                    byte[] previous = null;
                    final byte[] left = new byte[8192];
                    final byte[] right = new byte[8192];
                    for (BytesRef key; (key = reader.next()) != null;) {
                        if (previous != null && getLong(new BytesRef(previous), 0) == getLong(key, 0)) {
                            final BytesRef earlier = new BytesRef(previous);
                            if (sameFrame(data, earlier, key, left, right) == false) {
                                throw new IOException("two acknowledged operations share a sequence number: seqNo " + getLong(key, 0));
                            }
                        }
                        previous = Arrays.copyOfRange(key.bytes, key.offset, key.offset + key.length);
                    }
                    CodecUtil.checkFooter(sortedInput);
                }
                directory.deleteFile(sorted);
            }
            directory.deleteFile("sequences");
            if (mixed) {
                prepareLegacy(directory, data);
            }
        }
    }

    private static OfflineSorter sorter(FSDirectory directory, Comparator<BytesRef> comparator) {
        return new OfflineSorter(directory, "sort", comparator, OfflineSorter.BufferSize.megabytes(16), 2, -1, null, 0);
    }

    private static boolean sameFrame(IndexInput data, BytesRef a, BytesRef b, byte[] left, byte[] right) throws IOException {
        final int length = getInt(a, 16);
        if (length != getInt(b, 16)) {
            return false;
        }
        for (int done = 0; done < length;) {
            final int bytes = Math.min(left.length, length - done);
            data.seek(getLong(a, 8) + done);
            data.readBytes(left, 0, bytes);
            data.seek(getLong(b, 8) + done);
            data.readBytes(right, 0, bytes);
            if (Arrays.equals(left, 0, bytes, right, 0, bytes) == false) {
                return false;
            }
            done += bytes;
        }
        return true;
    }

    private void prepareLegacy(FSDirectory directory, IndexInput data) throws IOException {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        try (
            IndexOutput idsOutput = directory.createOutput("ids", IOContext.DEFAULT);
            var ids = new OfflineSorter.ByteSequencesWriter(idsOutput)
        ) {
            final byte[] idBuffer = new byte[8192];
            data.seek(0L);
            for (int position = 0; position < count; position++) {
                final long offset = data.getFilePointer();
                final int length = Integer.reverseBytes(data.readInt());
                final int flags = (data.readByte() & 255);
                data.skipBytes(3 * Long.BYTES);
                final int chars = Integer.reverseBytes(data.readInt());
                digest.reset();
                for (long remaining = 2L * chars; remaining > 0;) {
                    final int bytes = (int) Math.min(idBuffer.length, remaining);
                    data.readBytes(idBuffer, 0, bytes);
                    digest.update(idBuffer, 0, bytes);
                    remaining -= bytes;
                }
                final byte[] key = new byte[49];
                System.arraycopy(digest.digest(), 0, key, 0, 32);
                putLong(key, 32, offset);
                putLong(key, 40, position);
                key[48] = (byte) flags;
                ids.write(key);
                data.seek(offset + Integer.BYTES + length);
            }
            CodecUtil.writeFooter(idsOutput);
        }
        final Comparator<BytesRef> comparator = (a, b) -> {
            final int hash = Arrays.compareUnsigned(a.bytes, a.offset, a.offset + 32, b.bytes, b.offset, b.offset + 32);
            if (hash != 0) {
                return hash;
            }
            try {
                // The digest is only an accelerator. Exact UTF-16 IDs disambiguate even hash collisions
                // and IDs longer than OfflineSorter's per-key limit; payloads never enter the sort buffer.
                final int id = idAt(data, getLong(a, 32)).compareTo(idAt(data, getLong(b, 32)));
                return id != 0 ? id : Long.compare(getLong(b, 40), getLong(a, 40));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        final String sorted;
        try {
            sorted = sorter(directory, comparator).sort("ids");
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        try (
            var sortedInput = directory.openChecksumInput(sorted);
            var reader = new OfflineSorter.ByteSequencesReader(sortedInput, sorted);
            var skipped = Files.newByteChannel(
                path.resolve("skipped"),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
            )
        ) {
            final ByteBuffer bitsBuffer = ByteBuffer.allocate(1);
            skipped.position((count + 7L) / 8L - 1L);
            while (bitsBuffer.hasRemaining()) {
                skipped.write(bitsBuffer);
            }
            byte[] lastHash = null;
            String lastId = null;
            boolean newerModern = false;
            for (BytesRef key; (key = reader.next()) != null;) {
                final String id = idAt(data, getLong(key, 32));
                final byte[] hash = Arrays.copyOfRange(key.bytes, key.offset, key.offset + 32);
                if (Arrays.equals(lastHash, hash) == false || id.equals(lastId) == false) {
                    newerModern = false;
                    lastHash = hash;
                    lastId = id;
                }
                if ((key.bytes[key.offset + 48] & 1) != 0) {
                    newerModern = true;
                } else if (newerModern) {
                    final long position = getLong(key, 40);
                    skipped.position(position >>> 3);
                    bitsBuffer.clear();
                    while (bitsBuffer.hasRemaining()) {
                        if (skipped.read(bitsBuffer) < 0) {
                            throw new IOException("truncated WAL replay bitmap");
                        }
                    }
                    final int bits = bitsBuffer.get(0) & 255;
                    bitsBuffer.clear();
                    bitsBuffer.put((byte) (bits | (1 << (position & 7)))).flip();
                    skipped.position(position >>> 3);
                    while (bitsBuffer.hasRemaining()) {
                        skipped.write(bitsBuffer);
                    }
                }
            }
            CodecUtil.checkFooter(sortedInput);
        }
        try (
            var checksumInput = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(path.resolve("skipped"))), new CRC32C())
        ) {
            final byte[] buffer = new byte[8192];
            while (checksumInput.read(buffer) != -1) {
                // Consume the complete bitmap through the checksum stream.
            }
            skippedChecksum = checksumInput.getChecksum().getValue();
        }
        directory.deleteFile(sorted);
        directory.deleteFile("ids");
    }

    private static String idAt(IndexInput data, long offset) throws IOException {
        data.seek(offset + Integer.BYTES + 1 + 3 * Long.BYTES);
        final int chars = Integer.reverseBytes(data.readInt());
        final byte[] bytes = new byte[Math.multiplyExact(chars, Character.BYTES)];
        data.readBytes(bytes, 0, bytes.length);
        return ByteBuffer.wrap(bytes).asCharBuffer().toString();
    }

    private FrameInput reader() throws IOException {
        return new FrameInput(Files.newInputStream(records), new CRC32C());
    }

    private static final class FrameInput extends DataInputStream {
        private final CRC32C checksum;
        private final byte[] skippedBytes = new byte[8192];

        FrameInput(InputStream input, CRC32C checksum) {
            super(new CheckedInputStream(new BufferedInputStream(input), checksum));
            this.checksum = checksum;
        }

        int begin() throws IOException {
            final int length = readInt();
            checksum.reset();
            return length;
        }

        void verify() throws IOException {
            final int actual = (int) checksum.getValue();
            if (readInt() != actual) {
                throw new IOException("WAL replay spool checksum mismatch");
            }
        }

        void skipFrame(int length) throws IOException {
            for (long remaining = length - 1L - Integer.BYTES; remaining > 0;) {
                final int bytes = (int) Math.min(skippedBytes.length, remaining);
                readFully(skippedBytes, 0, bytes);
                remaining -= bytes;
            }
            verify();
        }
    }

    public int modernCount() {
        return modernCount;
    }

    public long highestSeqNo() {
        return highest;
    }

    public Translog.Snapshot snapshot() throws IOException {
        final var input = reader();
        return new Translog.Snapshot() {
            private int remaining = count;

            @Override
            public int totalOperations() {
                return modernCount;
            }

            @Override
            public Translog.Operation next() throws IOException {
                while (remaining-- > 0) {
                    final int length = input.begin();
                    final int flags = input.readUnsignedByte();
                    if ((flags & 1) == 0) {
                        input.skipFrame(length);
                        continue;
                    }
                    final long seqNo = input.readLong();
                    final long term = input.readLong();
                    final long version = input.readLong();
                    final String id = readId(input);
                    final byte[] source = new byte[input.readInt()];
                    input.readFully(source);
                    input.verify();
                    return (flags & 2) != 0
                        ? new Translog.Delete(id, seqNo, term, version)
                        : new Translog.Index(id, seqNo, term, version, source, null, -1);
                }
                return null;
            }

            @Override
            public void close() throws IOException {
                input.close();
            }
        };
    }

    public void forEachLegacy(CheckedConsumer<WalRecord, IOException> consumer) throws IOException {
        if (legacy == false) {
            return;
        }
        try (
            var input = reader();
            var skipped = mixed
                ? new CheckedInputStream(new BufferedInputStream(Files.newInputStream(path.resolve("skipped"))), new CRC32C())
                : null
        ) {
            int bits = 0;
            for (int position = 0; position < count; position++) {
                if (skipped != null && (position & 7) == 0) {
                    bits = skipped.read();
                    if (bits < 0) {
                        throw new IOException("truncated WAL replay bitmap");
                    }
                }
                final int length = input.begin();
                final int flags = input.readUnsignedByte();
                if ((flags & 1) != 0 || (skipped != null && (bits & (1 << (position & 7))) != 0)) {
                    input.skipFrame(length);
                } else {
                    consumer.accept(readRecord(input, flags));
                }
            }
            if (skipped != null && skipped.getChecksum().getValue() != skippedChecksum) {
                throw new IOException("WAL replay bitmap checksum mismatch");
            }
        }
    }

    private static WalRecord readRecord(FrameInput input, int flags) throws IOException {
        final long seqNo = input.readLong();
        final long term = input.readLong();
        final long version = input.readLong();
        final String id = readId(input);
        final byte[] source = new byte[input.readInt()];
        input.readFully(source);
        input.verify();
        return (flags & 2) != 0
            ? WalRecord.deletion(id, seqNo, term, version)
            : new WalRecord(id, new String(source, StandardCharsets.UTF_8), seqNo, term, version);
    }

    private static String readId(DataInputStream input) throws IOException {
        final char[] id = new char[input.readInt()];
        for (int i = 0; i < id.length; i++) {
            id[i] = input.readChar();
        }
        return new String(id);
    }

    private static long getLong(BytesRef key, int start) {
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value = (value << 8) | (key.bytes[key.offset + start + i] & 255L);
        }
        return value;
    }

    private static int getInt(BytesRef key, int start) {
        int value = 0;
        for (int i = 0; i < Integer.BYTES; i++) {
            value = (value << 8) | (key.bytes[key.offset + start + i] & 255);
        }
        return value;
    }

    private static void putLong(byte[] key, int start, long value) {
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            key[start + i] = (byte) value;
            value >>>= 8;
        }
    }

    private static void putInt(byte[] key, int start, int value) {
        for (int i = Integer.BYTES - 1; i >= 0; i--) {
            key[start + i] = (byte) value;
            value >>>= 8;
        }
    }

    @Override
    public void close() throws IOException {
        if (closed == false) {
            closed = true;
            IOUtils.rm(path);
        }
    }
}
