/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.zip.CRC32C;

/** Fixed-size local boundary evidence. A corrupt slot fences recovery rather than hiding a newer floor. */
final class JournalDurableFloor implements Closeable {
    private static final int MAGIC = 0x4a464c52;
    private static final int VERSION = 1;
    private static final int RECORD_BYTES = 76;
    private static final int FILE_BYTES = 2 * RECORD_BYTES;
    private final FileChannel channel;
    private final UUID deployment;
    private final UUID voter;
    private final UUID group;
    private long sequence;
    private long index;
    private IOException failed;

    private JournalDurableFloor(FileChannel channel, UUID deployment, UUID voter, UUID group, long sequence, long index) {
        this.channel = channel;
        this.deployment = deployment;
        this.voter = voter;
        this.group = group;
        this.sequence = sequence;
        this.index = index;
    }

    static JournalDurableFloor create(Path path, UUID deployment, UUID voter, UUID group) throws IOException {
        final var channel = FileChannel.open(
            path,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            LinkOption.NOFOLLOW_LINKS
        );
        try {
            final var floor = new JournalDurableFloor(channel, deployment, voter, group, 0, -1);
            final byte[] initial = floor.encode(0, -1);
            writeFully(channel, initial, 0);
            writeFully(channel, initial, RECORD_BYTES);
            channel.force(true);
            JournalDiskIdentity.forceDirectory(path.getParent());
            return floor;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    static JournalDurableFloor open(Path path, UUID deployment, UUID voter, UUID group) throws IOException {
        final var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        try {
            if (channel.size() != FILE_BYTES) {
                throw new IOException("journal durable floor has an invalid size");
            }
            final var bytes = ByteBuffer.allocate(FILE_BYTES);
            while (bytes.hasRemaining()) {
                if (channel.read(bytes) < 0) {
                    throw new IOException("truncated journal durable floor");
                }
            }
            final var floor = new JournalDurableFloor(channel, deployment, voter, group, 0, -1);
            final var first = floor.decode(bytes.array(), 0);
            final var second = floor.decode(bytes.array(), RECORD_BYTES);
            if ((first.sequence & 1) != 0 || (second.sequence != 0 && (second.sequence & 1) != 1)) {
                throw new IOException("journal durable floor generation is stored in the wrong slot");
            }
            if (first.sequence == second.sequence) {
                if (first.sequence != 0 || first.index != -1 || second.index != -1) {
                    throw new IOException("journal durable floor has duplicate non-initial generations");
                }
            } else if (Math.max(first.sequence, second.sequence) - Math.min(first.sequence, second.sequence) != 1) {
                throw new IOException("journal durable floor generations are not adjacent");
            }
            final var latest = first.sequence >= second.sequence ? first : second;
            floor.sequence = latest.sequence;
            floor.index = latest.index;
            // A valid record can still be dirty page-cache data from an interrupted force.
            // Re-establish persistence even when recovery observes the same boundary index.
            channel.force(true);
            JournalDiskIdentity.forceDirectory(path.getParent());
            return floor;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    synchronized long index() {
        return index;
    }

    /** Must be serialized with the native log worker, including legitimate suffix truncation. */
    synchronized void persist(long nextIndex) throws IOException {
        if (channel.isOpen() == false) {
            throw new IOException("journal durable floor is closed");
        }
        if (failed != null) {
            throw new IOException("journal durable floor is fenced after an I/O failure", failed);
        }
        if (nextIndex < -1 || sequence == Long.MAX_VALUE) {
            throw new IOException("invalid or exhausted journal durable floor");
        }
        if (nextIndex == index) {
            return;
        }
        final long nextSequence = sequence + 1;
        try {
            writeFully(channel, encode(nextSequence, nextIndex), (nextSequence & 1) * RECORD_BYTES);
            channel.force(true);
            sequence = nextSequence;
            index = nextIndex;
        } catch (IOException failure) {
            failed = failure;
            throw failure;
        }
    }

    private record Boundary(long sequence, long index) {
    }

    private byte[] encode(long nextSequence, long nextIndex) {
        final var frame = ByteBuffer.allocate(RECORD_BYTES);
        frame.putInt(MAGIC).putInt(VERSION);
        writeUuid(frame, deployment);
        writeUuid(frame, voter);
        writeUuid(frame, group);
        frame.putLong(nextSequence).putLong(nextIndex);
        final var crc = new CRC32C();
        crc.update(frame.array(), 0, RECORD_BYTES - Integer.BYTES);
        frame.putInt((int) crc.getValue());
        return frame.array();
    }

    private Boundary decode(byte[] bytes, int offset) throws IOException {
        final var crc = new CRC32C();
        crc.update(bytes, offset, RECORD_BYTES - Integer.BYTES);
        final var frame = ByteBuffer.wrap(bytes, offset, RECORD_BYTES);
        if (frame.getInt() != MAGIC
            || frame.getInt() != VERSION
            || deployment.equals(readUuid(frame)) == false
            || voter.equals(readUuid(frame)) == false
            || group.equals(readUuid(frame)) == false) {
            throw new IOException("journal durable floor format or lineage mismatch");
        }
        final long storedSequence = frame.getLong();
        final long storedIndex = frame.getLong();
        if (frame.getInt() != (int) crc.getValue() || storedSequence < 0 || storedIndex < -1) {
            throw new IOException("journal durable floor checksum or boundary mismatch");
        }
        return new Boundary(storedSequence, storedIndex);
    }

    private static void writeUuid(ByteBuffer frame, UUID uuid) {
        frame.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
    }

    private static UUID readUuid(ByteBuffer frame) {
        return new UUID(frame.getLong(), frame.getLong());
    }

    private static void writeFully(FileChannel channel, byte[] bytes, long offset) throws IOException {
        final var buffer = ByteBuffer.wrap(bytes);
        long position = offset;
        while (buffer.hasRemaining()) {
            position += channel.write(buffer, position);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }
}
