/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.annotation.InternalApi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * Persistent identity of a journal storage lineage. A genuinely blank replacement directory receives
 * a new identity; missing/corrupt identity metadata beside existing data is never silently recreated.
 * The owning controller must bind this identity to membership and authorize all bootstrap/recovery.
 * Copying a live voter's directory to another live voter is not a supported recovery operation.
 */
@InternalApi
public final class JournalDiskIdentity {

    private static final String IDENTITY_FILE = "voter-identity";
    private static final String LOCK_FILE = "voter-lock";
    private static final int MAGIC = 0x4c4c4449;
    private static final byte VERSION = 1;
    private static final int MARKER_BYTES = Integer.BYTES + 1 + Long.BYTES * 2 + Integer.BYTES;

    private JournalDiskIdentity() {}

    /**
     * Discovers or initializes a storage identity under an exclusive directory lock. The discovery
     * lock is released before return; the running journal separately acquires and retains its lease.
     * A cached membership identity must be compared against this value before starting a voter.
     *
     * The host must provision and persist the directory itself. A missing directory is refused,
     * rather than recreated on a possibly different filesystem after a failed volume mount.
     *
     * @param directory dedicated, already provisioned durable local journal directory
     * @return persisted storage identity
     * @throws IOException on conflicting ownership, invalid metadata, or failed persistence
     */
    public static UUID discover(Path directory) throws IOException {
        try (Lease lease = acquire(directory)) {
            return lease.id;
        }
    }

    static Lease acquire(Path directory) throws IOException {
        final Path root = prepareDirectory(directory);
        final Path marker = root.resolve(IDENTITY_FILE);
        // Avoid placing even a lock file into an unrelated, nonempty directory.
        if (Files.exists(marker, LinkOption.NOFOLLOW_LINKS) == false) {
            requireUninitialized(root);
        }
        final Path lockPath = root.resolve(LOCK_FILE);
        if (Files.exists(lockPath, LinkOption.NOFOLLOW_LINKS) && Files.isRegularFile(lockPath, LinkOption.NOFOLLOW_LINKS) == false) {
            throw new IOException("journal lock must be a regular file");
        }
        final FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock = null;
        try {
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new IOException("journal directory is already in use: " + root, e);
            }
            if (lock == null) {
                throw new IOException("journal directory is already in use: " + root);
            }
            final UUID id;
            if (Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) {
                id = read(marker);
            } else {
                requireUninitialized(root);
                id = UUID.randomUUID();
                write(root, marker, id);
            }
            // A previous attempt may have renamed the marker but failed its directory sync. Reading
            // an existing valid marker must establish durability too, rather than accepting visibility.
            try (FileChannel identityFile = FileChannel.open(marker, StandardOpenOption.READ)) {
                identityFile.force(true);
            }
            forceDirectory(root);
            return new Lease(root, id, channel, lock);
        } catch (IOException | RuntimeException | Error failure) {
            if (lock != null) {
                try {
                    lock.release();
                } catch (IOException e) {
                    failure.addSuppressed(e);
                }
            }
            try {
                channel.close();
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
            throw failure;
        }
    }

    private static Path prepareDirectory(Path directory) throws IOException {
        final Path absolute = directory.toAbsolutePath().normalize();
        if (absolute.getParent() == null) {
            throw new IOException("a filesystem root cannot be a journal directory");
        }
        if (Files.isDirectory(absolute) == false) {
            throw new IOException("journal directory must already be provisioned: " + absolute);
        }
        return absolute.toRealPath();
    }

    private static void requireUninitialized(Path root) throws IOException {
        try (var files = Files.list(root)) {
            if (files.anyMatch(path -> path.getFileName().toString().equals(LOCK_FILE) == false)) {
                throw new IOException("journal identity is missing from a nonempty directory: " + root);
            }
        }
    }

    private static UUID read(Path marker) throws IOException {
        if (Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS) == false) {
            throw new IOException("journal identity must be a regular file");
        }
        final byte[] bytes;
        try (var in = Files.newInputStream(marker)) {
            bytes = in.readNBytes(MARKER_BYTES + 1);
        }
        if (bytes.length != MARKER_BYTES) {
            throw new IOException("invalid journal identity length");
        }
        final ByteBuffer input = ByteBuffer.wrap(bytes);
        if (input.getInt() != MAGIC || input.get() != VERSION) {
            throw new IOException("unsupported journal identity format");
        }
        final UUID id = new UUID(input.getLong(), input.getLong());
        final var crc = new CRC32C();
        crc.update(bytes, 0, MARKER_BYTES - Integer.BYTES);
        if (input.getInt() != (int) crc.getValue()) {
            throw new IOException("journal identity checksum mismatch");
        }
        return id;
    }

    private static void write(Path root, Path marker, UUID id) throws IOException {
        final ByteBuffer bytes = ByteBuffer.allocate(MARKER_BYTES);
        bytes.putInt(MAGIC).put(VERSION).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
        final var crc = new CRC32C();
        crc.update(bytes.array(), 0, MARKER_BYTES - Integer.BYTES);
        bytes.putInt((int) crc.getValue()).flip();
        final Path temporary = Files.createTempFile(root, "voter-identity-", ".tmp");
        try {
            try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                while (bytes.hasRemaining()) {
                    output.write(bytes);
                }
                output.force(true);
            }
            Files.move(temporary, marker, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    /** Held privately by a running journal until its replication services have stopped. */
    static final class Lease implements AutoCloseable {
        final Path directory;
        final UUID id;
        private final FileChannel channel;
        private final FileLock lock;
        private boolean closed;

        private Lease(Path directory, UUID id, FileChannel channel, FileLock lock) {
            this.directory = directory;
            this.id = id;
            this.channel = channel;
            this.lock = lock;
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                lock.release();
            } finally {
                channel.close();
            }
        }
    }
}
