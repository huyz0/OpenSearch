/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.fs;

import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.io.Channels;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Compare-and-swap registers on a local filesystem, backing the register methods of {@link FsBlobContainer}.
 *
 * <p>A register file holds an 8-byte generation followed by the value. Every published register file is
 * immutable: a write stages a new file and atomically moves it over the old one. A reader therefore needs no
 * lock, since an open file stays one consistent version through a replacement or an unlink.
 *
 * <p>Writers serialise on a stable sidecar lock file in a reserved {@link #LOCK_DIRECTORY} next to the
 * register, never on the data file itself, whose inode changes on every replacement. {@link FileChannel} locks
 * exclude other processes, but overlapping locks taken in one JVM throw rather than wait, so a bounded set of
 * in-process mutex stripes provides the local exclusion. There are at most {@value #STRIPE_COUNT} lock files per
 * directory rather than one per register. Separate files are needed because on some systems closing any channel
 * on a file releases every lock this process holds on it. Every process sharing a filesystem register must use
 * this protocol. The lock directory stays behind when a register is deleted.
 */
final class FsBlobRegisters {

    /** Reserved directory holding the stable lock files; excluded from container listings and blob names. */
    static final String LOCK_DIRECTORY = ".opensearch-register-locks#";

    private static final String STAGED_FILE_PREFIX = "pending-register-";
    private static final int STRIPE_COUNT = 64;
    private static final ReentrantLock[] STRIPES = new ReentrantLock[STRIPE_COUNT];
    static {
        for (int i = 0; i < STRIPE_COUNT; i++) {
            STRIPES[i] = new ReentrantLock();
        }
    }

    private FsBlobRegisters() {}

    /** Whether any component of {@code path} is the reserved lock directory. */
    static boolean isLockPath(Path path) {
        for (Path part : path) {
            if (part.toString().equals(LOCK_DIRECTORY)) {
                return true;
            }
        }
        return false;
    }

    static Optional<BlobRegister> read(Path registerPath) throws IOException {
        try (FileChannel channel = FileChannel.open(registerPath, StandardOpenOption.READ)) {
            final long size = channel.size();
            if (size == 0L) {
                return Optional.empty();
            }
            if (size < Long.BYTES || size > Integer.MAX_VALUE) {
                throw new IOException("invalid register length " + size + " at " + registerPath);
            }
            final ByteBuffer buffer = ByteBuffer.wrap(Channels.readFromFileChannel(channel, 0, (int) size));
            final long generation = buffer.getLong();
            final byte[] value = new byte[buffer.remaining()];
            buffer.get(value);
            return Optional.of(new BlobRegister(generation, new BytesArray(value)));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }

    static BlobRegisterCasResult compareAndSwap(Path registerPath, long expectedGeneration, BytesReference newValue) throws IOException {
        Files.createDirectories(registerPath.getParent());
        return underLock(registerPath, () -> {
            final long generation = read(registerPath).map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
            if (generation != expectedGeneration) {
                return BlobRegisterCasResult.conflict(generation);
            }
            final long next = generation == BlobRegister.ABSENT_GENERATION ? BlobRegister.initialGeneration() : generation + 1;
            write(registerPath, next, newValue);
            return BlobRegisterCasResult.applied(next);
        });
    }

    static boolean deleteIfUnchanged(Path registerPath, long expectedGeneration) throws IOException {
        try {
            Files.readAttributes(registerPath, BasicFileAttributes.class);
            return underLock(registerPath, () -> {
                final Optional<BlobRegister> current = read(registerPath);
                if (current.isEmpty() || current.get().generation() != expectedGeneration) {
                    return false;
                }
                Files.delete(registerPath);
                IOUtils.fsync(registerPath.getParent(), true);
                return true;
            });
        } catch (NoSuchFileException e) {
            return false;
        }
    }

    /** Runs {@code work} holding both the in-process stripe and the file lock for this register. */
    private static <T> T underLock(Path registerPath, CheckedSupplier<T, IOException> work) throws IOException {
        // The canonical parent keeps aliases of one directory on the same stripe.
        final Path parent = registerPath.getParent().toRealPath();
        final int stripe = Math.floorMod(parent.resolve(registerPath.getFileName()).toString().hashCode(), STRIPE_COUNT);
        final Path lockDirectory = parent.resolve(LOCK_DIRECTORY);
        Files.createDirectories(lockDirectory);
        final ReentrantLock lock = STRIPES[stripe];
        lock.lock();
        try (
            FileChannel channel = FileChannel.open(
                lockDirectory.resolve(Integer.toString(stripe)),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
            );
            FileLock held = channel.lock()
        ) {
            assert held != null;
            return work.get();
        } finally {
            lock.unlock();
        }
    }

    private static void write(Path registerPath, long generation, BytesReference value) throws IOException {
        final byte[] valueBytes = BytesReference.toBytes(value);
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + valueBytes.length);
        buffer.putLong(generation);
        buffer.put(valueBytes);
        // Staged in the lock directory so the staged name does not depend on the register name's length,
        // which may already be at the filesystem's limit.
        final Path staged = registerPath.getParent().resolve(LOCK_DIRECTORY).resolve(STAGED_FILE_PREFIX + UUIDs.randomBase64UUID());
        try {
            try (FileChannel channel = FileChannel.open(staged, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                Channels.writeToChannel(buffer.array(), channel, 0);
                channel.force(true);
            }
            Files.move(staged, registerPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            IOUtils.fsync(registerPath.getParent(), true);
        } finally {
            Files.deleteIfExists(staged);
        }
    }
}
