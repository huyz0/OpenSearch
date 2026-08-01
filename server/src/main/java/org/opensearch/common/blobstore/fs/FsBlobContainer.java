/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.blobstore.fs;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.support.AbstractBlobContainer;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.io.Streams;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.unmodifiableMap;

/**
 * A file system based implementation of {@link org.opensearch.common.blobstore.BlobContainer}.
 * All blobs in the container are stored on a file system, the location of which is specified by the {@link BlobPath}.
 * <p>
 * Note that the methods in this implementation of {@link org.opensearch.common.blobstore.BlobContainer} may
 * additionally throw a {@link java.lang.SecurityException} if the configured {@link java.lang.SecurityManager}
 * does not permit read and/or write access to the underlying files.
 *
 * @opensearch.internal
 */
public class FsBlobContainer extends AbstractBlobContainer {

    private static final String TEMP_FILE_PREFIX = "pending-";

    protected final FsBlobStore blobStore;
    protected final Path path;

    // Guards readRegister/compareAndSwapRegister: FileChannel#lock() is documented as
    // inter-process only, and a second overlapping lock() from a different FileChannel in the
    // *same* JVM throws OverlappingFileLockException rather than blocking, so real intra-process
    // mutual exclusion has to come from here, not from the filesystem.
    //
    // Static, and keyed by the resolved register path rather than by blob name, because the unit of
    // exclusion is the file and not the container that reached it. As a per-instance field keyed by
    // name this arbitrated nothing between two containers over the same directory: both read the
    // same generation, both passed the equality check, both wrote, and the second silently won.
    // Internal cluster tests give every node its own container, so that is the normal case rather
    // than an exotic one, and a test asserting one winner among concurrent CAS callers would have
    // been asserting nothing.
    private static final ConcurrentHashMap<String, ReentrantLock> REGISTER_LOCKS_BY_PATH = new ConcurrentHashMap<>();

    public FsBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
        super(blobPath);
        this.blobStore = blobStore;
        this.path = path;
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        Map<String, BlobContainer> builder = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            for (Path file : stream) {
                if (Files.isDirectory(file)) {
                    final String name = file.getFileName().toString();
                    builder.put(name, new FsBlobContainer(blobStore, path().add(name), file));
                }
            }
        }
        return unmodifiableMap(builder);
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        Map<String, BlobMetadata> builder = new HashMap<>();

        blobNamePrefix = blobNamePrefix == null ? "" : blobNamePrefix;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, blobNamePrefix + "*")) {
            for (Path file : stream) {
                final BasicFileAttributes attrs;
                try {
                    attrs = Files.readAttributes(file, BasicFileAttributes.class);
                } catch (FileNotFoundException | NoSuchFileException e) {
                    // The file was concurrently deleted between listing files and trying to get its attributes so we skip it here
                    continue;
                }
                if (attrs.isRegularFile()) {
                    builder.put(file.getFileName().toString(), new PlainBlobMetadata(file.getFileName().toString(), attrs.size()));
                }
            }
        }
        return unmodifiableMap(builder);
    }

    @Override
    public DeleteResult delete() throws IOException {
        final AtomicLong filesDeleted = new AtomicLong(0L);
        final AtomicLong bytesDeleted = new AtomicLong(0L);
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException impossible) throws IOException {
                assert impossible == null;
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                filesDeleted.incrementAndGet();
                bytesDeleted.addAndGet(attrs.size());
                return FileVisitResult.CONTINUE;
            }
        });
        return new DeleteResult(filesDeleted.get(), bytesDeleted.get());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        IOUtils.rm(blobNames.stream().map(path::resolve).toArray(Path[]::new));
    }

    @Override
    public boolean blobExists(String blobName) {
        return Files.exists(path.resolve(blobName));
    }

    @Override
    public InputStream readBlob(String name) throws IOException {
        final Path resolvedPath = path.resolve(name);
        try {
            return Files.newInputStream(resolvedPath);
        } catch (FileNotFoundException fnfe) {
            throw new NoSuchFileException("[" + name + "] blob not found");
        }
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        final SeekableByteChannel channel = Files.newByteChannel(path.resolve(blobName));
        if (position > 0L) {
            channel.position(position);
        }
        assert channel.position() == position;
        return Streams.limitStream(Channels.newInputStream(channel), length);
    }

    @Override
    public long readBlobPreferredLength() {
        // This container returns streams that are cheap to close early, so we can tell consumers to request as much data as possible.
        return Long.MAX_VALUE;
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        final Path file = path.resolve(blobName);
        try {
            writeToPath(inputStream, file, blobSize);
        } catch (FileAlreadyExistsException faee) {
            if (failIfAlreadyExists) {
                throw faee;
            }
            deleteBlobsIgnoringIfNotExists(Collections.singletonList(blobName));
            writeToPath(inputStream, file, blobSize);
        }
        IOUtils.fsync(path, true);
    }

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        @Nullable Map<String, String> metadata,
        @Nullable CryptoMetadata cryptoMetadata
    ) throws IOException {
        // FsBlobContainer doesn't handle metadata or encryption
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(final String blobName, final InputStream inputStream, final long blobSize, boolean failIfAlreadyExists)
        throws IOException {
        final String tempBlob = tempBlobName(blobName);
        final Path tempBlobPath = path.resolve(tempBlob);
        try {
            writeToPath(inputStream, tempBlobPath, blobSize);
            moveBlobAtomic(tempBlob, blobName, failIfAlreadyExists);
        } catch (IOException ex) {
            try {
                deleteBlobsIgnoringIfNotExists(Collections.singletonList(tempBlob));
            } catch (IOException e) {
                ex.addSuppressed(e);
            }
            throw ex;
        } finally {
            IOUtils.fsync(path, true);
        }
    }

    private void writeToPath(InputStream inputStream, Path tempBlobPath, long blobSize) throws IOException {
        Files.createDirectories(path);
        try (OutputStream outputStream = Files.newOutputStream(tempBlobPath, StandardOpenOption.CREATE_NEW)) {
            final int bufferSize = blobStore.bufferSizeInBytes();
            org.opensearch.common.util.io.Streams.copy(
                inputStream,
                outputStream,
                new byte[blobSize < bufferSize ? Math.toIntExact(blobSize) : bufferSize]
            );
        }
        IOUtils.fsync(tempBlobPath, false);
    }

    public void moveBlobAtomic(final String sourceBlobName, final String targetBlobName, final boolean failIfAlreadyExists)
        throws IOException {
        final Path sourceBlobPath = path.resolve(sourceBlobName);
        final Path targetBlobPath = path.resolve(targetBlobName);
        // If the target file exists then Files.move() behaviour is implementation specific
        // the existing file might be replaced or this method fails by throwing an IOException.
        if (Files.exists(targetBlobPath)) {
            if (failIfAlreadyExists) {
                throw new FileAlreadyExistsException("blob [" + targetBlobPath + "] already exists, cannot overwrite");
            } else {
                deleteBlobsIgnoringIfNotExists(Collections.singletonList(targetBlobName));
            }
        }
        Files.move(sourceBlobPath, targetBlobPath, StandardCopyOption.ATOMIC_MOVE);
    }

    public static String tempBlobName(final String blobName) {
        return "pending-" + blobName + "-" + UUIDs.randomBase64UUID();
    }

    /**
     * Returns true if the blob is a leftover temporary blob.
     * <p>
     * The temporary blobs might be left after failed atomic write operation.
     */
    public static boolean isTempBlobName(final String blobName) {
        return blobName.startsWith(TEMP_FILE_PREFIX);
    }

    @Override
    public Optional<BlobRegister> readRegister(String blobName) throws IOException {
        Path registerPath = path.resolve(blobName);
        if (Files.exists(registerPath) == false) {
            return Optional.empty();
        }
        ReentrantLock lock = registerLockFor(registerPath);
        lock.lock();
        try (FileChannel channel = FileChannel.open(registerPath, StandardOpenOption.READ)) {
            // Shared, because the channel is read-only and an exclusive lock on it would throw. Enough
            // to exclude a writer in another process, which is all this adds over the lock above.
            try (FileLock fileLock = channel.lock(0L, Long.MAX_VALUE, true)) {
                assert fileLock != null;
                return readRegisterUnderLock(channel);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
        throws IOException {
        Path registerPath = path.resolve(blobName);
        Files.createDirectories(path);

        ReentrantLock lock = registerLockFor(registerPath);
        lock.lock();
        try (
            FileChannel channel = FileChannel.open(
                registerPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
            )
        ) {
            // Held across the read and the write, so the compare and the swap are one step. Taken only
            // after the intra-process lock above, which is what keeps this from throwing
            // OverlappingFileLockException when two containers in one JVM reach the same file.
            try (FileLock fileLock = channel.lock()) {
                assert fileLock != null;
                Optional<BlobRegister> current = readRegisterUnderLock(channel);
                long currentGeneration = current.map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
                if (currentGeneration != expectedGeneration) {
                    return BlobRegisterCasResult.conflict(currentGeneration);
                }

                long newGeneration = currentGeneration + 1;
                writeRegisterUnderLock(channel, newGeneration, newValue);
                return BlobRegisterCasResult.applied(newGeneration);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * The intra-process lock for one register file.
     *
     * <p>Keyed by the absolute normalised path so two containers that reach the same file through
     * different {@link BlobPath}s share a lock, and two containers over different directories with the
     * same blob name do not.
     */
    private static ReentrantLock registerLockFor(Path registerPath) {
        return REGISTER_LOCKS_BY_PATH.computeIfAbsent(registerPath.toAbsolutePath().normalize().toString(), ignored -> new ReentrantLock());
    }

    private Optional<BlobRegister> readRegisterUnderLock(FileChannel channel) throws IOException {
        long size = channel.size();
        if (size == 0) {
            return Optional.empty();
        }
        byte[] bytes = org.opensearch.common.io.Channels.readFromFileChannel(channel, 0, (int) size);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long generation = buffer.getLong();
        byte[] value = new byte[buffer.remaining()];
        buffer.get(value);
        return Optional.of(new BlobRegister(generation, new BytesArray(value)));
    }

    private void writeRegisterUnderLock(FileChannel channel, long newGeneration, BytesReference newValue) throws IOException {
        byte[] valueBytes = BytesReference.toBytes(newValue);
        ByteBuffer buffer = ByteBuffer.allocate(8 + valueBytes.length);
        buffer.putLong(newGeneration);
        buffer.put(valueBytes);
        buffer.flip();

        channel.truncate(0);
        org.opensearch.common.io.Channels.writeToChannel(buffer.array(), channel, 0);
        channel.force(true);
    }
}
