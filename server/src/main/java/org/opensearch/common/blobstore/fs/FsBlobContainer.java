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
import org.opensearch.common.blobstore.BlobVersionConflictException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.VersionedBlob;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
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
    /** Stable lock files, reserved for registers and excluded from container listings. */
    private static final String REGISTER_LOCK_DIRECTORY = ".opensearch-register-locks#";

    protected final FsBlobStore blobStore;
    protected final Path path;

    // FileChannel locks exclude other processes, but overlapping locks in one JVM throw rather than
    // wait. Bounded mutex stripes provide that local exclusion without retaining one lock per key.
    // Writers lock stable sidecar files; the data inode itself changes on every atomic replacement.
    // Published register files are immutable, so readers need only open one consistent version.
    private static final int REGISTER_LOCK_STRIPE_COUNT = 64;
    private static final ReentrantLock[] REGISTER_LOCK_STRIPES = new ReentrantLock[REGISTER_LOCK_STRIPE_COUNT];
    static {
        for (int i = 0; i < REGISTER_LOCK_STRIPE_COUNT; i++) {
            REGISTER_LOCK_STRIPES[i] = new ReentrantLock();
        }
    }

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
                if (file.getFileName().toString().equals(REGISTER_LOCK_DIRECTORY) == false && Files.isDirectory(file)) {
                    final String name = file.getFileName().toString();
                    builder.put(name, new FsBlobContainer(blobStore, path().add(name), file));
                }
            }
        }
        return unmodifiableMap(builder);
    }

    @Override
    public Map<String, BlobContainer> children(String startAfter, int limit) throws IOException {
        if (limit < 0) {
            throw new IllegalArgumentException("limit should not be a negative value");
        }
        // In key order, as every store lists child prefixes; see BlobContainer#children(String, int).
        final Comparator<String> order = CHILD_KEY_ORDER;
        final PriorityQueue<String> page = new PriorityQueue<>(Math.max(1, Math.min(limit, 1024)), order.reversed());
        if (limit > 0) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path file : stream) {
                    final String name = file.getFileName().toString();
                    if (name.equals(REGISTER_LOCK_DIRECTORY)
                        || (startAfter != null && order.compare(name, startAfter) <= 0)
                        || (page.size() == limit && order.compare(name, page.peek()) >= 0)
                        || Files.isDirectory(file) == false) {
                        continue;
                    }
                    page.add(name);
                    if (page.size() > limit) {
                        page.poll();
                    }
                }
            } catch (NoSuchFileException e) {
                return new LinkedHashMap<>();
            }
        }
        final List<String> names = new ArrayList<>(page);
        names.sort(order);
        final Map<String, BlobContainer> children = new LinkedHashMap<>();
        for (String name : names) {
            children.put(name, new FsBlobContainer(blobStore, path().add(name), path.resolve(name)));
        }
        return children;
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
                } catch (java.nio.file.AccessDeniedException e) {
                    // Windows reports a file that is being deleted as access denied rather than missing; a temporary
                    // blob is deleted as soon as it is published, so it is skipped the way a deleted one is.
                    if (isTempBlobName(file.getFileName().toString())) {
                        continue;
                    }
                    throw e;
                }
                if (attrs.isRegularFile()) {
                    if (file.getFileName().toString().equals(REGISTER_LOCK_DIRECTORY) == false) {
                        builder.put(file.getFileName().toString(), new PlainBlobMetadata(file.getFileName().toString(), attrs.size()));
                    }
                }
            }
        }
        return unmodifiableMap(builder);
    }

    /**
     * A sorted walk of the directory that only ever holds {@code limit} names: the smallest names after the
     * cursor, kept in a bounded heap as the directory streams past.
     */
    @Override
    public List<BlobMetadata> listBlobsByPrefix(String blobNamePrefix, String startAfter, int limit) throws IOException {
        if (limit < 0) {
            throw new IllegalArgumentException("limit should not be a negative value");
        }
        final String prefix = blobNamePrefix == null ? "" : blobNamePrefix;
        // The largest kept name at the head, so a smaller one can displace it.
        final PriorityQueue<BlobMetadata> page = new PriorityQueue<>(
            Math.max(1, Math.min(limit, 1024)),
            BlobNameSortOrder.LEXICOGRAPHIC.comparator().reversed()
        );
        if (limit == 0) {
            return new ArrayList<>();
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            for (Path file : stream) {
                final String name = file.getFileName().toString();
                if (name.equals(REGISTER_LOCK_DIRECTORY)
                    || name.startsWith(prefix) == false
                    || (startAfter != null && name.compareTo(startAfter) <= 0)) {
                    continue;
                }
                if (page.size() == limit && name.compareTo(page.peek().name()) >= 0) {
                    continue;
                }
                final BasicFileAttributes attrs;
                try {
                    attrs = Files.readAttributes(file, BasicFileAttributes.class);
                } catch (FileNotFoundException | NoSuchFileException e) {
                    continue;
                } catch (java.nio.file.AccessDeniedException e) {
                    // A temporary blob mid-delete on Windows; see listBlobsByPrefix.
                    if (isTempBlobName(name)) {
                        continue;
                    }
                    throw e;
                }
                if (attrs.isRegularFile()) {
                    page.add(new PlainBlobMetadata(name, attrs.size()));
                    if (page.size() > limit) {
                        page.poll();
                    }
                }
            }
        } catch (NoSuchFileException e) {
            return new ArrayList<>();
        }
        final List<BlobMetadata> sorted = new ArrayList<>(page);
        sorted.sort(BlobNameSortOrder.LEXICOGRAPHIC.comparator());
        return sorted;
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
                if (file.getParent().getFileName().toString().equals(REGISTER_LOCK_DIRECTORY) == false) {
                    filesDeleted.incrementAndGet();
                    bytesDeleted.addAndGet(attrs.size());
                }
                return FileVisitResult.CONTINUE;
            }
        });
        // Mirror the delete in the conditional-write version map, so that a blob recreated at the same path later is
        // a new version: any token issued before the delete must lose its CAS, as it would against a real object
        // store's ETag semantics. The trailing separator is load-bearing: without it, deleting the container at
        // /repo/idx-1 would also invalidate live tokens under a sibling like /repo/idx-10, spuriously fencing an
        // unrelated shard.
        final String containerPrefix = path.toAbsolutePath().toString() + path.getFileSystem().getSeparator();
        CONDITIONAL_WRITE_VERSIONS.keySet().removeIf(key -> key.startsWith(containerPrefix));
        return new DeleteResult(filesDeleted.get(), bytesDeleted.get());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        IOUtils.rm(blobNames.stream().map(this::resolveBlobPath).toArray(Path[]::new));
        // See delete(): deletion invalidates any previously issued version token for these paths.
        blobNames.forEach(blobName -> CONDITIONAL_WRITE_VERSIONS.remove(conditionalWriteKey(blobName)));
    }

    @Override
    public boolean blobExists(String blobName) {
        return isRegisterCoordinationPath(path.resolve(blobName)) == false && Files.exists(path.resolve(blobName));
    }

    @Override
    public InputStream readBlob(String name) throws IOException {
        final Path resolvedPath = resolveBlobPath(name);
        try {
            return Files.newInputStream(resolvedPath);
        } catch (FileNotFoundException fnfe) {
            throw new NoSuchFileException("[" + name + "] blob not found");
        }
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        final SeekableByteChannel channel = Files.newByteChannel(resolveBlobPath(blobName));
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
        final Path file = resolveBlobPath(blobName);
        if (failIfAlreadyExists) {
            createWhole(blobName, inputStream, blobSize);
        } else {
            try {
                writeToPath(inputStream, file, blobSize);
            } catch (FileAlreadyExistsException faee) {
                deleteBlobsIgnoringIfNotExists(Collections.singletonList(blobName));
                writeToPath(inputStream, file, blobSize);
            }
        }
        IOUtils.fsync(path, true);
    }

    /**
     * Creates a blob only if its name is free, and never lets the name be seen before the content is whole.
     *
     * <p>Writing in place created the name first and filled it afterwards, so a concurrent listing could see the
     * blob at any length from zero up. An object store never shows a partly written object, and callers written
     * against one rely on that: a log that marks a fence with a blob of no bytes read a record still being written
     * as a fence, skipped fencing the term, and let the fenced writer append after a takeover. So the content goes to
     * a temporary blob first and is published under its name by a hard link, which either creates the name with the
     * whole content behind it or fails because the name is taken -- the same answer put-if-absent gives.
     */
    private void createWhole(String blobName, InputStream inputStream, long blobSize) throws IOException {
        final Path target = resolveBlobPath(blobName);
        final Path temp = path.resolve(tempBlobName(blobName));
        writeToPath(inputStream, temp, blobSize);
        try {
            try {
                Files.createLink(target, temp);
            } catch (UnsupportedOperationException e) {
                // A filesystem without hard links: a copy still refuses a taken name, though not atomically.
                Files.copy(temp, target);
            }
        } finally {
            Files.deleteIfExists(temp);
        }
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
        resolveBlobPath(blobName);
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
        final Path sourceBlobPath = resolveBlobPath(sourceBlobName);
        final Path targetBlobPath = resolveBlobPath(targetBlobName);
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

    /**
     * Fixed set of stripes used to emulate compare-and-swap semantics for conditional writes. Object stores provide
     * this atomically server-side; on a local filesystem we serialize readers/writers of the same blob within this
     * JVM. Stripes are keyed by blob path hash, so distinct blobs may share a lock: over-locking only costs
     * contention, whereas a per-blob map keyed by path would grow without bound as shards and indices come and go.
     */
    private static final int CONDITIONAL_WRITE_STRIPES = 64;
    private static final Object[] CONDITIONAL_WRITE_LOCKS = new Object[CONDITIONAL_WRITE_STRIPES];
    static {
        for (int i = 0; i < CONDITIONAL_WRITE_STRIPES; i++) {
            CONDITIONAL_WRITE_LOCKS[i] = new Object();
        }
    }

    private Object conditionalWriteLock(final String blobName) {
        final int hash = conditionalWriteKey(blobName).hashCode();
        // Math.floorMod rather than % so that Integer.MIN_VALUE does not yield a negative index.
        return CONDITIONAL_WRITE_LOCKS[Math.floorMod(hash, CONDITIONAL_WRITE_STRIPES)];
    }

    /**
     * Version tokens for conditionally-written blobs, keyed by absolute path. Tokens are random rather than derived
     * from content: a content hash would make two writes of identical bytes indistinguishable, letting a stale writer
     * that happens to write the current content pass a CAS it should lose (ABA). The map is JVM-local, matching the
     * scope of this emulation, and only ever holds blobs touched through the conditional-write API (fence blobs), so
     * growth is bounded by live conditionally-written blobs rather than by all blobs. Deletions through this
     * container's delete APIs invalidate the token (best effort, mirroring an object store's new-ETag-on-recreate
     * semantics); an <b>overwrite</b> through a non-conditional API remains invisible to the version map, which is
     * acceptable because conditionally-written blobs are, by contract, written exclusively through this API.
     */
    private static final Map<String, String> CONDITIONAL_WRITE_VERSIONS = new ConcurrentHashMap<>();

    private String conditionalWriteKey(final String blobName) {
        return path.resolve(blobName).toAbsolutePath().toString();
    }

    /**
     * The current version token of the blob, or {@code null} if it does not exist. Must be called under the blob's
     * conditional-write lock. A blob that exists on disk but has never been seen by this API (e.g. created before the
     * JVM started) is assigned a fresh token on first observation.
     */
    private String currentVersionToken(final String blobName) {
        final String key = conditionalWriteKey(blobName);
        if (Files.exists(path.resolve(blobName)) == false) {
            CONDITIONAL_WRITE_VERSIONS.remove(key);
            return null;
        }
        return CONDITIONAL_WRITE_VERSIONS.computeIfAbsent(key, k -> UUIDs.randomBase64UUID());
    }

    /**
     * {@inheritDoc}
     * <p>
     * <b>Emulated, and only within this JVM.</b> A local filesystem offers no atomic compare-and-swap, so the
     * conditional write below is serialized with in-process locks. That is sufficient for tests and single-node
     * development, but it is <b>not</b> a cross-node fencing primitive: two nodes writing the same blob on a shared
     * filesystem can both believe they won the CAS. Only a repository whose backing store enforces the precondition
     * server-side (such as S3 conditional writes) can fence writers across nodes.
     */
    /**
     * Always {@code false}, even though {@link #writeBlobConditionally} and {@link #readBlobWithVersion} below are
     * implemented and behave correctly within one JVM. A local filesystem offers no compare-and-swap, so the
     * precondition is emulated with in-process locks over a JVM-local version map - which means two nodes writing the
     * same blob on a shared filesystem can both believe they won. Emulation is not support: see
     * {@link BlobContainer#isConditionalWriteSupported()}. Remote store fencing therefore refuses this container, which
     * is deliberate, because an emulated fence would report a shard as protected when it is not.
     * <p>
     * Integration tests still need to exercise fencing, and their cluster nodes share a single JVM, so the emulation is
     * genuinely sufficient there. {@code ReloadableFsRepository} overrides this for that purpose alone.
     */
    @Override
    public boolean isConditionalWriteSupported() {
        return false;
    }

    @Override
    public VersionedBlob readBlobWithVersion(String blobName) throws IOException {
        synchronized (conditionalWriteLock(blobName)) {
            final Path file = resolveBlobPath(blobName);
            final String versionToken = currentVersionToken(blobName);
            try {
                // Versioned reads materialize the blob in memory and are meant for small control blobs only; refuse
                // anything larger than the write-side bound rather than risk loading an arbitrarily large file.
                if (Files.size(file) > blobStore.bufferSizeInBytes()) {
                    throw new IOException("[" + blobName + "] blob of size [" + Files.size(file) + "] is too large for a versioned read");
                }
                final byte[] content = Files.readAllBytes(file);
                return new VersionedBlob(content, versionToken);
            } catch (FileNotFoundException | NoSuchFileException e) {
                throw new NoSuchFileException("[" + blobName + "] blob not found");
            }
        }
    }

    @Override
    public String writeBlobConditionally(String blobName, InputStream inputStream, long blobSize, @Nullable String expectedVersionToken)
        throws IOException {
        resolveBlobPath(blobName);
        synchronized (conditionalWriteLock(blobName)) {
            final String currentVersionToken = currentVersionToken(blobName);
            if (Objects.equals(expectedVersionToken, currentVersionToken) == false) {
                throw new BlobVersionConflictException(
                    "conditional write conflict for blob [" + blobName + "]: expected [" + expectedVersionToken + "]"
                );
            }
            final byte[] content = inputStream.readAllBytes();
            final String tempBlob = tempBlobName(blobName);
            final Path tempBlobPath = path.resolve(tempBlob);
            try {
                Files.createDirectories(path);
                Files.write(tempBlobPath, content);
                IOUtils.fsync(tempBlobPath, false);
                moveBlobAtomic(tempBlob, blobName, false);
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
            final String newVersionToken = UUIDs.randomBase64UUID();
            CONDITIONAL_WRITE_VERSIONS.put(conditionalWriteKey(blobName), newVersionToken);
            return newVersionToken;
        }
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
        // A published data inode is never modified. An open file therefore remains a consistent
        // snapshot through replacement or unlink, without creating coordination files on read-only stores.
        return readRegisterFile(resolveBlobPath(blobName));
    }

    @Override
    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
        throws IOException {
        final Path registerPath = resolveBlobPath(blobName);
        Files.createDirectories(registerPath.getParent());
        return underRegisterLock(registerPath, () -> {
            final Optional<BlobRegister> current = readRegisterFile(registerPath);
            final long generation = current.map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
            if (generation != expectedGeneration) {
                return BlobRegisterCasResult.conflict(generation);
            }
            final long next = generation == BlobRegister.ABSENT_GENERATION ? BlobRegister.initialGeneration() : generation + 1;
            writeRegisterUnderLock(registerPath, next, newValue);
            return BlobRegisterCasResult.applied(next);
        });
    }

    /** Deletes only the expected version, while the stable coordination inode remains locked. */
    @Override
    public boolean deleteRegisterIfUnchanged(String blobName, long expectedGeneration) throws IOException {
        final Path registerPath = resolveBlobPath(blobName);
        try {
            Files.readAttributes(registerPath, BasicFileAttributes.class);
            return underRegisterLock(registerPath, () -> {
                final Optional<BlobRegister> current = readRegisterFile(registerPath);
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

    private Path resolveBlobPath(String blobName) {
        final Path registerPath = path.resolve(blobName);
        if (isRegisterCoordinationPath(registerPath)) {
            throw new IllegalArgumentException("reserved register coordination file: " + blobName);
        }
        return registerPath;
    }

    private static boolean isRegisterCoordinationPath(Path path) {
        for (Path part : path) {
            if (part.toString().equals(REGISTER_LOCK_DIRECTORY)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Coordinates through a stable sidecar, never the data inode that a delete or atomic replacement removes.
     * Every process sharing a filesystem register must use this protocol. Canonical parent paths also keep
     * aliases of one directory on the same in-process stripe. There are at most 64 lock files per
     * container, rather than one per register. Separate files are necessary: on some systems closing
     * any channel on a file releases all locks this process holds on it, including other byte ranges.
     * The coordination directory remains after a register is deleted.
     */
    private <T> T underRegisterLock(Path registerPath, org.opensearch.common.CheckedSupplier<T, IOException> work) throws IOException {
        final Path parent = registerPath.getParent().toRealPath();
        final int stripe = Math.floorMod(parent.resolve(registerPath.getFileName()).toString().hashCode(), REGISTER_LOCK_STRIPE_COUNT);
        final Path lockDirectory = parent.resolve(REGISTER_LOCK_DIRECTORY);
        Files.createDirectories(lockDirectory);
        final ReentrantLock lock = REGISTER_LOCK_STRIPES[stripe];
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

    private Optional<BlobRegister> readRegisterFile(Path registerPath) throws IOException {
        try (FileChannel channel = FileChannel.open(registerPath, StandardOpenOption.READ)) {
            final long size = channel.size();
            if (size == 0L) {
                return Optional.empty();
            }
            if (size < Long.BYTES || size > Integer.MAX_VALUE) {
                throw new IOException("invalid register length " + size + " at " + registerPath);
            }
            final byte[] bytes = org.opensearch.common.io.Channels.readFromFileChannel(channel, 0, (int) size);
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            final long generation = buffer.getLong();
            final byte[] value = new byte[buffer.remaining()];
            buffer.get(value);
            return Optional.of(new BlobRegister(generation, new BytesArray(value)));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }

    private void writeRegisterUnderLock(Path registerPath, long newGeneration, BytesReference newValue) throws IOException {
        final byte[] valueBytes = BytesReference.toBytes(newValue);
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + valueBytes.length);
        buffer.putLong(newGeneration);
        buffer.put(valueBytes);
        // Independent of the register's name length, including names already at the filesystem's limit.
        final Path staged = registerPath.getParent()
            .resolve(REGISTER_LOCK_DIRECTORY)
            .resolve(TEMP_FILE_PREFIX + "register-" + UUIDs.randomBase64UUID());
        try {
            try (FileChannel channel = FileChannel.open(staged, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                org.opensearch.common.io.Channels.writeToChannel(buffer.array(), channel, 0);
                channel.force(true);
            }
            Files.move(staged, registerPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            IOUtils.fsync(registerPath.getParent(), true);
        } finally {
            Files.deleteIfExists(staged);
        }
    }
}
