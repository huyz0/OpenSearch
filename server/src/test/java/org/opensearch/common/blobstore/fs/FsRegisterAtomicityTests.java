/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.common.blobstore.fs;

import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@LuceneTestCase.SuppressFileSystems("*")
public class FsRegisterAtomicityTests extends OpenSearchTestCase {
    private static class Faults extends FilterFileSystemProvider {
        final AtomicBoolean interruptWrite = new AtomicBoolean();
        final AtomicBoolean readOnly = new AtomicBoolean();
        final AtomicReference<Runnable> beforeDelete = new AtomicReference<>();
        final AtomicReference<Runnable> afterWrite = new AtomicReference<>();

        Faults(FileSystem delegate) {
            super("register-faults://", delegate);
        }

        private void deleting(Path path) {
            if (path.getFileName().toString().equals("record")) {
                final Runnable hook = beforeDelete.getAndSet(null);
                if (hook != null) {
                    hook.run();
                }
            }
        }

        @Override
        public boolean deleteIfExists(Path path) throws IOException {
            deleting(path);
            return super.deleteIfExists(path);
        }

        @Override
        public void delete(Path path) throws IOException {
            deleting(path);
            super.delete(path);
        }

        @Override
        public void createDirectory(Path path, FileAttribute<?>... attrs) throws IOException {
            if (readOnly.get()) {
                throw new java.nio.file.AccessDeniedException(path.toString());
            }
            super.createDirectory(path, attrs);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            if (readOnly.get()
                && (options.contains(java.nio.file.StandardOpenOption.WRITE)
                    || options.contains(java.nio.file.StandardOpenOption.CREATE)
                    || options.contains(java.nio.file.StandardOpenOption.CREATE_NEW))) {
                throw new java.nio.file.AccessDeniedException(path.toString());
            }
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
                private void failPartway(ByteBuffer src, long position) throws IOException {
                    if (interruptWrite.compareAndSet(true, false)) {
                        final ByteBuffer partial = src.slice();
                        partial.limit(Math.max(1, partial.remaining() / 2));
                        delegate.write(partial, position);
                        throw new IOException("interrupted register write");
                    }
                }

                private void wrote() {
                    final Runnable hook = afterWrite.getAndSet(null);
                    if (hook != null) {
                        hook.run();
                    }
                }

                @Override
                public int write(ByteBuffer src) throws IOException {
                    failPartway(src, position());
                    final int written = super.write(src);
                    wrote();
                    return written;
                }

                @Override
                public int write(ByteBuffer src, long position) throws IOException {
                    failPartway(src, position);
                    final int written = super.write(src, position);
                    wrote();
                    return written;
                }
            };
        }
    }

    public void testAnInterruptedWritePreservesTheCommittedRegister() throws Exception {
        final Path directory = createTempDir();
        final Faults faults = new Faults(directory.getFileSystem());
        try (FileSystem fs = faults.getFileSystem(null); var store = new FsBlobStore(1024, fs.getPath(directory.toString()), false)) {
            final BlobContainer container = store.blobContainer(BlobPath.cleanPath());
            final long generation = container.createRegisterIfAbsent("record", new BytesArray("committed-value")).currentGeneration();
            faults.interruptWrite.set(true);
            expectThrows(
                IOException.class,
                () -> container.compareAndSwapRegister("record", generation, new BytesArray("replacement-value"))
            );
            final var retained = container.readRegister("record").orElseThrow();
            assertEquals(generation, retained.generation());
            assertEquals("committed-value", retained.value().utf8ToString());
        }
    }

    public void testConditionalDeletionCannotEraseAnIndependentWritersAcknowledgedValue() throws Exception {
        final Path directory = createTempDir();
        final Faults faults = new Faults(directory.getFileSystem());
        try (FileSystem fs = faults.getFileSystem(null); var store = new FsBlobStore(1024, fs.getPath(directory.toString()), false)) {
            final BlobContainer container = store.blobContainer(BlobPath.cleanPath());
            final long generation = container.createRegisterIfAbsent("record", new BytesArray("old")).currentGeneration();
            final BlobContainer independent = independentContainer(store, fs.getPath(directory.toString()));
            final AtomicBoolean acknowledged = new AtomicBoolean();
            final AtomicBoolean interleaved = new AtomicBoolean();
            faults.beforeDelete.set(() -> {
                interleaved.set(true);
                try {
                    final long current = independent.readRegister("record").map(BlobRegister::generation).orElse(0L);
                    acknowledged.set(independent.compareAndSwapRegister("record", current, new BytesArray("renewed")).applied());
                } catch (OverlappingFileLockException blocked) {
                    // Separate class loaders model processes with independent Java locks. This JVM throws
                    // on an overlapping OS lock; an actual second process would wait until deletion ends.
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
            assertTrue(container.deleteRegisterIfUnchanged("record", generation));
            assertTrue("the second writer must run in the deletion window", interleaved.get());
            if (acknowledged.get() == false) {
                assertTrue(independent.createRegisterIfAbsent("record", new BytesArray("renewed")).applied());
            }
            assertEquals(
                "an acknowledged renewal must survive deletion",
                "renewed",
                container.readRegister("record").orElseThrow().value().utf8ToString()
            );
        }
    }

    public void testRegisterCoordinationIsHiddenAndLongNamesRemainUsable() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final BlobContainer container = store.blobContainer(BlobPath.cleanPath());
            final String name = "r".repeat(255);
            final long generation = container.createRegisterIfAbsent(name, new BytesArray("value")).currentGeneration();
            assertEquals(Set.of(name), container.listBlobs().keySet());
            assertTrue(container.children().isEmpty());
            assertTrue(container.children(null, 10).isEmpty());
            assertEquals(
                java.util.List.of(name),
                container.listBlobsByPrefix("", null, 10).stream().map(org.opensearch.common.blobstore.BlobMetadata::name).toList()
            );
            assertTrue(container.deleteRegisterIfUnchanged(name, generation));
            assertTrue(container.listBlobs().isEmpty());
            assertTrue(container.createRegisterIfAbsent(name, new BytesArray("new-incarnation")).applied());
            assertEquals(1L, container.delete().blobsDeleted());
        }
    }

    public void testConcurrentRegistersKeepTheirLocksIndependent() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final BlobContainer container = store.blobContainer(BlobPath.cleanPath());
            final int workers = 4;
            for (int i = 0; i < workers; i++) {
                container.createRegisterIfAbsent("parallel-" + i, new BytesArray("initial"));
            }
            final var executor = java.util.concurrent.Executors.newFixedThreadPool(workers);
            final var ready = new java.util.concurrent.CyclicBarrier(workers);
            final java.util.List<java.util.concurrent.Future<?>> tasks = new java.util.ArrayList<>();
            try {
                for (int i = 0; i < workers; i++) {
                    final String name = "parallel-" + i;
                    tasks.add(executor.submit(() -> {
                        ready.await();
                        for (int pass = 0; pass < 100; pass++) {
                            final long generation = container.readRegister(name).orElseThrow().generation();
                            assertTrue(container.compareAndSwapRegister(name, generation, new BytesArray("value-" + pass)).applied());
                        }
                        return null;
                    }));
                }
                for (var task : tasks) {
                    task.get(30, java.util.concurrent.TimeUnit.SECONDS);
                }
                for (int i = 0; i < workers; i++) {
                    assertEquals("value-99", container.readRegister("parallel-" + i).orElseThrow().value().utf8ToString());
                }
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS));
            }
        }
    }

    public void testReadingALegacyRegisterRequiresNoFilesystemWrites() throws Exception {
        final Path directory = createTempDir();
        final byte[] content = "legacy-value".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.nio.file.Files.write(
            directory.resolve("record"),
            ByteBuffer.allocate(Long.BYTES + content.length).putLong(42L).put(content).array()
        );
        final Faults faults = new Faults(directory.getFileSystem());
        faults.readOnly.set(true);
        try (FileSystem fs = faults.getFileSystem(null); var store = new FsBlobStore(1024, fs.getPath(directory.toString()), true)) {
            final var record = store.blobContainer(BlobPath.cleanPath()).readRegister("record").orElseThrow();
            assertEquals(42L, record.generation());
            assertEquals("legacy-value", record.value().utf8ToString());
        }
    }

    public void testReadersSeeWholeVersionsDuringConcurrentReplacement() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final BlobContainer container = store.blobContainer(BlobPath.cleanPath());
            container.createRegisterIfAbsent("record", new BytesArray("initial"));
            final var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
            final var ready = new java.util.concurrent.CyclicBarrier(2);
            final AtomicBoolean done = new AtomicBoolean();
            final String padding = "x".repeat(32_768);
            try {
                final var writer = executor.submit(() -> {
                    ready.await();
                    try {
                        for (int i = 0; i < 100; i++) {
                            final long generation = container.readRegister("record").orElseThrow().generation();
                            assertTrue(
                                container.compareAndSwapRegister(
                                    "record",
                                    generation,
                                    new BytesArray("g=" + (generation + 1) + ":" + padding)
                                ).applied()
                            );
                        }
                    } finally {
                        done.set(true);
                    }
                    return null;
                });
                final var reader = executor.submit(() -> {
                    ready.await();
                    do {
                        final var record = container.readRegister("record").orElseThrow();
                        final String value = record.value().utf8ToString();
                        if (value.equals("initial") == false) {
                            assertEquals("g=" + record.generation() + ":" + padding, value);
                        }
                    } while (done.get() == false);
                    return null;
                });
                writer.get(30, java.util.concurrent.TimeUnit.SECONDS);
                reader.get(30, java.util.concurrent.TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS));
            }
        }
    }

    public void testStagedRegisterVersionsAreNotListedAsUserBlobs() throws Exception {
        final Path directory = createTempDir();
        final Faults faults = new Faults(directory.getFileSystem());
        try (FileSystem fs = faults.getFileSystem(null); var store = new FsBlobStore(1024, fs.getPath(directory.toString()), false)) {
            final BlobContainer container = store.blobContainer(BlobPath.cleanPath());
            final long generation = container.createRegisterIfAbsent("record", new BytesArray("old")).currentGeneration();
            final AtomicBoolean observed = new AtomicBoolean();
            faults.afterWrite.set(() -> {
                try {
                    observed.set(true);
                    assertEquals(Set.of("record"), container.listBlobs().keySet());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
            assertTrue(container.compareAndSwapRegister("record", generation, new BytesArray("new")).applied());
            assertTrue("the listing must run while the new version is still staged", observed.get());
        }
    }

    private static BlobContainer independentContainer(FsBlobStore store, Path directory) throws Exception {
        final String name = FsBlobContainer.class.getName();
        final ClassLoader loader = new ClassLoader(FsBlobContainer.class.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String requested, boolean resolve) throws ClassNotFoundException {
                if (requested.equals(name) == false) {
                    return super.loadClass(requested, resolve);
                }
                synchronized (getClassLoadingLock(requested)) {
                    Class<?> type = findLoadedClass(requested);
                    if (type == null) {
                        try (var in = getParent().getResourceAsStream(requested.replace('.', '/') + ".class")) {
                            final byte[] bytes = in.readAllBytes();
                            type = defineClass(requested, bytes, 0, bytes.length, FsBlobContainer.class.getProtectionDomain());
                        } catch (IOException e) {
                            throw new ClassNotFoundException(requested, e);
                        }
                    }
                    if (resolve) {
                        resolveClass(type);
                    }
                    return type;
                }
            }
        };
        return (BlobContainer) loader.loadClass(name)
            .getConstructor(FsBlobStore.class, BlobPath.class, Path.class)
            .newInstance(store, BlobPath.cleanPath(), directory);
    }
}
