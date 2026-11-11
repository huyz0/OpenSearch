/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.VersionedBlob;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * One writer's view of an object store, which can stop or fail that writer at a chosen store call.
 *
 * <p>Every call made through this store is recorded as a {@link Step}, numbered in the order it was made. A test can
 * then pick a step and a {@link Fault} and run the same scenario again: the call at that step is made or not, its
 * answer is delivered or lost, and the writer either carries on or is gone. That is enough to model each way a
 * process can die around a store request:
 *
 * <ul>
 *   <li>{@link Fault#CRASH_BEFORE}: the process dies before the request is sent. The store never sees it.</li>
 *   <li>{@link Fault#CRASH_AFTER}: the request lands and the process dies before it reads the answer.</li>
 *   <li>{@link Fault#FAIL_BEFORE}: the request fails without reaching the store, and the process lives.</li>
 *   <li>{@link Fault#AMBIGUOUS}: the request lands, the answer is lost as a timeout, and the process lives. This
 *       is the case where a writer cannot tell whether its append exists.</li>
 * </ul>
 *
 * <p>After a crash every later call through this store throws {@link CrashedException}, so a writer that keeps
 * going after it should have died is caught rather than quietly allowed to write. Other writers use their own
 * instance over the same delegate and are unaffected.
 *
 * <p>Wrap one instance per writer and keep the scenario single-threaded: {@link CrashPointExplorer} relies on the
 * same scenario making the same calls in the same order.
 */
public final class CrashPointBlobStore implements BlobStore {

    /** The kind of store call a step made. */
    public enum Kind {
        /** A whole or ranged blob read. */
        READ,
        /** An existence check. */
        EXISTS,
        /** A blob write that may overwrite. */
        WRITE,
        /** A blob write that fails if the blob exists: an append, a fence, a create-only marker. */
        CREATE,
        /** A blob write conditioned on the version the writer last read. */
        CONDITIONAL_WRITE,
        /** Any listing. */
        LIST,
        /** A blob or container delete. */
        DELETE,
        /** A register read. */
        READ_REGISTER,
        /** A register create-if-absent. */
        CREATE_REGISTER,
        /** A register compare-and-swap. */
        SWAP_REGISTER,
        /** A conditional register delete. */
        DELETE_REGISTER
    }

    /** What happens at the chosen step. */
    public enum Fault {
        /** The process dies before the request is sent. */
        CRASH_BEFORE,
        /** The request lands and the process dies before reading the answer. */
        CRASH_AFTER,
        /** The request fails before reaching the store; the process carries on. */
        FAIL_BEFORE,
        /** The request lands and its answer is lost as an error; the process carries on. */
        AMBIGUOUS;

        /** @return whether the store saw the request */
        public boolean landed() {
            return this == CRASH_AFTER || this == AMBIGUOUS;
        }

        /** @return whether the writer is gone afterwards */
        public boolean crashes() {
            return this == CRASH_BEFORE || this == CRASH_AFTER;
        }
    }

    /**
     * One store call.
     *
     * @param index the call's position in this store's history, from 0
     * @param kind what kind of call it was
     * @param path the container path
     * @param blobName the blob or register name, or empty for a listing or container delete
     */
    public record Step(int index, Kind kind, String path, String blobName) {
        /** @return the call without its position, for comparing two runs of the same scenario */
        public String what() {
            return kind + " " + path + blobName;
        }

        @Override
        public String toString() {
            return "#" + index + " " + what();
        }
    }

    /** Thrown for the call a crash happens at and for every call after it. */
    public static final class CrashedException extends IOException {
        CrashedException(String message) {
            super(message);
        }

        CrashedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Thrown for an injected failure the writer survives. */
    public static final class InjectedFailureException extends IOException {
        InjectedFailureException(String message) {
            super(message);
        }

        InjectedFailureException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final BlobStore delegate;
    private final List<Step> steps = new ArrayList<>();
    private final List<Injection> injections = new ArrayList<>();
    private Step crashedAt;

    private record Injection(Predicate<Step> where, Fault fault, int[] remaining) {
    }

    /**
     * Wraps a store for one writer.
     *
     * @param delegate the store every writer shares
     */
    public CrashPointBlobStore(BlobStore delegate) {
        this.delegate = delegate;
    }

    /**
     * Injects a fault at the step with this index.
     *
     * @param index the step's position, as {@link #steps()} reported it on an earlier run
     * @param fault what happens there
     * @return this store
     */
    public synchronized CrashPointBlobStore at(int index, Fault fault) {
        return on(step -> step.index() == index, fault, 1);
    }

    /**
     * Injects a fault at the first {@code times} steps that match.
     *
     * @param where which steps
     * @param fault what happens there
     * @param times how many matching steps get it
     * @return this store
     */
    public synchronized CrashPointBlobStore on(Predicate<Step> where, Fault fault, int times) {
        injections.add(new Injection(where, fault, new int[] { times }));
        return this;
    }

    /** @return every call made so far, in order, including the one a crash happened at */
    public synchronized List<Step> steps() {
        return List.copyOf(steps);
    }

    /** @return the step the writer crashed at, if it has */
    public synchronized Optional<Step> crashedAt() {
        return Optional.ofNullable(crashedAt);
    }

    private <T> T step(Kind kind, BlobPath path, String blobName, CheckedSupplier<T, IOException> call) throws IOException {
        final Step step;
        final Fault fault;
        synchronized (this) {
            if (crashedAt != null) {
                throw new CrashedException("the writer crashed at " + crashedAt + " and cannot make " + kind + " " + blobName);
            }
            step = new Step(steps.size(), kind, path.buildAsString(), blobName);
            steps.add(step);
            fault = faultFor(step);
            if (fault != null && fault.crashes()) {
                // Marked before the call is made, so a call from another thread cannot slip in while this one is
                // in flight: from here on the writer is dead whatever the store answers.
                crashedAt = step;
            }
            if (fault == Fault.CRASH_BEFORE) {
                throw new CrashedException("crashed before " + step);
            }
            if (fault == Fault.FAIL_BEFORE) {
                throw new InjectedFailureException("failed before reaching the store: " + step);
            }
        }
        final T result;
        try {
            result = call.get();
        } catch (IOException | RuntimeException e) {
            // The store answered with an error (an existing blob, a refused swap). After a crash or a lost answer
            // the writer never sees it, so it gets the injected outcome instead, with the store's answer attached.
            if (fault == Fault.CRASH_AFTER) {
                throw new CrashedException("crashed after " + step + " was answered with " + e, e);
            }
            if (fault == Fault.AMBIGUOUS) {
                throw new InjectedFailureException("the answer to " + step + " was lost; it was " + e, e);
            }
            throw e;
        }
        if (fault == Fault.CRASH_AFTER) {
            closeQuietly(result);
            throw new CrashedException("crashed after " + step + " landed");
        }
        if (fault == Fault.AMBIGUOUS) {
            closeQuietly(result);
            throw new InjectedFailureException("the answer to " + step + " was lost; it landed");
        }
        return result;
    }

    /**
     * Whether an exception is one this store injected, or one a caller raised because of it.
     *
     * @param e the exception a writer ended with
     * @return true if its cause chain holds an injected crash or failure
     */
    public static boolean isInjected(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof CrashedException || t instanceof InjectedFailureException) {
                return true;
            }
            for (Throwable suppressed : t.getSuppressed()) {
                if (isInjected(suppressed)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Fault faultFor(Step step) {
        for (Injection injection : injections) {
            if (injection.remaining()[0] > 0 && injection.where().test(step)) {
                injection.remaining()[0]--;
                return injection.fault();
            }
        }
        return null;
    }

    private static void closeQuietly(Object result) {
        if (result instanceof java.io.Closeable stream) {
            try {
                stream.close();
            } catch (IOException ignored) {
                // The answer is being discarded anyway.
            }
        }
    }

    private void voidStep(Kind kind, BlobPath path, String blobName, VoidCall call) throws IOException {
        step(kind, path, blobName, () -> {
            call.run();
            return null;
        });
    }

    @FunctionalInterface
    private interface VoidCall {
        void run() throws IOException;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new Container(delegate.blobContainer(path));
    }

    @Override
    public void validateKey(BlobPath path, String blobName) {
        delegate.validateKey(path, blobName);
    }

    @Override
    public Map<String, Long> stats() {
        return delegate.stats();
    }

    @Override
    public Map<Metric, Map<String, Long>> extendedStats() {
        return delegate.extendedStats();
    }

    @Override
    public boolean isBlobMetadataEnabled() {
        return delegate.isBlobMetadataEnabled();
    }

    @Override
    public void reload(org.opensearch.cluster.metadata.RepositoryMetadata repositoryMetadata) {
        delegate.reload(repositoryMetadata);
    }

    @Override
    public void close() throws IOException {
        // The delegate is shared with other writers; closing one writer's view leaves it open.
    }

    private final class Container extends DelegatingBlobContainer {

        private final BlobContainer inner;

        Container(BlobContainer inner) {
            super(inner);
            this.inner = inner;
        }

        @Override
        public boolean blobExists(String blobName) throws IOException {
            return step(Kind.EXISTS, path(), blobName, () -> inner.blobExists(blobName));
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            return step(Kind.READ, path(), blobName, () -> buffered(inner.readBlob(blobName)));
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return step(Kind.READ, path(), blobName, () -> buffered(inner.readBlob(blobName, position, length)));
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            voidStep(
                failIfAlreadyExists ? Kind.CREATE : Kind.WRITE,
                path(),
                blobName,
                () -> inner.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists)
            );
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            voidStep(
                failIfAlreadyExists ? Kind.CREATE : Kind.WRITE,
                path(),
                blobName,
                () -> inner.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists)
            );
        }

        @Override
        public DeleteResult delete() throws IOException {
            return step(Kind.DELETE, path(), "", inner::delete);
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            voidStep(Kind.DELETE, path(), String.join(",", blobNames), () -> inner.deleteBlobsIgnoringIfNotExists(blobNames));
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            return step(Kind.LIST, path(), "", inner::listBlobs);
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            return wrapped(step(Kind.LIST, path(), "", inner::children));
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            return step(Kind.LIST, path(), blobNamePrefix, () -> inner.listBlobsByPrefix(blobNamePrefix));
        }

        @Override
        public List<BlobMetadata> listBlobsByPrefix(String blobNamePrefix, String startAfter, int limit) throws IOException {
            return step(Kind.LIST, path(), blobNamePrefix, () -> inner.listBlobsByPrefix(blobNamePrefix, startAfter, limit));
        }

        @Override
        public Map<String, BlobContainer> children(String startAfter, int limit) throws IOException {
            return wrapped(step(Kind.LIST, path(), "", () -> inner.children(startAfter, limit)));
        }

        /** Child containers from a listing go through this store too, or their calls would escape every fault. */
        private Map<String, BlobContainer> wrapped(Map<String, BlobContainer> children) {
            final Map<String, BlobContainer> out = new java.util.LinkedHashMap<>();
            children.forEach((name, child) -> out.put(name, new Container(child)));
            return out;
        }

        @Override
        public boolean isConditionalWriteSupported() {
            return inner.isConditionalWriteSupported();
        }

        @Override
        public long readBlobPreferredLength() {
            return inner.readBlobPreferredLength();
        }

        @Override
        public VersionedBlob readBlobWithVersion(String blobName) throws IOException {
            return step(Kind.READ, path(), blobName, () -> inner.readBlobWithVersion(blobName));
        }

        @Override
        public String writeBlobConditionally(String blobName, InputStream inputStream, long blobSize, String expectedVersionToken)
            throws IOException {
            return step(
                Kind.CONDITIONAL_WRITE,
                path(),
                blobName,
                () -> inner.writeBlobConditionally(blobName, inputStream, blobSize, expectedVersionToken)
            );
        }

        @Override
        public InputStreamWithMetadata readBlobWithMetadata(String blobName) throws IOException {
            return step(Kind.READ, path(), blobName, () -> inner.readBlobWithMetadata(blobName));
        }

        @Override
        public void writeBlobWithMetadata(
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists,
            Map<String, String> metadata
        ) throws IOException {
            voidStep(
                failIfAlreadyExists ? Kind.CREATE : Kind.WRITE,
                path(),
                blobName,
                () -> inner.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata)
            );
        }

        @Override
        public void writeBlobAtomicWithMetadata(
            String blobName,
            InputStream inputStream,
            Map<String, String> metadata,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws IOException {
            voidStep(
                failIfAlreadyExists ? Kind.CREATE : Kind.WRITE,
                path(),
                blobName,
                () -> inner.writeBlobAtomicWithMetadata(blobName, inputStream, metadata, blobSize, failIfAlreadyExists)
            );
        }

        @Override
        public List<BlobMetadata> listBlobsByPrefixInSortedOrder(String blobNamePrefix, int limit, BlobNameSortOrder order)
            throws IOException {
            return step(Kind.LIST, path(), blobNamePrefix, () -> inner.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, order));
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            return step(Kind.READ_REGISTER, path(), blobName, () -> ConditionalBlobContainer.of(inner).readRegister(blobName));
        }

        @Override
        public boolean deleteRegisterIfUnchanged(String blobName, long expectedGeneration) throws IOException {
            return step(
                Kind.DELETE_REGISTER,
                path(),
                blobName,
                () -> ConditionalBlobContainer.of(inner).deleteRegisterIfUnchanged(blobName, expectedGeneration)
            );
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return step(
                Kind.SWAP_REGISTER,
                path(),
                blobName,
                () -> ConditionalBlobContainer.of(inner).compareAndSwapRegister(blobName, expectedGeneration, newValue)
            );
        }

        @Override
        public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
            return step(
                Kind.CREATE_REGISTER,
                path(),
                blobName,
                () -> ConditionalBlobContainer.of(inner).createRegisterIfAbsent(blobName, value)
            );
        }
    }

    /**
     * Reads a stream fully, so the request has completed when the step returns and a fault after it means the bytes
     * really were fetched. A lazily read stream would let the "after" fault fire before the store was asked.
     */
    private static InputStream buffered(InputStream in) throws IOException {
        try (in) {
            return new ByteArrayInputStream(in.readAllBytes());
        }
    }
}
