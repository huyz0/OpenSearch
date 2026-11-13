/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A store whose every request takes at least a fixed extra time, and which records what each request was.
 *
 * <p>A local S3 answers in a millisecond; a real one in tens. Latency, not request count, is what a caller
 * waiting on a cold shard feels, and serial requests multiply it where concurrent ones do not -- so the delay
 * is what makes a sequence's shape measurable on a laptop. The record is what shows which requests the
 * sequence repeats.
 */
final class LatencyInjectingBlobStore implements BlobStore {

    /** One request: what, where, and on which thread. */
    record Request(String op, String key, String thread) {
        @Override
        public String toString() {
            return op + " " + key;
        }
    }

    private final BlobStore inner;
    private volatile long delayNanos;
    private final ConcurrentLinkedQueue<Request> log = new ConcurrentLinkedQueue<>();

    LatencyInjectingBlobStore(BlobStore inner, long delayMillis) {
        this.inner = inner;
        this.delayNanos = TimeUnit.MILLISECONDS.toNanos(delayMillis);
    }

    void setDelayMillis(long delayMillis) {
        this.delayNanos = TimeUnit.MILLISECONDS.toNanos(delayMillis);
    }

    private volatile int tailEvery;
    private volatile long tailNanos;
    private final java.util.concurrent.atomic.AtomicLong requests = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Makes every {@code every}-th request take {@code tailMillis} instead, the slow tail an object store has.
     *
     * @param every how often; zero or less for never
     * @param tailMillis how long those take
     */
    void setTail(int every, long tailMillis) {
        this.tailNanos = TimeUnit.MILLISECONDS.toNanos(tailMillis);
        this.tailEvery = every;
        // From the start of the count, so two runs put their slow requests at the same places.
        requests.set(0);
    }

    /**
     * Per-request latency by kind of request, with jitter and a slow tail, drawn from a seeded random so two runs see the same
     * sequence of delays. Each kind takes a uniform delay in {@code [min, max]} milliseconds; one request in {@code tailEvery}
     * (on average) takes {@code tailFactor} times that, and one in {@code stallEvery} takes {@code [stallMin, stallMax]}.
     *
     * @param read GET, ranged GET, HEAD and register read
     * @param put unconditional PUT
     * @param conditional PUT with If-Match or If-None-Match: register create and compare-and-swap
     * @param delete DELETE, single, batch or conditional
     * @param list LIST
     */
    record Profile(String name, long[] read, long[] put, long[] conditional, long[] delete, long[] list, int tailEvery, double tailFactor,
        int stallEvery, long stallMin, long stallMax) {
        /**
         * Shaped on published S3 Standard same-region figures for small objects: first-byte GET latency in the tens of
         * milliseconds, PUT roughly twice a GET, conditional PUTs a little above a plain PUT, LIST a little above a PUT,
         * a p99 several times the median and rare stalls of half a second or more.
         */
        static final Profile S3 = new Profile(
            "s3",
            new long[] { 20, 30 },
            new long[] { 40, 60 },
            new long[] { 50, 70 },
            new long[] { 25, 40 },
            new long[] { 40, 70 },
            50,
            4.0,
            1000,
            500,
            1000
        );

        /** No added delay. */
        static final Profile NONE = new Profile(
            "none",
            new long[] { 0, 0 },
            new long[] { 0, 0 },
            new long[] { 0, 0 },
            new long[] { 0, 0 },
            new long[] { 0, 0 },
            0,
            1.0,
            0,
            0,
            0
        );

        /**
         * The profile a name selects.
         *
         * @param name {@code none} or {@code s3}
         * @return the profile
         */
        static Profile named(String name) {
            return switch (name) {
                case "none" -> NONE;
                case "s3" -> S3;
                default -> throw new IllegalArgumentException("unknown latency profile [" + name + "]: none or s3");
            };
        }

        long[] rangeOf(String op) {
            if (op.startsWith("GET") || op.equals("HEAD")) {
                return read;
            } else if (op.equals("CAS-reg") || op.equals("CREATE-reg")) {
                return conditional;
            } else if (op.startsWith("PUT")) {
                return put;
            } else if (op.startsWith("DELETE")) {
                return delete;
            } else {
                return list;
            }
        }

        @Override
        public String toString() {
            if (this == NONE) {
                return "none";
            }
            return String.format(
                java.util.Locale.ROOT,
                "%s (GET %d-%d, PUT %d-%d, conditional PUT %d-%d, DELETE %d-%d, LIST %d-%d ms; 1 in %d x%.0f; 1 in %d at %d-%d ms)",
                name,
                read[0],
                read[1],
                put[0],
                put[1],
                conditional[0],
                conditional[1],
                delete[0],
                delete[1],
                list[0],
                list[1],
                tailEvery,
                tailFactor,
                stallEvery,
                stallMin,
                stallMax
            );
        }
    }

    private volatile long bytesPerSecond;
    private final Map<String, java.util.concurrent.atomic.LongAdder[]> putBytes = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Adds transfer time to every PUT and GET in proportion to its body, as a single connection to an object store has: a
     * request of {@code n} bytes takes {@code n / bytesPerSecond} on top of its first-byte latency. Zero turns it off, which
     * is what every meter but the large-document one wants: their bodies are small enough for it not to matter.
     *
     * @param bytesPerSecond the per-request transfer rate, or zero for none
     */
    void setBytesPerSecond(long bytesPerSecond) {
        this.bytesPerSecond = Math.max(0L, bytesPerSecond);
    }

    /**
     * What has been PUT, by where: for each place, the PUT count, total bytes and the largest body.
     *
     * @return place to {count, bytes, largest}
     */
    Map<String, long[]> putBytesByPlace() {
        final Map<String, long[]> out = new java.util.TreeMap<>();
        putBytes.forEach((place, adders) -> out.put(place, new long[] { adders[0].sum(), adders[1].sum(), adders[2].sum() }));
        return out;
    }

    /** Forgets what {@link #putBytesByPlace} has counted. */
    void resetPutBytes() {
        putBytes.clear();
    }

    private static String placeOf(BlobPath path, String blobName) {
        final String full = path.buildAsString();
        if (full.contains("nodelog/")) {
            return "node log";
        }
        final int dot = blobName.lastIndexOf('.');
        final String kind = dot < 0 ? "(no extension)" : blobName.substring(dot);
        final int slash = full.indexOf('/');
        return (slash < 0 ? full : full.substring(0, slash)) + " " + kind;
    }

    private void transfer(long bytes) {
        final long rate = bytesPerSecond;
        if (rate <= 0 || bytes <= 0) {
            return;
        }
        final long delay = bytes * 1_000_000_000L / rate;
        final long until = System.nanoTime() + delay;
        for (long left = delay; left > 0; left = until - System.nanoTime()) {
            LockSupport.parkNanos(left);
        }
    }

    private void notePut(BlobPath path, String blobName, long size) {
        final java.util.concurrent.atomic.LongAdder[] adders = putBytes.computeIfAbsent(
            placeOf(path, blobName),
            ignored -> new java.util.concurrent.atomic.LongAdder[] {
                new java.util.concurrent.atomic.LongAdder(),
                new java.util.concurrent.atomic.LongAdder(),
                new java.util.concurrent.atomic.LongAdder() }
        );
        adders[0].increment();
        adders[1].add(size);
        synchronized (adders) {
            if (size > adders[2].sum()) {
                adders[2].add(size - adders[2].sum());
            }
        }
        transfer(size);
    }

    private InputStream throttled(InputStream in) {
        if (bytesPerSecond <= 0) {
            return in;
        }
        return new java.io.FilterInputStream(in) {
            @Override
            public int read() throws IOException {
                final int b = super.read();
                if (b >= 0) {
                    transfer(1);
                }
                return b;
            }

            @Override
            public int read(byte[] buf, int off, int len) throws IOException {
                final int n = super.read(buf, off, len);
                transfer(n);
                return n;
            }
        };
    }

    private volatile Profile profile;
    private final java.util.Random jitter = new java.util.Random(0x5EED);

    /**
     * Replaces the fixed delay and tail with a per-request profile; {@code null} goes back to the fixed delay.
     *
     * @param profile the profile, or null
     */
    void setProfile(Profile profile) {
        synchronized (jitter) {
            jitter.setSeed(0x5EED);
        }
        this.profile = profile;
    }

    private long profiledNanos(Profile p, String op) {
        final long[] range = p.rangeOf(op);
        final double r;
        final double tail;
        final double stall;
        synchronized (jitter) {
            r = jitter.nextDouble();
            tail = jitter.nextDouble();
            stall = jitter.nextDouble();
        }
        double millis = range[0] + r * (range[1] - range[0]);
        if (p.stallEvery() > 0 && stall < 1.0 / p.stallEvery()) {
            millis = p.stallMin() + r * (p.stallMax() - p.stallMin());
        } else if (p.tailEvery() > 0 && tail < 1.0 / p.tailEvery()) {
            millis *= p.tailFactor();
        }
        return (long) (millis * 1_000_000L);
    }

    /** Takes and clears what has been recorded since the last call. */
    List<Request> drain() {
        final List<Request> drained = new ArrayList<>();
        for (Request r = log.poll(); r != null; r = log.poll()) {
            drained.add(r);
        }
        return drained;
    }

    @Override
    public void validateKey(BlobPath path, String blobName) {
        inner.validateKey(path, blobName);
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new Delayed(inner.blobContainer(path));
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }

    @Override
    public Map<String, Long> stats() {
        return inner.stats();
    }

    private void pay(String op, BlobPath path, String name) {
        log.add(new Request(op, path.buildAsString() + (name == null ? "" : name), Thread.currentThread().getName()));
        final Profile p = profile;
        final int every = tailEvery;
        final long delay = p != null ? profiledNanos(p, op)
            : every > 0 && requests.incrementAndGet() % every == 0 ? Math.max(tailNanos, delayNanos)
            : delayNanos;
        final long until = System.nanoTime() + delay;
        for (long left = delay; left > 0; left = until - System.nanoTime()) {
            LockSupport.parkNanos(left);
        }
    }

    private final class Delayed extends DelegatingBlobContainer {

        private final BlobContainer delegate;

        Delayed(BlobContainer delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        private Map<String, BlobContainer> wrap(Map<String, BlobContainer> children) {
            final Map<String, BlobContainer> wrapped = new LinkedHashMap<>();
            for (Map.Entry<String, BlobContainer> child : children.entrySet()) {
                wrapped.put(child.getKey(), new Delayed(child.getValue()));
            }
            return wrapped;
        }

        @Override
        public boolean blobExists(String blobName) throws IOException {
            pay("HEAD", path(), blobName);
            return delegate.blobExists(blobName);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            pay("GET", path(), blobName);
            return throttled(delegate.readBlob(blobName));
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            pay("GET-range", path(), blobName);
            return throttled(delegate.readBlob(blobName, position, length));
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            pay("PUT", path(), blobName);
            notePut(path(), blobName, blobSize);
            delegate.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            pay("PUT", path(), blobName);
            notePut(path(), blobName, blobSize);
            delegate.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            pay("DELETE-tree", path(), null);
            return delegate.delete();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            pay("DELETE", path(), String.valueOf(blobNames));
            delegate.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            pay("LIST", path(), null);
            return delegate.listBlobs();
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            pay("LIST-children", path(), null);
            return wrap(delegate.children());
        }

        @Override
        public Map<String, BlobContainer> children(String startAfter, int limit) throws IOException {
            pay("LIST-children", path(), null);
            return wrap(delegate.children(startAfter, limit));
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            pay("LIST", path(), blobNamePrefix);
            return delegate.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public List<BlobMetadata> listBlobsByPrefix(String blobNamePrefix, String startAfter, int limit) throws IOException {
            pay("LIST", path(), blobNamePrefix);
            return delegate.listBlobsByPrefix(blobNamePrefix, startAfter, limit);
        }

        @Override
        public List<BlobMetadata> listBlobsByPrefixInSortedOrder(String blobNamePrefix, int limit, BlobNameSortOrder order)
            throws IOException {
            pay("LIST", path(), blobNamePrefix);
            return delegate.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, order);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            pay("GET-reg", path(), blobName);
            return ConditionalBlobContainer.of(delegate).readRegister(blobName);
        }

        @Override
        public boolean deleteRegisterIfUnchanged(String blobName, long expectedGeneration) throws IOException {
            pay("DELETE-reg", path(), blobName);
            return ConditionalBlobContainer.of(delegate).deleteRegisterIfUnchanged(blobName, expectedGeneration);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            pay("CAS-reg", path(), blobName);
            return ConditionalBlobContainer.of(delegate).compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }

        @Override
        public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
            pay("CREATE-reg", path(), blobName);
            return ConditionalBlobContainer.of(delegate).createRegisterIfAbsent(blobName, value);
        }
    }
}
