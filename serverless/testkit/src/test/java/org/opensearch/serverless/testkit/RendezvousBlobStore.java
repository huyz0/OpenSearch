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
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A blob store that makes work for several shards meet each other, or fail.
 *
 * <p><b>This asserts concurrency rather than speed.</b> The obvious test for a fan-out is to time it and
 * check the total is closer to the slowest shard than to their sum, and that test is a coin flip on a busy
 * build machine — the notes for {@code _bulk} already record a wall-clock comparison that measured a
 * network hop and reported it as the cost of batching.
 *
 * <p>Instead the first read for each distinct shard blocks until <em>every</em> shard has arrived, and
 * <b>fails if they do not</b>. Run one at a time, a shard waits for peers that cannot arrive until it
 * returns, times out, and drops out of the search's coverage. Run together, they all arrive, the latch
 * trips, and everything proceeds. There is no threshold to tune and no timing to be unlucky with:
 * sequential cannot pass this, and concurrent cannot fail it.
 *
 * <p>The failure on timeout is load-bearing rather than tidy. Letting a timed-out waiter continue — which
 * is what this did first — leaves a sequential fan-out answering every shard, just slowly, and every
 * assertion passing.
 */
public final class RendezvousBlobStore implements BlobStore {

    /** Which operation the shards are made to meet on. */
    public enum Meet {
        /** Reads — a search opening each shard from the object store. */
        READS,
        /** Writes — a batch appending to each shard's log. */
        WRITES
    }

    private final BlobStore delegate;
    private final int parties;
    private final Meet on;
    private final long timeoutMillis;
    private volatile CountDownLatch arrived;
    private final Set<String> seen = ConcurrentHashMap.newKeySet();

    /**
     * Wraps a store, meeting on reads.
     *
     * @param delegate the real store
     * @param parties how many distinct shards must meet
     * @param timeoutMillis how long a waiter gives the others before giving up
     */
    public RendezvousBlobStore(BlobStore delegate, int parties, long timeoutMillis) {
        this(delegate, parties, timeoutMillis, Meet.READS);
    }

    /**
     * Wraps a store, meeting on the operation named.
     *
     * <p>Reads are what a search does; writes are what a batch does. It has to be one or the other rather
     * than both, because a fixture is loaded through the same store before the thing under test runs — and
     * a rendezvous that tripped on the fixture's writes would deadlock the setup.
     *
     * @param delegate the real store
     * @param parties how many distinct shards must meet
     * @param timeoutMillis how long a waiter gives the others before giving up
     * @param on which operation the shards meet on
     */
    public RendezvousBlobStore(BlobStore delegate, int parties, long timeoutMillis, Meet on) {
        this.delegate = delegate;
        this.parties = parties;
        this.on = on;
        this.timeoutMillis = timeoutMillis;
        this.arrived = new CountDownLatch(parties);
    }

    /**
     * Forgets who has already arrived and waits for a fresh set.
     *
     * <p>For a test whose fixture goes through the same store as the thing under test: load the fixture,
     * arm, then run the operation whose concurrency is in question. Without this, the setup's own
     * operations are the ones that meet and the operation under test never rendezvouses at all — which
     * would make the assertion pass whether or not it ran concurrently.
     */
    public void arm() {
        seen.clear();
        arrived = new CountDownLatch(parties);
    }

    /**
     * Reports whether every party turned up.
     *
     * @return true when the rendezvous completed
     */
    public boolean everyoneArrived() {
        return arrived.getCount() == 0;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new Rendezvous(delegate.blobContainer(path));
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /** The shard a path belongs to, which is the segment after "segments/". */
    private static String shardOf(BlobPath path) {
        final String[] parts = path.toArray();
        for (int i = 0; i < parts.length - 1; i++) {
            if (parts[i].equals("segments")) {
                return parts[i + 1];
            }
        }
        return null;
    }

    private final class Rendezvous implements BlobContainer {

        private final BlobContainer inner;

        Rendezvous(BlobContainer inner) {
            this.inner = inner;
        }

        private void meet(Meet kind) throws IOException {
            if (kind != on) {
                return;
            }
            final CountDownLatch arrived = RendezvousBlobStore.this.arrived;
            final String shard = shardOf(inner.path());
            if (shard == null || seen.add(shard) == false) {
                return;
            }
            arrived.countDown();
            try {
                // Throwing on timeout, and that is the whole discriminating power of this class. A first
                // version let a timed-out waiter proceed, reasoning that the failure would show up as
                // coverage -- it did not: run one at a time, each shard arrives, waits out its timeout,
                // carries on and answers, so the search completes and every assertion passes. A sequential
                // fan-out has to be unable to finish, not merely slow.
                if (arrived.await(timeoutMillis, TimeUnit.MILLISECONDS) == false) {
                    throw new IOException("shard " + shard + " waited for its peers at the rendezvous and they never arrived");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted at the rendezvous", e);
            }
        }

        @Override
        public BlobPath path() {
            return inner.path();
        }

        @Override
        public boolean blobExists(String blobName) throws IOException {
            return inner.blobExists(blobName);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            meet(Meet.READS);
            return inner.readBlob(blobName);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            meet(Meet.READS);
            return inner.readBlob(blobName, position, length);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            // A takeover seal is not part of the operation under test. This class exists to prove that the
            // shard groups of ONE request run at the same time, and it does that by refusing to let a shard
            // proceed until its peers have arrived. Shards are activated one at a time, long before any
            // request, so a write on the activation path can never meet peers -- it would deadlock every
            // test using this store rather than discriminate between concurrent and sequential fan-out.
            if (blobName.startsWith("seal-") == false) {
                meet(Meet.WRITES);
            }
            inner.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            inner.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            return inner.delete();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            inner.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            return inner.listBlobs();
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            return inner.children().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new Rendezvous(e.getValue())));
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            return inner.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            return inner.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return inner.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }
}
