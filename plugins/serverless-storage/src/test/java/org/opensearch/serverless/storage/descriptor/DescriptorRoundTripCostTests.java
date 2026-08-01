/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * How many object store round trips each descriptor operation costs.
 *
 * <h2>What this measures, and what it deliberately does not</h2>
 *
 * Wall-clock latency against an object store needs a real bucket, and timing against a filesystem
 * container would report local disk speed dressed as an object store result. That half stays unmeasured
 * and is still stated as unmeasured.
 *
 * <p>The round trip <em>count</em> does not need a bucket, and it is the half the design's claims actually
 * rest on. T22 said index creation costs one request rather than two. T10 said a deleted name costs a
 * second read. T8 said a live read costs one. Every one of those is a claim about how many times the
 * descriptor layer touches the store, none of them had been checked end to end through that layer, and
 * each is exactly the kind of thing that silently regresses when somebody adds a read "just to check".
 *
 * <p>Latency then follows as arithmetic over a stated round trip time rather than as a measurement
 * pretending to be one, which is the honest form: the count is measured, the millisecond figure is
 * multiplication, and the reader can see which is which.
 *
 * <h2>Why the cost table is not itself a measurement</h2>
 *
 * A container operation is not always one request. {@code compareAndSwapRegister} on S3 issues a GET and
 * then a conditional PUT, which {@code S3BlobContainer} does in that order and its own javadoc describes.
 * {@code createRegisterIfAbsent} issues the PUT alone, which is the whole of T22 and is asserted directly
 * in {@code S3BlobStoreContainerTests} and server-side in {@code S3BlobStoreRepositoryTests}. So the
 * mapping below is read off that implementation rather than guessed, and if it ever drifts those tests
 * fail rather than this one quietly reporting the wrong number.
 */
public class DescriptorRoundTripCostTests extends OpenSearchTestCase {

    /** S3 requests per container operation, read off {@code S3BlobContainer}. */
    private static final int REQUESTS_PER_READ_REGISTER = 1;
    private static final int REQUESTS_PER_CREATE_REGISTER_IF_ABSENT = 1;
    private static final int REQUESTS_PER_CAS_REGISTER = 2;
    private static final int REQUESTS_PER_DELETE = 1;

    /** Counts what the descriptor layer asks of the store, without changing any of it. */
    private static final class CountingContainer implements BlobContainer {
        private final BlobContainer delegate;
        final AtomicLong readRegister = new AtomicLong();
        final AtomicLong createIfAbsent = new AtomicLong();
        final AtomicLong cas = new AtomicLong();
        final AtomicLong deletes = new AtomicLong();

        CountingContainer(BlobContainer delegate) {
            this.delegate = delegate;
        }

        void reset() {
            readRegister.set(0);
            createIfAbsent.set(0);
            cas.set(0);
            deletes.set(0);
        }

        /** The requests an S3-backed container would have issued for what was just counted. */
        long requests() {
            return readRegister.get() * REQUESTS_PER_READ_REGISTER + createIfAbsent.get() * REQUESTS_PER_CREATE_REGISTER_IF_ABSENT + cas
                .get() * REQUESTS_PER_CAS_REGISTER + deletes.get() * REQUESTS_PER_DELETE;
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            readRegister.incrementAndGet();
            return delegate.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            cas.incrementAndGet();
            return delegate.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }

        @Override
        public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
            createIfAbsent.incrementAndGet();
            return delegate.createRegisterIfAbsent(blobName, value);
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            deletes.incrementAndGet();
            delegate.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public BlobPath path() {
            return delegate.path();
        }

        @Override
        public boolean blobExists(String blobName) throws IOException {
            return delegate.blobExists(blobName);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            return delegate.readBlob(blobName);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return delegate.readBlob(blobName, position, length);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            delegate.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            delegate.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            return delegate.delete();
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            return delegate.listBlobs();
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            return delegate.children();
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            return delegate.listBlobsByPrefix(blobNamePrefix);
        }
    }

    private CountingContainer counting;
    private BlobDescriptorBackend backend;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        counting = new CountingContainer(new FsBlobContainer(store, BlobPath.cleanPath(), store.path()));
        backend = new BlobDescriptorBackend(counting);
    }

    private static IndexDescriptor descriptor(String name) {
        return new IndexDescriptor(
            name,
            java.util.UUID.randomUUID().toString(),
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            org.opensearch.Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            0L
        );
    }

    /** T22's claim, checked through the descriptor layer rather than at the container. */
    public void testCreatingAnIndexCostsOneRequest() {
        counting.reset();
        assertTrue(backend.create(descriptor("tenant-a")));

        assertEquals("creation must not read before it writes", 0, counting.readRegister.get());
        assertEquals(1, counting.createIfAbsent.get());
        assertEquals("index creation is one object store request", 1, counting.requests());
    }

    /** A live descriptor is one read, which is the common path and the one that must stay cheapest. */
    public void testReadingALiveDescriptorCostsOneRequest() {
        backend.create(descriptor("tenant-a"));
        counting.reset();

        assertNotNull(backend.get("tenant-a"));
        assertEquals(1, counting.requests());
    }

    /**
     * T10's cost, made explicit. A name that is not live falls through to the tombstone prefix, so it is
     * two reads rather than one. That is the price of keeping deleted names out of the descriptor listing,
     * and it is worth stating because it lands on the error path rather than the hot one.
     */
    public void testANameThatIsNotLiveCostsTwoRequests() {
        counting.reset();
        assertNull(backend.get("never-created"));
        assertEquals("a miss checks descriptors then tombstones", 2, counting.requests());

        backend.create(descriptor("tenant-a"));
        backend.putTombstoneAsync(backend.get("tenant-a").tombstoned());
        counting.reset();

        assertFalse(backend.get("tenant-a").exists());
        assertEquals("and a deleted name pays the same two", 2, counting.requests());
    }

    /** Deletion is a tombstone CAS plus removing the live object. */
    public void testDeletingAnIndexCostsFourRequests() {
        backend.create(descriptor("tenant-a"));
        IndexDescriptor tombstone = backend.get("tenant-a").tombstoned();
        counting.reset();

        backend.putTombstoneAsync(tombstone);

        assertEquals(1, counting.readRegister.get());
        assertEquals(1, counting.cas.get());
        assertEquals(1, counting.deletes.get());
        assertEquals("read the tombstone generation, CAS it, drop the live object", 4, counting.requests());
    }

    /**
     * The number the design's throughput story turns on, reported rather than asserted against a
     * threshold, because the millisecond column is arithmetic over an assumed round trip and only the
     * request count is measured.
     */
    public void testReportTheCostTable() {
        counting.reset();
        backend.create(descriptor("cost-create"));
        long create = counting.requests();

        counting.reset();
        backend.get("cost-create");
        long liveRead = counting.requests();

        counting.reset();
        backend.get("cost-absent");
        long missRead = counting.requests();

        // The read is done before the counter is reset, so the delete figure needs no adjustment. An
        // earlier version subtracted a fudge factor for it and got 3 against the dedicated assertion's 4,
        // which is exactly how a reported number drifts from the asserted one.
        IndexDescriptor tombstone = backend.get("cost-create").tombstoned();
        counting.reset();
        backend.putTombstoneAsync(tombstone);
        long delete = counting.requests();

        StringBuilder table = new StringBuilder("\noperation      | requests | at 20ms RTT | at 50ms RTT | at 100ms RTT\n");
        for (Object[] row : new Object[][] {
            { "create", create },
            { "read (live)", liveRead },
            { "read (absent)", missRead },
            { "delete", delete } }) {
            long requests = (Long) row[1];
            table.append(
                String.format(
                    Locale.ROOT,
                    "%-14s | %8d | %9d ms | %9d ms | %10d ms%n",
                    row[0],
                    requests,
                    requests * 20,
                    requests * 50,
                    requests * 100
                )
            );
        }
        logger.info("descriptor round trip cost (requests measured, latency is arithmetic){}", table);

        assertTrue("every operation must cost at least one request", create >= 1 && liveRead >= 1);
    }
}
