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
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs against a real {@link FsBlobContainer} rather than a mock, because the properties under test are
 * the store's rather than this class's: that a create-if-absent has one winner, and that a descriptor
 * survives a round trip through the register byte format.
 *
 * <p>That is only a meaningful arm because T21 fixed {@code FsBlobContainer}'s register lock, which
 * previously arbitrated per container instance rather than per file. Before that, a uniqueness assertion
 * here would have been asserting nothing.
 */
public class BlobDescriptorBackendTests extends OpenSearchTestCase {

    private BlobContainer containerOver(Path directory) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, directory, false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    private BlobDescriptorBackend backendOver(Path directory) throws Exception {
        return new BlobDescriptorBackend(containerOver(directory));
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

    public void testMissingDescriptorIsAbsentRatherThanAnError() throws Exception {
        assertNull(backendOver(createTempDir()).get("never-created"));
    }

    public void testCreatedDescriptorRoundTripsThroughTheRegister() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        IndexDescriptor written = descriptor("serverless_tenant-a");

        assertTrue(backend.create(written));

        IndexDescriptor read = backend.get("serverless_tenant-a");
        assertNotNull(read);
        assertEquals(written.name(), read.name());
        assertEquals(written.uuid(), read.uuid());
        assertEquals(written.shardCount(), read.shardCount());
    }

    public void testSecondCreateOfTheSameNameLoses() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        assertTrue(backend.create(descriptor("serverless_tenant-a")));

        IndexDescriptor loser = descriptor("serverless_tenant-a");
        assertFalse("losing a create race is an answer, not an error", backend.create(loser));

        // And the winner's value is what survives, rather than the loser having overwritten it.
        assertNotEquals(loser.uuid(), backend.get("serverless_tenant-a").uuid());
    }

    /**
     * The property index creation actually depends on, exercised across separate backend instances over
     * one directory, which is the shape separate nodes produce.
     */
    public void testConcurrentCreateOfOneNameHasExactlyOneWinner() throws Exception {
        Path shared = createTempDir();
        int contenders = 16;
        ExecutorService executor = Executors.newFixedThreadPool(contenders);
        CountDownLatch startLine = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();

        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < contenders; i++) {
                final BlobDescriptorBackend backend = backendOver(shared);
                futures.add(executor.submit(() -> {
                    try {
                        startLine.await();
                        if (backend.create(descriptor("contested"))) {
                            winners.incrementAndGet();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            startLine.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals("a name can only be taken once", 1, winners.get());
        assertNotNull(backendOver(shared).get("contested"));
    }

    public void testPutOverwritesWhereCreateWouldHaveLost() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        assertTrue(backend.create(descriptor("serverless_tenant-a")));

        IndexDescriptor replacement = descriptor("serverless_tenant-a");
        backend.put(replacement);

        assertEquals(replacement.uuid(), backend.get("serverless_tenant-a").uuid());
    }

    /**
     * Keys keep the name as a readable suffix, which is what makes a LIST-based rebuild possible at all.
     * Asserted because it is a storage-layout contract rather than an implementation detail: hashing the
     * key would be invisible here and would remove the only path from the object store back to the name
     * index.
     */
    public void testKeysArePrefixPreservingSoAListCanEnumerateThem() throws Exception {
        Path directory = createTempDir();
        BlobDescriptorBackend backend = backendOver(directory);
        backend.create(descriptor("logs-alpha"));
        backend.create(descriptor("logs-beta"));
        backend.create(descriptor("metrics-gamma"));

        BlobContainer descriptors = containerOver(directory).children().get("descriptors");
        assertNotNull("descriptors must live under their own prefix", descriptors);

        // Containment rather than an exact count. Lucene's ExtrasFS deliberately drops an "extra0" file
        // into test directories to catch code that assumes a directory holds only what it wrote, and it
        // caught this assertion doing exactly that. The property under test is that a prefix selects the
        // right names, which is what a rebuild needs, not that nothing else shares the directory.
        assertEquals(Set.of("logs-alpha", "logs-beta"), descriptors.listBlobsByPrefix("logs-").keySet());
        assertEquals(Set.of("metrics-gamma"), descriptors.listBlobsByPrefix("metrics-").keySet());
        assertTrue(descriptors.listBlobs().keySet().containsAll(Set.of("logs-alpha", "logs-beta", "metrics-gamma")));
    }

    /**
     * The case that has no answer without an idempotency token: a creation whose acknowledgement was lost.
     *
     * <p>Resending the same descriptor is what makes the retry recognisable. Reporting it as a collision
     * would fail a creation that actually succeeded.
     */
    public void testRetryingTheSameCreationRecognisesItsOwnWork() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        IndexDescriptor mine = descriptor("serverless_tenant-a");

        assertEquals(DescriptorBackend.CreateOutcome.CREATED, backend.createIdempotently(mine));
        assertEquals(DescriptorBackend.CreateOutcome.ALREADY_MINE, backend.createIdempotently(mine));
        assertTrue(backend.createIdempotently(mine).owned());
    }

    /** And a genuinely different creator is still told the name is taken. */
    public void testADifferentCreatorIsToldTheNameIsTaken() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        assertEquals(DescriptorBackend.CreateOutcome.CREATED, backend.createIdempotently(descriptor("serverless_tenant-a")));

        DescriptorBackend.CreateOutcome outcome = backend.createIdempotently(descriptor("serverless_tenant-a"));
        assertEquals(DescriptorBackend.CreateOutcome.TAKEN, outcome);
        assertFalse(outcome.owned());
    }

    /**
     * A tombstone has to stay findable by name, which is the constraint that rules out the obvious
     * uuid-keyed layout. {@code State.DELETED} exists so a node partitioned during a delete consults the
     * descriptor and drops its shard data instead of resurrecting the index, and the name is all that node
     * has to look it up by.
     */
    public void testADeletedNameResolvesToItsTombstoneRatherThanToNothing() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        IndexDescriptor live = descriptor("serverless_tenant-a");
        assertTrue(backend.create(live));

        backend.putTombstoneAsync(live.tombstoned());

        IndexDescriptor read = backend.get("serverless_tenant-a");
        assertNotNull("a deleted name must not read as never-existed", read);
        assertFalse("and it must report itself as gone", read.exists());
        assertEquals(live.uuid(), read.uuid());
    }

    /** A name that was never used stays genuinely absent, so the two cases remain distinguishable. */
    public void testANameThatNeverExistedIsStillAbsentAfterTombstonesExist() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        backend.create(descriptor("serverless_tenant-a"));
        backend.putTombstoneAsync(backend.get("serverless_tenant-a").tombstoned());

        assertNull(backend.get("serverless_tenant-b"));
    }

    /**
     * The layout property the whole flat-key design rests on: a LIST over descriptors returns live names
     * only, so a rebuild does not resurrect deleted indices and does not need a read per key to find out.
     */
    public void testDeletedNamesLeaveTheDescriptorPrefix() throws Exception {
        Path directory = createTempDir();
        BlobDescriptorBackend backend = backendOver(directory);
        backend.create(descriptor("logs-alpha"));
        backend.create(descriptor("logs-beta"));

        backend.putTombstoneAsync(backend.get("logs-alpha").tombstoned());

        BlobContainer root = containerOver(directory);
        assertEquals(Set.of("logs-beta"), root.children().get("descriptors").listBlobsByPrefix("logs-").keySet());
        assertEquals(Set.of("logs-alpha"), root.children().get("tombstones").listBlobsByPrefix("logs-").keySet());
    }

    /** Deleting twice has to converge rather than fail, including after a partially applied delete. */
    public void testDeletingTwiceIsIdempotent() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        backend.create(descriptor("serverless_tenant-a"));
        IndexDescriptor tombstone = backend.get("serverless_tenant-a").tombstoned();

        backend.putTombstoneAsync(tombstone);
        backend.putTombstoneAsync(tombstone);

        assertFalse(backend.get("serverless_tenant-a").exists());
    }

    /** A recreated name is live again, and the stale tombstone does not shadow it. */
    public void testANameCanBeRecreatedAfterDeletion() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        backend.create(descriptor("serverless_tenant-a"));
        backend.putTombstoneAsync(backend.get("serverless_tenant-a").tombstoned());

        IndexDescriptor recreated = descriptor("serverless_tenant-a");
        assertTrue("the name is free once tombstoned", backend.create(recreated));

        IndexDescriptor read = backend.get("serverless_tenant-a");
        assertTrue(read.exists());
        assertEquals(recreated.uuid(), read.uuid());
    }

    public void testThereIsNothingToBootstrap() throws Exception {
        assertTrue(backendOver(createTempDir()).available());
    }
}
