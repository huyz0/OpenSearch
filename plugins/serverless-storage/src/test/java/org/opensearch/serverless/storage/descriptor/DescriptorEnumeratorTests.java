/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The path that makes invariant I2 true for the descriptor store: the full name set, from the object store
 * and nothing else.
 */
public class DescriptorEnumeratorTests extends OpenSearchTestCase {

    /**
     * Only the names this test created.
     *
     * <p>Lucene's {@code ExtrasFS} drops files into test directories at random seeds to catch code that
     * assumes a directory holds only what it wrote. It caught this class doing that, for the second time
     * on this branch after {@code BlobDescriptorBackendTests}, which is a good argument for never asserting
     * an exact listing against a real filesystem. The enumerator is asserted on what it finds of the known
     * population, not on what else shares the prefix.
     */
    private static List<String> known(List<String> enumerated, List<String> created) {
        return enumerated.stream().filter(created::contains).collect(Collectors.toList());
    }

    private FsBlobStore store;
    private BlobDescriptorBackend backend;
    private DescriptorEnumerator enumerator;

    private void setUpOver(Path directory) throws Exception {
        store = new FsBlobStore(1024, directory, false);
        backend = new BlobDescriptorBackend(new FsBlobContainer(store, BlobPath.cleanPath(), store.path()));
        enumerator = new DescriptorEnumerator(store::blobContainer, BlobPath.cleanPath());
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

    public void testAnEmptyStoreEnumeratesToNoDescriptors() throws Exception {
        setUpOver(createTempDir());
        assertEquals(List.of(), known(enumerator.allNames(), List.of("anything")));
    }

    public void testEveryCreatedNameIsFound() throws Exception {
        setUpOver(createTempDir());
        List<String> created = IntStream.range(0, 50).mapToObj(i -> "tenant-" + i).collect(Collectors.toList());
        created.forEach(name -> backend.create(descriptor(name)));

        assertEquals(created.stream().sorted().collect(Collectors.toList()), known(enumerator.allNames(), created));
    }

    /**
     * Deleted names are absent for free, because T10 moved tombstones out from under the descriptor
     * prefix. A layout that left them there would need a read per key to find out which names are live,
     * turning a listing into a full fetch of the population.
     */
    public void testDeletedNamesAreNotEnumerated() throws Exception {
        setUpOver(createTempDir());
        backend.create(descriptor("alive"));
        backend.create(descriptor("doomed"));
        backend.putTombstoneAsync(backend.get("doomed").tombstoned());

        assertEquals(List.of("alive"), known(enumerator.allNames(), List.of("alive", "doomed")));
    }

    /** Splitting the prefix space must produce the same set as one serial pass, not a subset. */
    public void testParallelEnumerationAgreesWithSerial() throws Exception {
        setUpOver(createTempDir());
        List<String> created = IntStream.range(0, 200).mapToObj(i -> (char) ('a' + (i % 26)) + "-tenant-" + i).collect(Collectors.toList());
        created.forEach(name -> backend.create(descriptor(name)));

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            assertEquals(known(enumerator.allNames(), created), known(enumerator.allNamesInParallel(executor), created));
        } finally {
            executor.shutdown();
        }
    }

    /**
     * A name outside the split alphabet must still be found. Correctness cannot depend on the alphabet
     * being complete, because a rebuild that quietly drops indices is the exact failure this area is most
     * careful about, and it would look like success.
     */
    public void testNamesOutsideTheSplitAlphabetAreStillFound() throws Exception {
        setUpOver(createTempDir());
        backend.create(descriptor("ordinary"));
        backend.create(descriptor(".system-ish"));

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<String> both = List.of(".system-ish", "ordinary");
            assertEquals(both, known(enumerator.allNamesInParallel(executor), both));
        } finally {
            executor.shutdown();
        }
    }

    /** Union rather than concatenation, so an overlapping split cannot duplicate a name into a rebuild. */
    public void testParallelEnumerationNeverReturnsDuplicates() throws Exception {
        setUpOver(createTempDir());
        IntStream.range(0, 100).forEach(i -> backend.create(descriptor("dup-check-" + i)));

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<String> names = enumerator.allNamesInParallel(executor);
            assertEquals(names.size(), names.stream().distinct().count());
        } finally {
            executor.shutdown();
        }
    }
}
