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

    /**
     * A wildcard finds gated indices on a filesystem store, which the obvious implementation would not.
     *
     * <p>This is the assertion that would have caught S7. Listing the base container under a
     * {@code "descriptors/"} prefix works on an object store, where a key is flat and the slash is just a
     * character, and returns nothing at all on a filesystem repository, where the slash is a directory
     * separator and a listing iterates one level. An expansion built that way expands to nothing, reports
     * success, and a tenant's {@code tenant-*} silently matches no index.
     *
     * <p>Asserted by finding something rather than by inspecting the addressing, because the addressing is
     * exactly the part a reader gets wrong.
     */
    public void testAWildcardExpandsOnAFilesystemStore() throws Exception {
        setUpOver(createTempDir());
        backend.create(descriptor("tenant-a"));
        backend.create(descriptor("tenant-b"));
        backend.create(descriptor("other-c"));

        var expansion = enumerator.expandPrefix("tenant-", 10);

        assertFalse("under the cap, so this is a list of matches rather than a refusal", expansion.exceeded());
        List<String> names = expansion.matches().stream().map(match -> match.name()).sorted().collect(Collectors.toList());
        assertEquals("zero here would mean the descriptor space is invisible rather than empty", List.of("tenant-a", "tenant-b"), names);
    }

    /**
     * Over the cap is an explicit refusal, not a short list.
     *
     * <p>The bound is part of the answer: a wildcard that matched five thousand tenants and quietly returned
     * a hundred is the failure the contract exists to make impossible. Asking the store for {@code limit + 1}
     * is how that is detected without counting the population.
     */
    public void testAnExpansionOverTheCapRefusesRatherThanTruncating() throws Exception {
        setUpOver(createTempDir());
        IntStream.range(0, 12).forEach(i -> backend.create(descriptor(String.format(java.util.Locale.ROOT, "capped-%02d", i))));

        var expansion = enumerator.expandPrefix("capped-", 5);

        assertTrue("twelve matches against a cap of five must be reported as over the cap", expansion.exceeded());
        assertEquals(5, expansion.limit());
    }

    /**
     * A deleted index leaves the expansion, and does so without a read per key.
     *
     * <p>Free only because tombstones live under their own prefix, so a listing of the descriptor space is
     * the live set by construction. A layout that left tombstones in place would need a GET per match to
     * tell live from deleted, which is the cost the cap exists to avoid.
     */
    public void testADeletedIndexLeavesTheExpansion() throws Exception {
        setUpOver(createTempDir());
        backend.create(descriptor("gone-1"));
        backend.create(descriptor("stays-1"));

        backend.putTombstoneAsync(descriptor("gone-1").tombstoned(System.currentTimeMillis()));

        List<String> names = enumerator.expandPrefix("", 50).matches().stream().map(match -> match.name()).collect(Collectors.toList());
        assertFalse("a tombstoned name must not answer a wildcard", names.contains("gone-1"));
        assertTrue(names.contains("stays-1"));
    }

    /** No descriptor ever written is an empty answer, not a failure: every cluster starts there. */
    public void testAnExpansionBeforeAnyDescriptorExistsIsEmpty() throws Exception {
        setUpOver(createTempDir());

        var expansion = enumerator.expandPrefix("tenant-", 10);

        assertFalse(expansion.exceeded());
        assertTrue("no gated index has ever been created, which is a complete answer", expansion.matches().isEmpty());
    }
}
