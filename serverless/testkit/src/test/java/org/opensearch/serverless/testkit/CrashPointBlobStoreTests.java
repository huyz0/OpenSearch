/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.testkit.CrashPointBlobStore.CrashedException;
import org.opensearch.serverless.testkit.CrashPointBlobStore.Fault;
import org.opensearch.serverless.testkit.CrashPointBlobStore.InjectedFailureException;
import org.opensearch.serverless.testkit.CrashPointBlobStore.Kind;
import org.opensearch.serverless.testkit.CrashPointExplorer.Ledger;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// The blob store under test sits on a temp directory, and ExtrasFS drops a stray file into new directories: the
// listings these tests assert on would then name a blob no writer made. An object store never invents a key.
/** The crash-point store does what each fault says, and the explorer catches a writer that is not crash-safe. */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class CrashPointBlobStoreTests extends OpenSearchTestCase {

    private FsBlobStore store() throws IOException {
        return new FsBlobStore(1024, createTempDir(), false);
    }

    private static void put(BlobContainer container, String name, String value, boolean createOnly) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        container.writeBlob(name, new ByteArrayInputStream(bytes), bytes.length, createOnly);
    }

    public void testEachCallIsRecordedWithItsKind() throws Exception {
        final FsBlobStore honest = store();
        final CrashPointBlobStore store = new CrashPointBlobStore(honest);
        final ConditionalBlobContainer container = ConditionalBlobContainer.of(store.blobContainer(BlobPath.cleanPath().add("c")));
        put(container, "a", "1", true);
        put(container, "b", "2", false);
        container.readBlob("a").close();
        container.listBlobsByPrefix("");
        container.createRegisterIfAbsent("r", new BytesArray("x"));
        container.readRegister("r");
        container.deleteBlobsIgnoringIfNotExists(List.of("b"));

        final var kinds = store.steps().stream().map(CrashPointBlobStore.Step::kind).toList();
        assertEquals(List.of(Kind.CREATE, Kind.WRITE, Kind.READ, Kind.LIST, Kind.CREATE_REGISTER, Kind.READ_REGISTER, Kind.DELETE), kinds);
        assertEquals("a", store.steps().get(0).blobName());
    }

    public void testACrashBeforeACallLeavesNothingAndStopsTheWriter() throws Exception {
        final FsBlobStore honest = store();
        final CrashPointBlobStore store = new CrashPointBlobStore(honest).at(0, Fault.CRASH_BEFORE);
        final BlobContainer container = store.blobContainer(BlobPath.cleanPath().add("c"));
        expectThrows(CrashedException.class, () -> put(container, "a", "1", true));
        assertFalse("the store never saw it", honest.blobContainer(BlobPath.cleanPath().add("c")).blobExists("a"));
        expectThrows(CrashedException.class, () -> put(container, "b", "2", true));
        assertFalse("a dead writer writes nothing more", honest.blobContainer(BlobPath.cleanPath().add("c")).blobExists("b"));
        assertEquals(0, store.crashedAt().orElseThrow().index());
    }

    public void testACrashAfterACallKeepsItAndStopsTheWriter() throws Exception {
        final FsBlobStore honest = store();
        final CrashPointBlobStore store = new CrashPointBlobStore(honest).at(0, Fault.CRASH_AFTER);
        final BlobContainer container = store.blobContainer(BlobPath.cleanPath().add("c"));
        expectThrows(CrashedException.class, () -> put(container, "a", "1", true));
        assertTrue("the request landed", honest.blobContainer(BlobPath.cleanPath().add("c")).blobExists("a"));
        expectThrows(CrashedException.class, () -> container.readBlob("a"));
    }

    public void testAnAmbiguousCallLandsAndTheWriterCarriesOn() throws Exception {
        final FsBlobStore honest = store();
        final CrashPointBlobStore store = new CrashPointBlobStore(honest).on(s -> s.kind() == Kind.CREATE, Fault.AMBIGUOUS, 1);
        final ConditionalBlobContainer container = ConditionalBlobContainer.of(store.blobContainer(BlobPath.cleanPath().add("c")));
        expectThrows(InjectedFailureException.class, () -> put(container, "a", "1", true));
        assertTrue(honest.blobContainer(BlobPath.cleanPath().add("c")).blobExists("a"));
        put(container, "b", "2", true);
        assertTrue("only the first matching call was faulted", honest.blobContainer(BlobPath.cleanPath().add("c")).blobExists("b"));
        assertTrue(store.crashedAt().isEmpty());
    }

    public void testAFailedCallDoesNotLandAndTheWriterCarriesOn() throws Exception {
        final FsBlobStore honest = store();
        final CrashPointBlobStore store = new CrashPointBlobStore(honest).at(0, Fault.FAIL_BEFORE);
        final ConditionalBlobContainer container = ConditionalBlobContainer.of(store.blobContainer(BlobPath.cleanPath().add("c")));
        expectThrows(InjectedFailureException.class, () -> container.createRegisterIfAbsent("r", new BytesArray("x")));
        assertTrue(ConditionalBlobContainer.of(honest.blobContainer(BlobPath.cleanPath().add("c"))).readRegister("r").isEmpty());
        assertTrue(container.createRegisterIfAbsent("r", new BytesArray("x")).applied());
    }

    /** A writer that acknowledges before its write lands. The explorer must find the crash point that loses it. */
    public void testTheExplorerCatchesAWriterThatAcknowledgesBeforeWriting() throws Exception {
        final FsBlobStore honest = store();
        final AssertionError caught = expectThrows(
            AssertionError.class,
            () -> CrashPointExplorer.exploreAll(honest, BlobPath.cleanPath().add("explore"), (store, root, ledger) -> {
                final BlobContainer log = store.blobContainer(root);
                for (int i = 0; i < 3; i++) {
                    ledger.attempted("w" + i);
                    ledger.acknowledged("w" + i);
                    put(log, "w" + i, "w" + i, true);
                }
            }, (store, root, ledger, where) -> ledger.assertRecovered(recovered(store, root), where))
        );
        assertTrue(caught.getMessage(), caught.getMessage().contains("acknowledged writes were lost"));
    }

    /** The same writer, acknowledging only after the write returns, survives every fault at every call. */
    public void testTheExplorerAcceptsAWriterThatAcknowledgesAfterWriting() throws Exception {
        final FsBlobStore honest = store();
        final var result = CrashPointExplorer.exploreAll(honest, BlobPath.cleanPath().add("explore"), (store, root, ledger) -> {
            final BlobContainer log = store.blobContainer(root);
            for (int i = 0; i < 3; i++) {
                ledger.attempted("w" + i);
                put(log, "w" + i, "w" + i, true);
                ledger.acknowledged("w" + i);
            }
        }, (store, root, ledger, where) -> ledger.assertRecovered(recovered(store, root), where));
        assertEquals(3, result.recorded().size());
        assertEquals("every fault at every call", 3 * Fault.values().length, result.explored().size());
    }

    public void testTheExplorerRefusesANondeterministicScenario() throws Exception {
        final FsBlobStore honest = store();
        final int[] runs = new int[1];
        final AssertionError caught = expectThrows(
            AssertionError.class,
            () -> CrashPointExplorer.exploreAll(honest, BlobPath.cleanPath().add("explore"), (store, root, ledger) -> {
                final BlobContainer log = store.blobContainer(root);
                put(log, "first-" + (runs[0]++ == 0 ? "a" : "b"), "x", true);
            }, (store, root, ledger, where) -> {})
        );
        assertTrue(caught.getMessage(), caught.getMessage().contains("not deterministic"));
    }

    public void testCallsThroughAListedChildAreRecordedAndFaulted() throws Exception {
        final FsBlobStore honest = store();
        put(honest.blobContainer(BlobPath.cleanPath().add("p").add("child")), "x", "1", true);
        final CrashPointBlobStore store = new CrashPointBlobStore(honest).on(s -> s.blobName().equals("y"), Fault.CRASH_BEFORE, 1);
        final BlobContainer child = store.blobContainer(BlobPath.cleanPath().add("p")).children().get("child");
        child.listBlobs();
        expectThrows(CrashedException.class, () -> put(child, "y", "2", true));
        assertEquals(List.of(Kind.LIST, Kind.LIST, Kind.CREATE), store.steps().stream().map(CrashPointBlobStore.Step::kind).toList());
    }

    public void testACrashAfterACallTheStoreRefusedStillStopsTheWriter() throws Exception {
        final FsBlobStore honest = store();
        put(honest.blobContainer(BlobPath.cleanPath().add("c")), "taken", "1", true);
        final CrashPointBlobStore store = new CrashPointBlobStore(honest).at(0, Fault.CRASH_AFTER);
        final BlobContainer container = store.blobContainer(BlobPath.cleanPath().add("c"));
        final CrashedException crashed = expectThrows(CrashedException.class, () -> put(container, "taken", "2", true));
        assertTrue("the store's own answer is kept as the cause", crashed.getCause() instanceof java.nio.file.FileAlreadyExistsException);
        expectThrows(CrashedException.class, () -> container.listBlobs());
    }

    public void testTheExplorerReportsAWriterThatFailsForAnotherReason() throws Exception {
        final FsBlobStore honest = store();
        final AssertionError caught = expectThrows(
            AssertionError.class,
            () -> CrashPointExplorer.exploreAll(honest, BlobPath.cleanPath().add("explore"), (store, root, ledger) -> {
                final BlobContainer log = store.blobContainer(root);
                try {
                    put(log, "w", "w", true);
                } catch (IOException e) {
                    throw new IllegalStateException("a bug that drops the cause");
                }
            }, (store, root, ledger, where) -> {})
        );
        assertTrue(caught.getMessage(), caught.getMessage().contains("failed for another reason"));
    }

    public void testALedgerRefusesAnAcknowledgementWithoutAnAttempt() {
        final Ledger ledger = new Ledger();
        expectThrows(IllegalStateException.class, () -> ledger.acknowledged("never-sent"));
    }

    private static Set<String> recovered(org.opensearch.common.blobstore.BlobStore store, BlobPath root) throws IOException {
        return new HashSet<>(store.blobContainer(root).listBlobs().keySet());
    }
}
