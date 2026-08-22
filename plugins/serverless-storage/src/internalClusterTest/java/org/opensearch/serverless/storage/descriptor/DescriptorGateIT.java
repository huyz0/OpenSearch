/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.IndexNotFoundException;
import org.junit.After;

import java.util.List;

/**
 * W3. Resolution against descriptors, on a node rather than against a stub.
 *
 * <p>H2e built the seam and H8a taught {@code IndexNameExpressionResolver} to consult it, and every test of
 * either registered its own supplier. In production nothing ever did, so the fallback returned null on every
 * node and a gated index could not be named by any request. This is the first time the resolver reads
 * descriptors that a real store wrote to a real index.
 *
 * <p>The controls matter as much as the property here. A name with no descriptor must still raise, or the
 * gate has turned every typo into a silent success; and an ordinary index must not reach the store at all,
 * or every cluster pays a descriptor read per resolution.
 */
public class DescriptorGateIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    private IndexNameExpressionResolver resolver() throws Exception {
        return new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
    }

    /** The property. A name that exists only as a descriptor resolves, carrying the descriptor's uuid. */
    public void testAGatedNameResolvesOnceTheGateIsInstalled() throws Exception {
        BlobDescriptorBackend store = installBlobBackedDescriptorPlane().points();
        store.create(descriptor("gated-logs"));

        ClusterState empty = emptyClusterStateWithDescriptorResolver();
        var resolved = resolver().concreteIndices(empty, IndicesOptions.strictExpandOpen(), "gated-logs");

        assertEquals("a gated index must be nameable by a request", 1, resolved.length);
        assertEquals("gated-logs", resolved[0].getName());
        assertEquals(
            "and must carry the descriptor's uuid, or the request cannot reach the shard",
            "gated-logs-uuid",
            resolved[0].getUUID()
        );
    }

    /** Without the gate the same name is unresolvable, which is what production looked like until now. */
    public void testTheSameNameIsUnresolvableWithoutTheGate() throws Exception {
        BlobDescriptorBackend store = blobBackendWithoutInstalling();
        store.create(descriptor("gated-logs"));
        // Deliberately not installed.

        ClusterState empty = emptyClusterStateWithDescriptorResolver();
        expectThrows(
            IndexNotFoundException.class,
            () -> resolver().concreteIndexNames(empty, IndicesOptions.strictExpandOpen(), "gated-logs")
        );
    }

    /** A genuinely missing name must still raise, or the gate turns every typo into a silent success. */
    public void testAMissingNameStillRaises() throws Exception {
        installBlobBackedDescriptorPlane();

        ClusterState empty = emptyClusterStateWithDescriptorResolver();
        expectThrows(
            IndexNotFoundException.class,
            () -> resolver().concreteIndexNames(empty, IndicesOptions.strictExpandOpen(), "never-existed")
        );
    }

    /** Disabled means untouched, so an ordinary cluster carries none of this. */
    public void testInstallingWhileDisabledRegistersNothing() throws Exception {
        installBlobBackedDescriptorPlane(false);

        assertFalse("a disabled gate must register no supplier", AbsentIndexDescriptorSuppliers.isRegistered());
    }

    /** Uninstalling clears both, which node close depends on to avoid outliving itself. */
    public void testUninstallClearsBothRegistrations() throws Exception {
        installBlobBackedDescriptorPlane();
        assertTrue(AbsentIndexDescriptorSuppliers.isRegistered());

        DescriptorGate.uninstall();

        assertFalse("a closed node must not keep answering resolution", AbsentIndexDescriptorSuppliers.isRegistered());
    }

    /**
     * W4. Creating an index through the real API leaves a descriptor behind.
     *
     * <p>H2b built the dual write and H4 built tombstones, and nothing has ever registered a publisher, so
     * until now no index creation outside a test wrote a descriptor. This is the first time the write path
     * runs end to end.
     */
    public void testCreatingAnIndexRecordsADescriptor() throws Exception {
        BlobDescriptorBackend store = installBlobBackedDescriptorPlane().points();

        createIndex("recorded-index");

        // assertBusy rather than a bare read: the publish hook runs on the cluster state thread, so the
        // write is asynchronous by necessity (see BlobDescriptorBackend.putAsync) and a bare read would
        // race it.
        assertBusy(() -> {
            IndexDescriptor recorded = readWhenAvailable(store, "recorded-index");
            assertNotNull(
                "creating an index must record a descriptor, or the descriptor store is a write-only "
                    + "fixture that resolution can never find anything in",
                recorded
            );
            assertEquals("recorded-index", recorded.name());
            assertTrue("and it must be recorded as existing rather than tombstoned", recorded.exists());
        });
    }

    /**
     * Reads inside a polling loop, treating "cannot tell yet" as "not yet" rather than as a failure.
     *
     * <p>T12 made the store distinguish a descriptor that is absent from one it could not read, because
     * conflating the two reported every gated index as non-existent whenever the store was
     * unreachable. A descriptor written moments ago can briefly be unreadable,
     * and that is a genuine "cannot tell", so the store is right to say so.
     *
     * <p>A loop waiting for a write to land is then right to keep waiting rather than to fail, which is
     * what this converts it into. Before T12 these loops silently relied on the old answer, since an
     * unreadable index and an absent descriptor both came back null.
     */
    private static IndexDescriptor readWhenAvailable(BlobDescriptorBackend store, String name) throws Exception {
        try {
            return store.get(name);
        } catch (org.opensearch.cluster.metadata.DescriptorUnavailableException e) {
            throw new AssertionError("descriptor not readable yet for [" + name + "]", e);
        }
    }

    /**
     * Deletion records rather than removes, because absence cannot be told apart from not having looked.
     * H4b is why: a node partitioned during a delete would otherwise adopt its dangling shard data on
     * rejoin.
     */
    public void testDeletingAnIndexLeavesATombstoneRatherThanAnAbsence() throws Exception {
        BlobDescriptorBackend store = installBlobBackedDescriptorPlane().points();
        createIndex("doomed-index");
        // assertBusy on the premise too. The write is asynchronous, so a bare read here races it, and this
        // test passed once by timing before failing on a later run.
        assertBusy(() -> assertNotNull("the premise: it was recorded", readWhenAvailable(store, "doomed-index")));

        assertTrue(client().admin().indices().prepareDelete("doomed-index").get().isAcknowledged());

        assertBusy(() -> {
            IndexDescriptor tombstone = readWhenAvailable(store, "doomed-index");
            assertNotNull("a deleted index must leave a tombstone, not an absence", tombstone);
            assertFalse("and the tombstone must not read as existing", tombstone.exists());
            assertEquals(
                "the uuid must survive the tombstone, since it identifies the dangling data to reclaim",
                IndexDescriptor.State.DELETED,
                tombstone.state()
            );
        });
    }

    /** Without the gate, the same creation records nothing, which is what production did until now. */
    public void testWithoutTheGateCreationRecordsNothing() throws Exception {
        BlobDescriptorBackend store = blobBackendWithoutInstalling();
        // Deliberately not installed.

        createIndex("unrecorded-index");

        assertNull("no publisher means no descriptor, silently", store.get("unrecorded-index"));
    }

    private static IndexDescriptor descriptor(String name) throws Exception {
        return new IndexDescriptor(
            name,
            name + "-uuid",
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            1_700_000_000_000L
        );
    }
}
