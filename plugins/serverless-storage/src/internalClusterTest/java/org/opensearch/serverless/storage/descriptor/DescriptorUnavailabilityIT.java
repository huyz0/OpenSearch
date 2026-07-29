/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.DescriptorUnavailableException;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.List;

/**
 * T12. Telling "this index is not there" apart from "I could not find out".
 *
 * <p>{@link DescriptorStore#get} swallowed every exception and returned null, and null already meant no
 * such index. A gated index has no cluster state entry, so the descriptor store is the only thing that can
 * answer whether its name is taken. A node that could not read that store therefore reported <em>every</em>
 * gated index in the cluster as non-existent, with nothing in the answer to say otherwise.
 *
 * <p>The two answers have opposite safe responses. Absent invites the caller to create the index; unknown
 * must not, because creating over an index that already exists is how a tenant loses data.
 *
 * <p>Found by comparing this against how TiDB handles the same situation. Its schema validator returns
 * {@code ResultUnknown} rather than a verdict when it cannot tell, and a node that loses its etcd session
 * stops serving DML outright rather than answering from a stale view. We have no fencing and do not need
 * TiDB's version machinery, since our metadata changes are additive, but conflating unreachable with absent
 * is not a trade, just a wrong answer.
 *
 * <p>The distinction is drawn on the exception rather than guessed at: a missing descriptor <em>index</em>
 * means no gated index has ever been created, so absent is true. Anything else means the index exists and
 * could not be read.
 */
public class DescriptorUnavailabilityIT extends OpenSearchIntegTestCase {

    @After
    public void clearRegistration() {
        AbsentIndexDescriptorSuppliers.register(null);
    }

    /** Before the first gated creation there is no descriptor index, and absent is the true answer. */
    public void testNoDescriptorIndexMeansGenuinelyAbsent() {
        DescriptorStore store = new DescriptorStore(client(), 1);

        assertNull(
            "with no descriptor index at all, nothing has ever been gated, so a name is genuinely free. "
                + "This is the case the original blanket catch was written for and it must keep working",
            store.get("never-created")
        );
    }

    /** A descriptor index that exists but cannot be read must not answer absent. */
    public void testAnUnreadableDescriptorIndexIsNotAnAbsentIndex() {
        DescriptorStore store = new DescriptorStore(client(), 1);
        store.create(descriptor("real-idx"));
        assertNotNull("the premise: this index exists and resolves", store.get("real-idx"));

        // Close the descriptor index. It still exists, so this is "cannot read" rather than "never
        // created", which is exactly the pair the old blanket catch could not tell apart.
        client().admin().indices().prepareClose(DescriptorStore.DESCRIPTOR_INDEX).get();
        store.invalidate("real-idx");

        DescriptorUnavailableException thrown = expectThrows(DescriptorUnavailableException.class, () -> store.get("real-idx"));
        assertTrue(
            "the failure must name the index so an operator can act on it, but was [" + thrown.getMessage() + "]",
            thrown.getMessage().contains("real-idx")
        );
    }

    /**
     * The seam must let that distinction through, or drawing it in the store changes nothing.
     *
     * <p>{@code AbsentIndexDescriptorSuppliers.supply} swallows a throwing supplier by design, which Area C
     * settled: resolution is already a degradation path and a plugin bug should not fail a request. That
     * reasoning holds for a bug and fails for unreachable data, so exactly one type propagates.
     */
    public void testTheSeamPropagatesUnavailabilityButStillSwallowsBugs() {
        AbsentIndexDescriptorSuppliers.register(name -> { throw new DescriptorUnavailableException(name, new RuntimeException("down")); });
        expectThrows(DescriptorUnavailableException.class, () -> AbsentIndexDescriptorSuppliers.supply("some-idx"));

        AbsentIndexDescriptorSuppliers.register(name -> { throw new IllegalStateException("a plugin bug"); });
        assertNull(
            "a supplier bug must still degrade to no answer rather than failing the request, which is what "
                + "Area C settled and what this change deliberately does not undo",
            AbsentIndexDescriptorSuppliers.supply("some-idx")
        );
    }

    private static IndexDescriptor descriptor(String name) {
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
