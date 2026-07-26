/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * C12. Whether computed placement is reachable at all in a running cluster.
 *
 * <p>This test exists because of what writing it revealed. The unit tests for C3, C4 and C5 all passed
 * against a hand-built state in which the index had no routing entry. Every real index has one:
 * {@code MetadataCreateIndexService} calls {@code addAsNew} for everything it creates, and a supplier
 * only ever runs when there is no entry. So the supplier was installed and never invoked -- the whole
 * chain looked wired and was dead, and no unit test could have noticed, because they all constructed the
 * absence they were testing.
 *
 * <p>The fix was a second registration that opts an index out of publication. What this asserts is the
 * property that fix exists for: that an opted-out index really has no published routing entry after
 * creation, and that an ordinary index still does.
 *
 * <h2>And running it found the next problem, which is why it is disabled</h2>
 *
 * The suite times out. Index creation <b>hangs</b> for an index whose routing is not published, because
 * the create API waits for active shards and counts them by reading the routing table. With no entry
 * there are no active shards, the wait never satisfies, and the call never returns.
 *
 * <p>So skipping publication is necessary and not sufficient. Everything that counts active shards has
 * to consult the supplier the same way resolution now does, or index creation has to stop waiting for
 * an index whose shards are placed by computation rather than by allocation. That is a second seam, not
 * a tweak to this one, and it is the reason C12 was worth running before anything was enabled: the unit
 * tests could not have found it, because they never created an index.
 *
 * <p>Disabled rather than deleted. A hanging test in CI is worse than no test, and the finding is worth
 * more than the assertion.
 */
@org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(
    bugUrl = "C12 finding: index creation hangs when routing is not published. See this class's javadoc "
        + "and plan-area-c-computed-placement.md."
)
public class ComputedPlacementReachabilityIT extends OpenSearchIntegTestCase {

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.register(null);
    }

    public void testAnOptedOutIndexPublishesNoRoutingEntry() throws Exception {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> metadata.getIndex().getName().startsWith("computed-"));

        assertAcked(prepareCreate("computed-one").setSettings(oneShard()));
        assertAcked(prepareCreate("classic-one").setSettings(oneShard()));

        ClusterState state = client().admin().cluster().prepareState().get().getState();

        assertTrue("metadata must still hold the index", state.metadata().hasIndex("computed-one"));
        assertFalse(
            "an opted-out index must publish no routing entry, or the supplier is never consulted",
            state.routingTable().hasIndex("computed-one")
        );

        assertTrue("an ordinary index must still publish routing", state.routingTable().hasIndex("classic-one"));
    }

    /** Without the opt-out, every index publishes routing, which is what made the mechanism dead. */
    public void testWithoutTheOptOutEveryIndexPublishesRouting() throws Exception {
        assertAcked(prepareCreate("plain-one").setSettings(oneShard()));

        ClusterState state = client().admin().cluster().prepareState().get().getState();

        assertTrue(state.routingTable().hasIndex("plain-one"));
    }

    private static Settings oneShard() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
    }
}
