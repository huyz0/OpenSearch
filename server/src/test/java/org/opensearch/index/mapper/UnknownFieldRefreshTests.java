/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * W14. The trigger that replaces H6c's stamped generation.
 *
 * <p>W13 found that no request carries a mapping generation, so a shard cannot be told its mapping moved.
 * It can only discover it by meeting a field it does not know, which is exactly and only when being behind
 * matters.
 *
 * <p>The properties asserted here are the ones that decide whether this is cheaper than a broadcast, which
 * was H6c's whole claim. They are asserted on the seam rather than through a document, because what matters
 * is how often the refresher is consulted and what happens when it fails, and counting is the only way to
 * tell "did not need to read" from "read and got the same answer".
 */
public class UnknownFieldRefreshTests extends OpenSearchTestCase {

    @After
    public void clearRegistration() {
        UnknownFieldRefresh.register(null);
    }

    /** With nothing registered the seam is inert, so an ordinary cluster carries none of this. */
    public void testNothingRegisteredMeansNoRefresh() {
        assertFalse("an unregistered seam must not claim to have found anything", UnknownFieldRefresh.isRegistered());
        assertFalse(UnknownFieldRefresh.refreshed(null, "idx-uuid", "field"));
    }

    /** A registered refresher is consulted, and its answer is what the caller acts on. */
    public void testARegisteredRefresherDecidesWhetherTheFieldIsKnown() {
        UnknownFieldRefresh.register((mapperService, indexUuid, fieldName) -> "known".equals(fieldName));

        assertTrue("a field the refresher finds must be reported as known", UnknownFieldRefresh.refreshed(null, "idx-uuid", "known"));
        assertFalse(
            "and one it does not find must be reported absent, so the caller rejects or infers exactly as " + "it did before this existed",
            UnknownFieldRefresh.refreshed(null, "idx-uuid", "genuinely-new")
        );
    }

    /** The uuid and field name both reach the refresher, since it needs them to avoid pointless reads. */
    public void testTheRefresherReceivesTheIndexAndTheField() {
        StringBuilder seen = new StringBuilder();
        UnknownFieldRefresh.register((mapperService, indexUuid, fieldName) -> {
            seen.append(indexUuid).append('/').append(fieldName);
            return false;
        });

        UnknownFieldRefresh.refreshed(null, "some-uuid", "some-field");

        assertEquals("some-uuid/some-field", seen.toString());
    }

    /**
     * A failing refresher must not fail the document. This sits on the indexing path, and the caller's
     * existing behaviour of rejecting or inferring is a correct outcome while a failed write is not.
     */
    public void testAFailingRefresherIsTreatedAsFindingNothing() {
        UnknownFieldRefresh.register((mapperService, indexUuid, fieldName) -> { throw new IllegalStateException("store down"); });

        assertFalse(
            "a store hiccup must not turn every document carrying a new field into an error",
            UnknownFieldRefresh.refreshed(null, "idx-uuid", "field")
        );
    }

    /**
     * One consultation per miss, which is the property that makes this cheaper than a broadcast. A design
     * that consulted the store per field, or per document, would be correct and useless.
     */
    public void testEachMissConsultsTheRefresherExactlyOnce() {
        AtomicInteger consultations = new AtomicInteger();
        UnknownFieldRefresh.register((mapperService, indexUuid, fieldName) -> {
            consultations.incrementAndGet();
            return false;
        });

        UnknownFieldRefresh.refreshed(null, "idx-uuid", "field");

        assertEquals("a single miss must cost a single consultation", 1, consultations.get());
    }
}
