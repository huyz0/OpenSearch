/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.MultiSearchAction;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

/**
 * Unit coverage for {@link AffinityForwardingActionFilter#rawRequestedNames}: the cheap, no-I/O
 * pre-check that decides whether a request is even worth the GENERIC dispatch that does the real
 * (and possibly blocking) resolution. Deliberately does not cover {@code resolveSoleAffinityTarget}
 * or the forwarding decision itself here -- those need a real cluster state and node set, exercised
 * end-to-end in {@code ServerlessStorageAffinityForwardingIT} instead.
 */
public class AffinityForwardingActionFilterTests extends OpenSearchTestCase {

    public void testSearchRequestReturnsItsIndices() {
        SearchRequest request = new SearchRequest("a", "b");
        assertEquals(Set.of("a", "b"), AffinityForwardingActionFilter.rawRequestedNames(SearchAction.NAME, request));
    }

    public void testSearchRequestWithNoIndicesReturnsEmpty() {
        SearchRequest request = new SearchRequest();
        assertTrue(AffinityForwardingActionFilter.rawRequestedNames(SearchAction.NAME, request).isEmpty());
    }

    public void testBulkRequestAggregatesDistinctItemIndices() {
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("a").id("1").source("f", "v"));
        request.add(new IndexRequest("b").id("2").source("f", "v"));
        request.add(new IndexRequest("a").id("3").source("f", "v"));
        assertEquals(Set.of("a", "b"), AffinityForwardingActionFilter.rawRequestedNames(BulkAction.NAME, request));
    }

    public void testMultiSearchRequestAggregatesAcrossSubRequests() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest("a"));
        request.add(new SearchRequest("b", "c"));
        assertEquals(Set.of("a", "b", "c"), AffinityForwardingActionFilter.rawRequestedNames(MultiSearchAction.NAME, request));
    }

    public void testMultiSearchRequestWithAnIndexlessSubRequestBailsOutEntirely() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest("a"));
        request.add(new SearchRequest()); // no index named -- "search everything"
        assertTrue(
            "one sub-request with no target should void the whole batch's affinity check, "
                + "not just be skipped, since the batch as a whole has no single affine target",
            AffinityForwardingActionFilter.rawRequestedNames(MultiSearchAction.NAME, request).isEmpty()
        );
    }

    public void testUnhandledActionReturnsEmpty() {
        SearchRequest request = new SearchRequest("a");
        assertTrue(AffinityForwardingActionFilter.rawRequestedNames("indices:data/read/get", request).isEmpty());
    }
}
