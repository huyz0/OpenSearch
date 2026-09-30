/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.metadata.ConditionalDeleteProbe;
import org.opensearch.serverless.store.ObjectStores;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.util.List;

/**
 * The production wrapper passes through what the backend does well, rather than letting an interface default
 * stand in for it.
 *
 * <p>It did not: the wrapper every production store is built behind overrode neither the conditional delete nor
 * the bounded listings, so the probe failed on the default's refusal and every node stayed on tombstones, and a
 * capped pattern listing listed the whole prefix.
 */
public class MeteredStoreDelegationTests extends OpenSearchTestCase {

    public void testTheConditionalDeleteProbePassesThroughTheWrapper() throws Exception {
        final ObjectStores.Metered metered = new ObjectStores.Metered(new FsBlobStore(1024, createTempDir(), false));
        assertTrue(
            "a store that honours conditional deletes must be seen to through the production wrapper",
            ConditionalDeleteProbe.honoured(metered, BlobPath.cleanPath().add("probe"))
        );
    }

    public void testPagedListingsAreTheBackendsOwnAndCounted() throws Exception {
        final ObjectStores.Metered metered = new ObjectStores.Metered(new FsBlobStore(1024, createTempDir(), false));
        final BlobContainer container = metered.blobContainer(BlobPath.cleanPath().add("names"));
        for (String name : List.of("a", "b", "c", "d")) {
            container.writeBlob(name, new ByteArrayInputStream(new byte[1]), 1, false);
        }
        final long before = metered.impliedS3Requests();
        final List<BlobMetadata> page = container.listBlobsByPrefix("", "b", 1);
        assertEquals(List.of("c"), page.stream().map(BlobMetadata::name).toList());
        assertEquals("one listing, counted", before + 1, metered.impliedS3Requests());
        final BlobContainer root = metered.blobContainer(BlobPath.cleanPath());
        assertEquals(List.of("names"), root.children(null, 5).keySet().stream().filter(n -> n.startsWith("extra") == false).toList());
    }
}
