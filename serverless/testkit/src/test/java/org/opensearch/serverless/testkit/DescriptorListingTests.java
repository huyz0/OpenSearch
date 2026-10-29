/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Inventories share a register namespace with aliases, but return only index descriptors. */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class DescriptorListingTests extends OpenSearchTestCase {
    public void testAliasesAndStreamsDoNotBreakIndexInventories() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
            for (String name : List.of("logs-1", "logs-3", "logs-5")) {
                metadata.createIndex(new IndexDescriptor(name, "uuid-" + name, 1, "{}", null));
            }
            for (String name : List.of("logs-0", "logs-2", "logs-4", "logs-6")) {
                metadata.createAlias(new AliasRecord(name, List.of("logs-1")));
            }
            metadata.createAlias(new AliasRecord("logs-7", List.of("logs-3"), true, 1L, "@timestamp"));
            for (int limit : new int[] { 1, 2, 3, 10 }) {
                final var names = new ArrayList<String>();
                String cursor = null;
                int pages = 0;
                do {
                    final var page = metadata.descriptors().listPage("logs-", cursor, limit);
                    names.addAll(page.descriptors().keySet());
                    cursor = page.nextAfter();
                    assertTrue("the cursor must advance past skipped aliases", ++pages <= 4);
                } while (cursor != null);
                assertEquals(List.of("logs-1", "logs-3", "logs-5"), names);
            }
            assertEquals(Set.of("logs-1", "logs-3", "logs-5"), metadata.descriptors().listAll().keySet());
        }
    }

    public void testMaximumIntegerLimitsDoNotOverflowOnSmallInventories() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
            metadata.createIndex(new IndexDescriptor("logs-1", "uuid-one", 1, "{}", null));
            final var page = metadata.descriptors().listPage("logs-", null, Integer.MAX_VALUE);
            assertEquals(Set.of("logs-1"), page.descriptors().keySet());
            assertFalse(page.hasMore());
            assertEquals(List.of("logs-1"), metadata.namesWithPrefix("logs-", Integer.MAX_VALUE));
        }
    }
}
