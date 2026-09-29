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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shard heads are spread over many key prefixes, however alike the index names are.
 *
 * <p>S3 scales request rates per key prefix. Named plainly, heads of indices sharing a stem shared a prefix,
 * which puts a deployment's whole head traffic on one partition long before its size would. Index
 * descriptors are deliberately left unhashed: a pattern like {@code logs-*} is answered by a prefix listing of
 * them, which a hash would break.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessKeyLayoutTests extends OpenSearchTestCase {

    /** A year of daily indices, five shards each, lands on nearly every one of the 256 prefixes. */
    public void testHeadsOfIndicesSharingAStemSpreadOverManyPrefixes() {
        final Set<String> prefixes = new HashSet<>();
        for (int day = 0; day < 365; day++) {
            for (int shard = 0; shard < 5; shard++) {
                final String blob = RegisterMap.shardHeadBlob("logs-2026-" + day, shard);
                assertEquals("the same shard must always map to the same key", blob, RegisterMap.shardHeadBlob("logs-2026-" + day, shard));
                prefixes.add(blob.substring(0, 2));
            }
        }
        logger.info("key layout: 1,825 shard heads of one stem spread over {} prefixes", prefixes.size());
        assertTrue("heads of one stem must spread over most of the 256 prefixes, found " + prefixes.size(), prefixes.size() > 200);
    }

    /** The head a shard's activation writes is the one the store holds under the hashed name. */
    public void testAHeadIsWrittenUnderItsHashedName() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, 30_000L);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, "{}", null));
        assertTrue(plane.heads().acquire("alpha", 0, "node-a", "ephemeral-a").acquired());

        final Set<String> blobs = new HashSet<>(store.blobContainer(RegisterMap.shards(BlobPath.cleanPath())).listBlobs().keySet());
        blobs.removeIf(name -> name.startsWith("extra"));
        assertEquals(Set.of(RegisterMap.shardHeadBlob("alpha", 0)), blobs);
        assertTrue("and that name leads with its hash, not the index name", blobs.iterator().next().matches("[0-9a-f]{2}-alpha#0"));
        assertEquals("node-a", plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
    }
}
