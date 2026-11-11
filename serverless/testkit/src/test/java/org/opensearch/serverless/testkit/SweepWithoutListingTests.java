/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A sweep after a merge judges what the last sweep remembered instead of listing the shard's term containers:
 * only the first sweep under a live term lists, and the rule for what goes is unchanged.
 */
public class SweepWithoutListingTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-sweep")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    public void testOnlyTheFirstSweepUnderATermListsAndInheritedFilesAMergeDropsStillGo() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
            try (
                ServerlessNode first = new ServerlessNode(nodeSettings("sweep-first"));
                ServerlessNode successor = new ServerlessNode(nodeSettings("sweep-successor"))
            ) {
                first.startup().start();
                successor.startup().start();

                // Three segments in term 1, which the successor inherits.
                final ShardId onFirst = first.activation().activateWriter(plane, "alpha", 0).orElseThrow();
                final long term1 = plane.heads().read("alpha", 0).orElseThrow().term();
                for (int doc = 0; doc < 3; doc++) {
                    ShardOps.indexDoc(first.reconciler().shard(onFirst), "d" + doc, "{\"msg\":\"kept\",\"n\":" + doc + "}");
                    first.reconciler().shard(onFirst).refresh("sweep");
                }
                final CommitManifest inherited = first.publishShard(onFirst, term1);

                clock.addAndGet(TTL + 1);
                final ShardId onSuccessor = successor.activation().activateWriter(plane, "alpha", 0).orElseThrow();
                final long term2 = plane.heads().read("alpha", 0).orElseThrow().term();
                ShardOps.indexDoc(successor.reconciler().shard(onSuccessor), "d3", "{\"msg\":\"kept\",\"n\":3}");
                successor.reconciler().shard(onSuccessor).refresh("sweep");
                successor.publishShard(onSuccessor, term2);
                // A zombie's unpublished upload, which only a listing can find.
                store.blobContainer(plane.shardData("alpha", 0).add("t=" + term1))
                    .writeBlob("_zz.seg", new ByteArrayInputStream(new byte[] { 1, 2, 3 }), 3L, true);

                try (BackgroundReconciler loop = new BackgroundReconciler(successor, plane)) {
                    store.reset();
                    loop.sweepPublished(Set.of(onSuccessor));
                    assertTrue("the first sweep under a term lists", store.listings() > 0);

                    // The merge drops every inherited file; the next sweeps judge them from memory.
                    successor.reconciler().shard(onSuccessor).forceMerge(new ForceMergeRequest().maxNumSegments(1).flush(true));
                    final CommitManifest merged = successor.publishShard(onSuccessor, term2);
                    store.reset();
                    final Set<String> deleted = new HashSet<>();
                    for (int sweep = 0; sweep < 3; sweep++) {
                        clock.addAndGet(61_000L);
                        deleted.addAll(loop.sweepPublished(Set.of(onSuccessor)));
                    }
                    // A pass still reads the views and snapshots once, whatever the shard count; the shard is not listed.
                    final String places = store.byPlace();
                    assertFalse("no sweep after the first lists the shard: " + places, places.contains("LIST segments/"));
                    assertEquals("one listing each of views and snapshots per pass: " + places, 6L, store.listings());
                    logger.info("three sweeps after a merge: {}", store.byPlace());

                    final Set<String> inheritedBlobs = new HashSet<>();
                    for (String file : inherited.files().keySet()) {
                        inheritedBlobs.add(inherited.location(file).referencedBlob());
                    }
                    for (String file : merged.files().keySet()) {
                        inheritedBlobs.remove(merged.location(file).referencedBlob());
                    }
                    assertFalse("the merge dropped inherited blobs", inheritedBlobs.isEmpty());
                    assertTrue("every dropped inherited blob went: " + deleted, deleted.containsAll(inheritedBlobs));
                    assertTrue("and the zombie's upload, found by the first listing: " + deleted, deleted.contains("t=" + term1 + "/_zz.seg"));
                    for (Map.Entry<String, String> file : merged.files().entrySet()) {
                        assertTrue(
                            "the sweep deleted a file the live commit names: " + file.getKey(),
                            ShardOps.stored(store, plane.shardData("alpha", 0), merged, file.getKey())
                        );
                    }
                    assertEquals(4L, ShardOps.hits(successor.searchService(), onSuccessor, "msg", "kept"));
                }
            }
        }
    }
}
