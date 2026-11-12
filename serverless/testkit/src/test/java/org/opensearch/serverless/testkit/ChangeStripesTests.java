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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.ChangeStripes;
import org.opensearch.serverless.metadata.ChangeStripes.Kind;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.shell.StaleIncarnationException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The change stripes: what proves an index's descriptor unchanged since an earlier read, and a write path that confirms
 * incarnations by them and still refuses a write into an index deleted elsewhere.
 */
public class ChangeStripesTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";
    private static final long TTL = 30_000L;

    /** Two names in the same stripe. */
    private static String[] twoInOneStripe() {
        final String first = "alpha";
        for (int i = 0;; i++) {
            final String other = "other-" + i;
            if (ChangeStripes.stripeOf(other) == ChangeStripes.stripeOf(first)) {
                return new String[] { first, other };
            }
        }
    }

    private ChangeStripes stripes() throws java.io.IOException {
        return new ChangeStripes(new FsBlobStore(1024, createTempDir(), false).blobContainer(BlobPath.cleanPath().add("index-changes")));
    }

    public void testAChangeToAnotherIndexInTheStripeLeavesAnIndexConfirmed() throws Exception {
        final String[] names = twoInOneStripe();
        final ChangeStripes stripes = stripes();
        final int stripe = ChangeStripes.stripeOf(names[0]);
        final ChangeStripes.Snapshot before = stripes.read(stripe);
        stripes.bump(names[1], Kind.CHANGED);
        final ChangeStripes.Snapshot after = stripes.read(stripe);
        assertTrue(after.unchangedSince(names[0], before.epoch(), before.generation()));
        assertFalse(after.unchangedSince(names[1], before.epoch(), before.generation()));
    }

    public void testAnUnfinishedDeleteIsNeverConfirmedUntilItFinishes() throws Exception {
        final ChangeStripes stripes = stripes();
        final int stripe = ChangeStripes.stripeOf("alpha");
        stripes.bump("alpha", Kind.PENDING_DELETE);
        // A reader that read the stripe now, then the descriptor (still live), must not be able to rely on it later.
        final ChangeStripes.Snapshot during = stripes.read(stripe);
        assertFalse(during.unchangedSince("alpha", during.epoch(), during.generation()));
        // Not even after many other changes push the pending entry far behind.
        final String other = twoInOneStripe()[1];
        for (int i = 0; i < 300; i++) {
            stripes.bump(other, Kind.CHANGED);
        }
        final ChangeStripes.Snapshot later = stripes.read(stripe);
        assertFalse("an unfinished delete is kept", later.unchangedSince("alpha", later.epoch(), later.generation()));
        stripes.bump("alpha", Kind.DELETED);
        final ChangeStripes.Snapshot finished = stripes.read(stripe);
        assertTrue(finished.unchangedSince("alpha", finished.epoch(), finished.generation()));
        assertFalse("but the delete itself is a change", finished.unchangedSince("alpha", later.epoch(), later.generation()));
    }

    public void testAnUpdateDoesNotFinishADelete() throws Exception {
        final ChangeStripes stripes = stripes();
        stripes.bump("alpha", Kind.PENDING_DELETE);
        stripes.bump("alpha", Kind.CHANGED);
        final ChangeStripes.Snapshot read = stripes.read(ChangeStripes.stripeOf("alpha"));
        assertFalse(read.unchangedSince("alpha", read.epoch(), read.generation()));
    }

    public void testAReadOlderThanEveryKeptChangeIsNotConfirmed() throws Exception {
        final String[] names = twoInOneStripe();
        final ChangeStripes stripes = stripes();
        final int stripe = ChangeStripes.stripeOf(names[0]);
        final ChangeStripes.Snapshot before = stripes.read(stripe);
        for (int i = 0; i < 300; i++) {
            stripes.bump(names[1], Kind.CHANGED);
        }
        // The changes since that read are no longer all kept, so one of them might have named it.
        assertFalse(stripes.read(stripe).unchangedSince(names[0], before.epoch(), before.generation()));
    }

    public void testTooManyUnfinishedDeletesStartANewEpoch() throws Exception {
        final ChangeStripes stripes = stripes();
        final int stripe = ChangeStripes.stripeOf("alpha");
        final ChangeStripes.Snapshot before = stripes.read(stripe);
        int bumped = 0;
        for (int i = 0; bumped <= 64; i++) {
            final String name = "gone-" + i;
            if (ChangeStripes.stripeOf(name) == stripe) {
                stripes.bump(name, Kind.PENDING_DELETE);
                bumped++;
            }
        }
        final ChangeStripes.Snapshot after = stripes.read(stripe);
        assertNotEquals(before.epoch(), after.epoch());
        assertFalse(after.unchangedSince("alpha", before.epoch(), before.generation()));
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-stripes")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    public void testAWriteConfirmedByItsStripeIsRefusedOnceTheIndexIsDeletedElsewhere() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore fs = new FsBlobStore(1024, createTempDir(), false);
        try (CountingBlobStore store = new CountingBlobStore(fs)) {
            final MetadataPlane mine = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            final MetadataPlane theirs = new MetadataPlane(fs, BlobPath.cleanPath(), clock::get, TTL);
            theirs.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
            try (ServerlessNode node = new ServerlessNode(nodeSettings("stripes-delete"))) {
                node.startup().start();
                node.setMetadataPlane(mine);
                final BackgroundReconciler loop = new BackgroundReconciler(node, mine);
                loop.want("alpha", 0);
                loop.tick(clock.get());
                final ShardId shardId = node.reconciler().openShards().iterator().next();
                node.fences().setIncarnationFenceMillis(200L);

                node.writes().index(shardId, "1", "{\"msg\":\"first\"}");
                TimeUnit.MILLISECONDS.sleep(300);
                // Past the window: read by descriptor, which notes where the stripe stood.
                node.writes().index(shardId, "1b", "{\"msg\":\"by descriptor\"}");
                TimeUnit.MILLISECONDS.sleep(300);
                store.traceCallers(0);
                store.reset();
                node.writes().index(shardId, "2", "{\"msg\":\"by stripe\"}");
                assertFalse(
                    "a write whose stripe names no change reads no descriptor:\n" + store.byPlace(),
                    store.byPlace().contains("indices/")
                );

                theirs.deleteIndex("alpha");
                // The delete answers after its settle wait, longer than the window.
                TimeUnit.MILLISECONDS.sleep(300);
                final Exception refused = expectThrows(Exception.class, () -> node.writes().index(shardId, "3", "{\"msg\":\"after\"}"));
                assertNotNull(
                    "refused as a stale incarnation: " + refused,
                    org.opensearch.ExceptionsHelper.unwrap(refused, StaleIncarnationException.class)
                );
            }
        }
    }
}
