/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Reclaiming a deleted index's bytes, and refusing to when something still needs them.
 *
 * <p>Index deletion used to reclaim nothing at all from object storage -- it released the clone pin,
 * deregistered the WAL entry, deleted the local disk cache, and left every manifest, bundle and register
 * behind forever, with no engine left anywhere to sweep them later. For a fleet of many short-lived indices
 * that is the dominant storage cost in the system and it only ever grows.
 *
 * <p>The reason it cannot simply be a recursive delete is a clone: its segments physically live in its
 * source's bundles, so a live pin is the signal that says "someone outside this shard is still reading it".
 */
public class DeletedShardReclaimerTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "doomed-idx";
    private static final int SHARD_ID = 0;

    private static final BlobPath SHARD_PATH = BlobPath.cleanPath().add("shard");

    private FsBlobStore blobStore;
    private BlobContainer container;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // A child path rather than the store root, so the assertions below can re-resolve the container after
        // the reclaim has removed it -- the point being that what was under it is gone, not that the
        // directory entry survived.
        blobStore = new FsBlobStore(1024, createTempDir(), false);
        container = blobStore.blobContainer(SHARD_PATH);
        new BlobContainerBundleStore(container).writeBundle(
            BlobContainerBundleStore.NAME_PREFIX + "one",
            List.of(new BundleFileContent("segments_1", "bytes".getBytes("UTF-8")))
        );
    }

    public void testAnUnpinnedDeletedShardsPrefixIsReclaimed() throws Exception {
        assertEquals(
            DeletedShardReclaimer.Outcome.RECLAIMED,
            DeletedShardReclaimer.reclaimShard(INDEX_UUID, SHARD_ID, container, System.currentTimeMillis())
        );
        assertTrue(
            "nothing must be left behind: there is no engine to sweep it later",
            blobStore.blobContainer(SHARD_PATH).listBlobs().isEmpty()
        );
    }

    public void testAShardAClonePinsIsLeftEntirelyAlone() throws Exception {
        new BlobContainerDurablePinRegistry(container).addPin(INDEX_UUID, SHARD_ID, new PinRecord("clone:other-index:0", 1, 1));

        assertEquals(
            DeletedShardReclaimer.Outcome.SKIPPED_PINNED,
            DeletedShardReclaimer.reclaimShard(INDEX_UUID, SHARD_ID, container, System.currentTimeMillis())
        );
        assertFalse(
            "a clone's segments physically live here -- reclaiming this prefix would delete a live index's data",
            container.listBlobsByPrefix(BlobContainerBundleStore.NAME_PREFIX).isEmpty()
        );
    }

    /** An expired pin is how an abandoned operation stops holding storage, so it must not block reclaim either. */
    public void testAnExpiredPinDoesNotBlockReclaim() throws Exception {
        long now = System.currentTimeMillis();
        new BlobContainerDurablePinRegistry(container).addPin(
            INDEX_UUID,
            SHARD_ID,
            new PinRecord("abandoned-snapshot", 1, 1, "some-node", now - 1)
        );

        assertEquals(DeletedShardReclaimer.Outcome.RECLAIMED, DeletedShardReclaimer.reclaimShard(INDEX_UUID, SHARD_ID, container, now));
    }
}
