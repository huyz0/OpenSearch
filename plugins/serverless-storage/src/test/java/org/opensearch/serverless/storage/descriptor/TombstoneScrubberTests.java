/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;

/**
 * Tombstones are reclaimed once they are past the window, and never before.
 *
 * <h2>The two ways this can be wrong, and they are not symmetric</h2>
 *
 * Reclaiming late costs storage. Reclaiming early costs a node the record it needed to drop stale shard data,
 * so every ambiguous case here has to resolve towards keeping. That is why an unknown age is treated as young
 * rather than as ancient, and why an unreadable tombstone is kept rather than swept.
 *
 * <p>The zero case is the one worth pinning hardest, because the natural implementation gets it backwards.
 * {@link IndexDescriptor#deletedAtMillis()} is zero for a tombstone written before that field existed, and a
 * plain {@code deletedAt < cutoff} reads zero as the epoch, concludes ancient, and deletes every one of them
 * on the first pass.
 */
public class TombstoneScrubberTests extends OpenSearchTestCase {

    private static final long DAY = TimeUnit.DAYS.toMillis(1);

    private FsBlobStore store;
    private BlobDescriptorBackend backend;
    private TombstoneScrubber scrubber;
    private long now;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        store = new FsBlobStore(1024, createTempDir(), false);
        backend = new BlobDescriptorBackend(store.blobContainer(BlobPath.cleanPath()));
        now = TimeUnit.DAYS.toMillis(20_000);
        scrubber = new TombstoneScrubber(store::blobContainer, BlobPath.cleanPath(), () -> now);
    }

    private static IndexDescriptor descriptor(String name) {
        return IndexDescriptor.from(
            IndexMetadata.builder(name)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                        .build()
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build()
        );
    }

    private void tombstone(String name, long deletedAtMillis) {
        backend.create(descriptor(name));
        backend.putTombstoneAsync(descriptor(name).tombstoned(deletedAtMillis));
    }

    public void testATombstonePastTheWindowIsReclaimed() {
        tombstone("tenant-old", now - 10 * DAY);

        assertEquals(1, scrubber.scrubOnce(7 * DAY));
        assertNull("reclaimed means the name resolves as never having existed", backend.get("tenant-old"));
    }

    public void testATombstoneInsideTheWindowIsKept() {
        tombstone("tenant-recent", now - DAY);

        assertEquals(0, scrubber.scrubOnce(7 * DAY));

        IndexDescriptor still = backend.get("tenant-recent");
        assertNotNull(still);
        assertFalse("and it still reads as deleted rather than absent", still.exists());
    }

    /**
     * A tombstone with no recorded age is kept, however old the window is.
     *
     * <p>This is the case a naive comparison deletes on its first pass, and the one with the worst
     * consequence: every tombstone written before the field existed would go at once, across the whole
     * population.
     */
    public void testAnUndatedTombstoneIsNeverReclaimed() {
        tombstone("tenant-undated", 0L);

        assertEquals("unknown age is young, not ancient", 0, scrubber.scrubOnce(1));

        IndexDescriptor still = backend.get("tenant-undated");
        assertNotNull(still);
        assertFalse(still.exists());
    }

    /** Scrubbing is idempotent, since it runs on a timer and a pass that runs twice must not differ. */
    public void testScrubbingTwiceIsTheSameAsOnce() {
        tombstone("tenant-old", now - 10 * DAY);

        assertEquals(1, scrubber.scrubOnce(7 * DAY));
        assertEquals("nothing left the second time", 0, scrubber.scrubOnce(7 * DAY));
    }

    /** An empty tombstone space is an ordinary state, not a failure, and costs one listing. */
    public void testAnEmptyTombstoneSpaceIsFine() {
        assertEquals(0, scrubber.scrubOnce(7 * DAY));
    }

    /**
     * Reclaiming one tombstone leaves every other alone, including live descriptors.
     *
     * <p>The live check matters because the scrubber walks a keyspace rather than a curated list: if it ever
     * pointed at the descriptor space by mistake it would be deleting indices, so it refuses anything that
     * reads as existing regardless of where it found it.
     */
    public void testOnlyThePastWindowTombstoneGoes() {
        tombstone("tenant-old", now - 10 * DAY);
        tombstone("tenant-recent", now - DAY);
        backend.create(descriptor("tenant-live"));

        assertEquals(1, scrubber.scrubOnce(7 * DAY));

        assertNull(backend.get("tenant-old"));
        assertNotNull(backend.get("tenant-recent"));
        IndexDescriptor live = backend.get("tenant-live");
        assertNotNull(live);
        assertTrue("a live index must be untouched by tombstone reclamation", live.exists());
    }

    /**
     * The scrubber finds tombstones on a filesystem store, which the obvious implementation would not.
     *
     * <p>{@code listBlobsByPrefix("tombstones/")} on the root container returns every tombstone on an object
     * store, where keys are flat, and nothing at all on a filesystem repository, where the slash is a
     * directory separator and the listing iterates one level. A scrubber built that way passes review, logs
     * nothing and frees nothing. This asserts the child-container addressing that works on both, by the only
     * evidence that counts: something was actually reclaimed here.
     */
    public void testTheScrubberCanSeeTombstonesOnAFilesystemStore() {
        tombstone("tenant-old", now - 10 * DAY);

        assertEquals("zero here would mean the keyspace is invisible rather than empty", 1, scrubber.scrubOnce(7 * DAY));
    }
}
