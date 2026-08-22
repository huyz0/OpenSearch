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

import java.util.ArrayList;
import java.util.List;
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
        tombstone("serverless_tenant-old", now - 10 * DAY);

        assertEquals(1, scrubber.scrubOnce(7 * DAY));
        assertNull("reclaimed means the name resolves as never having existed", backend.get("serverless_tenant-old"));
    }

    public void testATombstoneInsideTheWindowIsKept() {
        tombstone("serverless_tenant-recent", now - DAY);

        assertEquals(0, scrubber.scrubOnce(7 * DAY));

        IndexDescriptor still = backend.get("serverless_tenant-recent");
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
        tombstone("serverless_tenant-undated", 0L);

        assertEquals("unknown age is young, not ancient", 0, scrubber.scrubOnce(1));

        IndexDescriptor still = backend.get("serverless_tenant-undated");
        assertNotNull(still);
        assertFalse(still.exists());
    }

    /** Scrubbing is idempotent, since it runs on a timer and a pass that runs twice must not differ. */
    public void testScrubbingTwiceIsTheSameAsOnce() {
        tombstone("serverless_tenant-old", now - 10 * DAY);

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
        tombstone("serverless_tenant-old", now - 10 * DAY);
        tombstone("serverless_tenant-recent", now - DAY);
        backend.create(descriptor("serverless_tenant-live"));

        assertEquals(1, scrubber.scrubOnce(7 * DAY));

        assertNull(backend.get("serverless_tenant-old"));
        assertNotNull(backend.get("serverless_tenant-recent"));
        IndexDescriptor live = backend.get("serverless_tenant-live");
        assertNotNull(live);
        assertTrue("a live index must be untouched by tombstone reclamation", live.exists());
    }

    /**
     * A tombstone refreshed after the pass began is not reclaimed on the strength of what was read first.
     *
     * <p>This is the realistic shape of the delete-recreate-delete race. The scrubber lists the keyspace,
     * and by the time it reaches a given name that name may have been deleted again, replacing an ancient
     * tombstone with a new one. Deleting it then would strip the record a node needs for the <em>second</em>
     * deletion, which is the one still inside its window.
     *
     * <p>Covered by re-reading each candidate immediately before deciding, rather than trusting the listing.
     * What remains uncovered is the gap between that read and the delete itself, which cannot be closed
     * without a conditional delete the blob store does not offer, and is stated in the scrubber rather than
     * implied. This test pins the part that is closed; the part that is not is microseconds wide and costs a
     * node stale shard data rather than an index returning.
     */
    public void testATombstoneRefreshedDuringThePassIsKept() {
        tombstone("serverless_tenant-churn", now - 10 * DAY);

        // The same name deleted again, a moment before the scrubber would have reached the old record.
        backend.putTombstoneAsync(descriptor("serverless_tenant-churn").tombstoned(now - 1));

        assertEquals("the record that matters now is the recent one, and it is inside the window", 0, scrubber.scrubOnce(7 * DAY));

        IndexDescriptor still = backend.get("serverless_tenant-churn");
        assertNotNull("deleting it would lose the record for the deletion that just happened", still);
        assertFalse(still.exists());
    }

    /**
     * The freshness check and the delete it authorises are adjacent, with nothing in between.
     *
     * <p>{@link #testATombstoneRefreshedDuringThePassIsKept} pins that the check happens; this pins that its
     * answer is acted on while it is still true. The candidates used to be batched a hundred at a time, so a
     * name checked first was deleted up to ninety-nine blob reads later -- and the window that opens is
     * exactly the one the check exists to close, because what can happen inside it is the name being deleted,
     * recreated and deleted again, leaving a young tombstone for a deletion still inside its window.
     *
     * <p>Asserted as the operation sequence rather than as an outcome, because the outcome under the old
     * code was right in every test that did not race. A property about ordering has to be tested as one.
     */
    public void testTheAgeCheckAndTheDeleteAreAdjacent() {
        for (int i = 0; i < 3; i++) {
            tombstone("serverless_tenant-old-" + i, now - 10 * DAY);
        }
        tombstone("serverless_tenant-recent", now - DAY);

        List<String> operations = new ArrayList<>();
        TombstoneScrubber recording = new TombstoneScrubber(
            path -> new RecordingContainer(store.blobContainer(path), operations),
            BlobPath.cleanPath(),
            () -> now
        );

        assertEquals(3, recording.scrubOnce(7 * DAY));

        // Every delete must be preceded directly by the read of the name it deletes.
        int deletes = 0;
        for (int i = 0; i < operations.size(); i++) {
            String operation = operations.get(i);
            if (operation.startsWith("delete ") == false) {
                continue;
            }
            deletes++;
            String name = operation.substring("delete ".length());
            assertTrue("a delete must not be the first operation", i > 0);
            assertEquals(
                "the age check must be the operation immediately before the delete it authorises, "
                    + "or it is not a freshness check at all: "
                    + operations,
                "read " + name,
                operations.get(i - 1)
            );
        }
        assertEquals("one delete per reclaimed tombstone", 3, deletes);
    }

    /** Records which blob each read and each delete touched, in order. */
    private static final class RecordingContainer implements org.opensearch.common.blobstore.BlobContainer {
        private final org.opensearch.common.blobstore.BlobContainer delegate;
        private final List<String> operations;

        RecordingContainer(org.opensearch.common.blobstore.BlobContainer delegate, List<String> operations) {
            this.delegate = delegate;
            this.operations = operations;
        }

        @Override
        public java.util.Optional<org.opensearch.common.blobstore.BlobRegister> readRegister(String blobName) throws java.io.IOException {
            operations.add("read " + blobName);
            return delegate.readRegister(blobName);
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws java.io.IOException {
            for (String blobName : blobNames) {
                operations.add("delete " + blobName);
            }
            delegate.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public java.util.Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobs() throws java.io.IOException {
            return delegate.listBlobs();
        }

        @Override
        public java.util.Map<String, org.opensearch.common.blobstore.BlobContainer> children() throws java.io.IOException {
            return delegate.children();
        }

        @Override
        public java.util.Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobsByPrefix(String prefix)
            throws java.io.IOException {
            return delegate.listBlobsByPrefix(prefix);
        }

        @Override
        public BlobPath path() {
            return delegate.path();
        }

        @Override
        public boolean blobExists(String blobName) throws java.io.IOException {
            return delegate.blobExists(blobName);
        }

        @Override
        public java.io.InputStream readBlob(String blobName) throws java.io.IOException {
            return delegate.readBlob(blobName);
        }

        @Override
        public java.io.InputStream readBlob(String blobName, long position, long length) throws java.io.IOException {
            return delegate.readBlob(blobName, position, length);
        }

        @Override
        public void writeBlob(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws java.io.IOException {
            delegate.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws java.io.IOException {
            delegate.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public org.opensearch.common.blobstore.DeleteResult delete() throws java.io.IOException {
            return delegate.delete();
        }
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
        tombstone("serverless_tenant-old", now - 10 * DAY);

        assertEquals("zero here would mean the keyspace is invisible rather than empty", 1, scrubber.scrubOnce(7 * DAY));
    }
}
