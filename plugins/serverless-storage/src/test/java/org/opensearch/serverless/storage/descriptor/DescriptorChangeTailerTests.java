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
import org.opensearch.serverless.storage.nameindex.NameIndexService;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * One node learning what another node wrote, which is the thing a gated index has no other way to do.
 *
 * <h2>Why this had to be built rather than fixed</h2>
 *
 * An ordinary index reaches every node through cluster state. A gated index has no cluster state entry, so
 * none of that applies, and the change log was the intended substitute. Both of its ends were unreachable:
 * {@code DescriptorGate.setChangeFeed} was referenced in production only by its own {@code uninstall}, so no
 * node appended a change, and nothing anywhere read one. A log with no writer and no reader is
 * indistinguishable from a cluster where nothing has happened.
 *
 * <h2>What the tests stand in for</h2>
 *
 * Two services over one store is what two nodes are, for this mechanism: the log is the only thing they
 * share, and neither can see the other's memory. So a change written through one and observed through the
 * other is exactly the property, without needing a cluster to demonstrate it.
 */
public class DescriptorChangeTailerTests extends OpenSearchTestCase {

    private BlobDescriptorChangeLog logOver(Path directory) throws Exception {
        return new BlobDescriptorChangeLog(new FsBlobStore(1024, directory, false)::blobContainer, BlobPath.cleanPath());
    }

    private BlobDescriptorBackend backendOver(Path directory) throws Exception {
        return new BlobDescriptorBackend(new FsBlobStore(1024, directory, false).blobContainer(BlobPath.cleanPath()));
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

    private static DescriptorChange created(String name) {
        return new DescriptorChange(name, name + "-uuid", DescriptorChange.Kind.UPDATED, System.currentTimeMillis());
    }

    private static DescriptorChange deleted(String name) {
        return new DescriptorChange(name, name + "-uuid", DescriptorChange.Kind.DELETED, System.currentTimeMillis());
    }

    public void testANameCreatedElsewhereBecomesResolvableHere() throws Exception {
        Path shared = createTempDir();
        BlobDescriptorChangeLog writerLog = logOver(shared);

        // The other node's write.
        writerLog.append(created("tenant-remote"));

        NameIndexService localNameIndex = new NameIndexService(true);
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), backendOver(createTempDir()), localNameIndex);

        assertFalse("nothing has been tailed yet", localNameIndex.getNameIndex().contains("tenant-remote"));
        assertEquals(1, tailer.tailOnce());
        assertTrue("an index created on another node must become resolvable here", localNameIndex.getNameIndex().contains("tenant-remote"));
    }

    /** A delete has to travel too, and it is the direction that matters more. */
    public void testANameDeletedElsewhereStopsResolvingHere() throws Exception {
        Path shared = createTempDir();
        BlobDescriptorChangeLog writerLog = logOver(shared);
        NameIndexService localNameIndex = new NameIndexService(true);
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), backendOver(createTempDir()), localNameIndex);

        writerLog.append(created("tenant-doomed"));
        tailer.tailOnce();
        assertTrue(localNameIndex.getNameIndex().contains("tenant-doomed"));

        writerLog.append(deleted("tenant-doomed"));
        tailer.tailOnce();
        assertFalse("a deleted index must stop resolving rather than linger until a rebuild", localNameIndex.getNameIndex().contains("tenant-doomed"));
    }

    /**
     * A cached descriptor for a name changed elsewhere must be dropped.
     *
     * <p>This is the half that has a correctness consequence rather than a freshness one: a deleted index
     * still resolving as live would accept a write against a shard the cluster no longer believes in.
     */
    public void testTailingInvalidatesTheCachedDescriptor() throws Exception {
        Path sharedStore = createTempDir();
        Path sharedLog = createTempDir();

        BlobDescriptorBackend readerBackend = backendOver(sharedStore);
        BlobDescriptorBackend writerBackend = backendOver(sharedStore);

        writerBackend.create(descriptor("tenant-x"));
        assertNotNull("the reader caches it", readerBackend.get("tenant-x"));

        // Deleted by the other node, which this node's cache knows nothing about.
        writerBackend.putTombstoneAsync(descriptor("tenant-x").tombstoned());
        BlobDescriptorChangeLog writerLog = logOver(sharedLog);
        writerLog.append(deleted("tenant-x"));

        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(sharedLog), readerBackend, null);
        tailer.tailOnce();

        IndexDescriptor after = readerBackend.get("tenant-x");
        assertTrue(
            "after tailing the delete, the cached live descriptor must be gone rather than served until the "
                + "freshness window expires",
            after == null || after.exists() == false
        );
    }

    /** Re-applying what has already been seen has to be harmless, which is what lets this be a poll. */
    public void testTailingIsIdempotent() throws Exception {
        Path shared = createTempDir();
        BlobDescriptorChangeLog writerLog = logOver(shared);
        writerLog.append(created("tenant-a"));
        writerLog.append(created("tenant-b"));

        NameIndexService localNameIndex = new NameIndexService(true);
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), backendOver(createTempDir()), localNameIndex);

        tailer.tailOnce();
        tailer.tailOnce();
        tailer.tailOnce();

        assertTrue(localNameIndex.getNameIndex().contains("tenant-a"));
        assertTrue(localNameIndex.getNameIndex().contains("tenant-b"));
        assertEquals("two names, however many passes", 2, localNameIndex.getNameIndex().baseSize() + localNameIndex.getNameIndex().pendingSize());
    }

    /**
     * The cursor advances, or every pass re-reads the whole log and the cost grows without bound.
     *
     * <p>It advances to the bucket captured <em>before</em> the read rather than after. Taking it after
     * would skip anything written to a rolling bucket between the listing and the capture.
     */
    public void testTheCursorAdvances() throws Exception {
        Path shared = createTempDir();
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), backendOver(createTempDir()), null);

        assertNull("the first pass reads everything, which is what a joining node needs", tailer.resumeFrom());
        tailer.tailOnce();
        assertNotNull("and then resumes from where it stopped", tailer.resumeFrom());
    }

    /** An empty log is an ordinary state and must not look like a failure. */
    public void testAnEmptyLogAppliesNothing() throws Exception {
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(
            logOver(createTempDir()),
            backendOver(createTempDir()),
            new NameIndexService(true)
        );
        assertEquals(0, tailer.tailOnce());
        assertEquals(1, tailer.passCount());
    }

    /** With no name index, invalidation still has to happen: the two consumers fail independently. */
    public void testInvalidationHappensWithoutANameIndex() throws Exception {
        Path shared = createTempDir();
        logOver(shared).append(created("tenant-a"));

        List<String> invalidated = new ArrayList<>();
        DescriptorBackend recording = new RecordingBackend(invalidated);

        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), recording, null);
        tailer.tailOnce();

        assertEquals(List.of("tenant-a"), invalidated);
    }

    /** A backend that records what it was asked to forget. */
    private static final class RecordingBackend implements DescriptorBackend {
        private final List<String> invalidated;

        RecordingBackend(List<String> invalidated) {
            this.invalidated = invalidated;
        }

        @Override
        public void invalidate(String name) {
            invalidated.add(name);
        }

        @Override
        public IndexDescriptor get(String name) {
            return null;
        }

        @Override
        public boolean create(IndexDescriptor descriptor) {
            return true;
        }

        @Override
        public java.util.concurrent.CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor) {
            return java.util.concurrent.CompletableFuture.completedFuture(true);
        }

        @Override
        public void put(IndexDescriptor descriptor) {}

        @Override
        public void putAsync(IndexDescriptor descriptor) {}

        @Override
        public void putTombstoneAsync(IndexDescriptor tombstone) {}

        @Override
        public void putTombstoneAsync(IndexDescriptor tombstone, org.opensearch.core.action.ActionListener<Void> whenDurable) {
            whenDurable.onResponse(null);
        }

        @Override
        public boolean available() {
            return true;
        }

        @Override
        public void warmAsync(java.util.Collection<String> names, org.opensearch.core.action.ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    }
}
