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

        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(sharedLog), readerBackend);
        tailer.tailOnce();

        IndexDescriptor after = readerBackend.get("tenant-x");
        assertTrue(
            "after tailing the delete, the cached live descriptor must be gone rather than served until the "
                + "freshness window expires",
            after == null || after.exists() == false
        );
    }

    /**
     * Re-applying what has already been seen has to be harmless, which is what lets this be a poll.
     *
     * <p>Harmless, not absent. The cursor advances to the bucket the read started in rather than past it, so
     * everything written in the current bucket is delivered again on the next pass, and buckets are a minute
     * wide against a five second interval. A change is therefore re-read and re-invalidated up to twelve
     * times before its bucket rolls.
     *
     * <p>That is correct and it is not free: re-invalidating a name evicts it, so the next read of a
     * recently-changed index goes back to the store. The assertion here is the correctness half, that no
     * name outside the change set is ever touched and repetition converges rather than corrupts. The cost
     * half is recorded as a finding rather than pinned as intended behaviour, because a cursor that
     * remembered the entries it had consumed inside the bucket would not pay it.
     */
    public void testTailingIsIdempotent() throws Exception {
        Path shared = createTempDir();
        BlobDescriptorChangeLog writerLog = logOver(shared);
        writerLog.append(created("tenant-a"));
        writerLog.append(created("tenant-b"));

        List<String> invalidated = new ArrayList<>();
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), new RecordingBackend(invalidated));

        tailer.tailOnce();
        tailer.tailOnce();
        tailer.tailOnce();

        assertEquals(
            "three passes must converge on the same two names and touch nothing else",
            java.util.Set.of("tenant-a", "tenant-b"),
            new java.util.HashSet<>(invalidated)
        );
    }

    /**
     * The cursor advances, or every pass re-reads the whole log and the cost grows without bound.
     *
     * <p>It advances to the bucket captured <em>before</em> the read rather than after. Taking it after
     * would skip anything written to a rolling bucket between the listing and the capture.
     */
    public void testTheCursorAdvances() throws Exception {
        Path shared = createTempDir();
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), backendOver(createTempDir()));

        assertNull("the first pass reads everything, which is what a joining node needs", tailer.resumeFrom());
        tailer.tailOnce();
        assertNotNull("and then resumes from where it stopped", tailer.resumeFrom());
    }

    /** An empty log is an ordinary state and must not look like a failure. */
    public void testAnEmptyLogAppliesNothing() throws Exception {
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(createTempDir()), backendOver(createTempDir()));
        assertEquals(0, tailer.tailOnce());
        assertEquals(1, tailer.passCount());
    }

    /** Invalidation is the tailer's reason to exist, so it is asserted on its own rather than only in passing. */
    public void testTailingInvalidatesEveryTouchedName() throws Exception {
        Path shared = createTempDir();
        logOver(shared).append(created("tenant-a"));

        List<String> invalidated = new ArrayList<>();
        DescriptorBackend recording = new RecordingBackend(invalidated);

        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), recording);
        tailer.tailOnce();

        assertEquals(List.of("tenant-a"), invalidated);
    }

    /**
     * A delete releases the shard, which is the half that had no mechanism other than a timer.
     *
     * <p>A gated delete produces no cluster state diff, so before this the only thing that told a node to
     * close the shard was D2's sweep re-deriving the answer on its next tick. The sweep stays as the
     * backstop; this makes the common case immediate.
     */
    public void testADeleteReleasesTheGatedIndex() throws Exception {
        Path shared = createTempDir();
        List<org.opensearch.core.index.Index> released = new ArrayList<>();
        org.opensearch.cluster.metadata.GatedIndexRelease.register(released::add);
        try {
            logOver(shared).append(created("tenant-live"));
            logOver(shared).append(deleted("tenant-gone"));

            new DescriptorChangeTailer(logOver(shared), backendOver(createTempDir())).tailOnce();

            assertEquals("only the deleted name may be released", 1, released.size());
            assertEquals("tenant-gone", released.get(0).getName());
            assertEquals(
                "released by uuid as well as name, or a name reused moments later would close a live shard",
                "tenant-gone-uuid",
                released.get(0).getUUID()
            );
        } finally {
            org.opensearch.cluster.metadata.GatedIndexRelease.register(null);
        }
    }

    /** With nothing registered, which is a stock node, tailing must not attempt to close anything. */
    public void testNoReleaserMeansNoRelease() throws Exception {
        Path shared = createTempDir();
        logOver(shared).append(deleted("tenant-gone"));
        org.opensearch.cluster.metadata.GatedIndexRelease.register(null);

        // The assertion is that this does not throw: an unregistered seam is a no-op, not a failure.
        new DescriptorChangeTailer(logOver(shared), backendOver(createTempDir())).tailOnce();
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
