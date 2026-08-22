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

        writerBackend.create(descriptor("serverless_tenant-x"));
        assertNotNull("the reader caches it", readerBackend.get("serverless_tenant-x"));

        // Deleted by the other node, which this node's cache knows nothing about.
        writerBackend.putTombstoneAsync(descriptor("serverless_tenant-x").tombstoned());
        BlobDescriptorChangeLog writerLog = logOver(sharedLog);
        writerLog.append(deleted("serverless_tenant-x"));

        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(sharedLog), readerBackend);
        tailer.tailOnce();

        IndexDescriptor after = readerBackend.get("serverless_tenant-x");
        assertTrue(
            "after tailing the delete, the cached live descriptor must be gone rather than served until the " + "freshness window expires",
            after == null || after.exists() == false
        );
    }

    /**
     * A change already consumed is not delivered again, however many passes run.
     *
     * <p>The cursor moves to the bucket the read started in rather than past it, so that entry written to
     * that bucket a moment after the listing is not missed. That alone used to mean every entry in the
     * bucket came back on every pass: buckets are a minute wide against a five second interval, so a change
     * was redelivered up to twelve times before its bucket rolled.
     *
     * <p>Which would be merely wasteful if applying were free, and it is not. Applying a change invalidates
     * the cached descriptor for that name, so redelivery evicted a live entry twelve times over and sent
     * the next read of a recently-changed index back to the object store. The tailer was manufacturing the
     * misses it exists to prevent.
     *
     * <p>So the assertion is exact rather than a set comparison: three passes over two changes must
     * invalidate exactly twice.
     */
    public void testAConsumedChangeIsNotDeliveredAgain() throws Exception {
        Path shared = createTempDir();
        BlobDescriptorChangeLog writerLog = logOver(shared);
        writerLog.append(created("serverless_tenant-a"));
        writerLog.append(created("serverless_tenant-b"));

        List<String> invalidated = new ArrayList<>();
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), new RecordingBackend(invalidated));

        assertEquals("the first pass consumes both", 2, tailer.tailOnce());
        assertEquals("and the rest have nothing left to do", 0, tailer.tailOnce());
        assertEquals(0, tailer.tailOnce());

        assertEquals("two changes, two invalidations, three passes", 2, invalidated.size());
        assertEquals(java.util.Set.of("serverless_tenant-a", "serverless_tenant-b"), new java.util.HashSet<>(invalidated));
    }

    /** A change appended after a pass still arrives, which is what the revisit exists to guarantee. */
    public void testAChangeAppendedAfterAPassIsStillDelivered() throws Exception {
        Path shared = createTempDir();
        BlobDescriptorChangeLog writerLog = logOver(shared);
        writerLog.append(created("serverless_tenant-early"));

        List<String> invalidated = new ArrayList<>();
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), new RecordingBackend(invalidated));
        assertEquals(1, tailer.tailOnce());

        // Same bucket, after the cursor already pointed at it. Skipping consumed keys must not turn into
        // skipping the whole bucket, which is the way this fix could have broken the thing it optimises.
        writerLog.append(created("serverless_tenant-late"));
        assertEquals("an entry added to the revisited bucket must still arrive", 1, tailer.tailOnce());

        assertEquals(List.of("serverless_tenant-early", "serverless_tenant-late"), invalidated);
    }

    /**
     * The cursor starts at the bucket the node started in, not at the beginning of the log.
     *
     * <p>It used to start at null, meaning the first pass read the entire history. That was right when the
     * tailer fed a name index that had to be built from that history; it has not been right since the index
     * was removed. A starting node has an empty descriptor cache and no shards opened on demand, so every
     * historical entry it read was applied to nothing, and the log is never pruned, so the cost of reading
     * it grows for the life of the cluster.
     */
    public void testAStartingNodeDoesNotReadTheWholeHistory() throws Exception {
        Path shared = createTempDir();
        org.opensearch.common.blobstore.BlobStore store = new FsBlobStore(1024, shared, false);

        // A driven clock, because the boundary being tested is a bucket boundary and buckets are a minute
        // wide. Writing the history an hour back is what puts it behind the cursor; entries in the bucket
        // the node starts in are deliberately still read, since "just before start-up" and "just after"
        // cannot be told apart inside one bucket and reading them is the safe direction.
        final long[] now = { java.util.concurrent.TimeUnit.HOURS.toMillis(1000) };
        BlobDescriptorChangeLog writerLog = new BlobDescriptorChangeLog(store::blobContainer, BlobPath.cleanPath(), () -> now[0]);
        writerLog.append(created("serverless_tenant-ancient"));
        writerLog.append(created("serverless_tenant-older-still"));

        now[0] += java.util.concurrent.TimeUnit.HOURS.toMillis(1);
        List<String> invalidated = new ArrayList<>();
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(
            new BlobDescriptorChangeLog(store::blobContainer, BlobPath.cleanPath(), () -> now[0]),
            new RecordingBackend(invalidated)
        );

        assertNotNull("a starting node names the bucket it started in rather than the start of time", tailer.resumeFrom());
        assertEquals("history written before this node existed has nothing here to act on", 0, tailer.tailOnce());
        assertEquals(List.of(), invalidated);
    }

    /**
     * The cursor advances, and to the bucket captured <em>before</em> the read rather than after. Taking it
     * after would skip anything written to a rolling bucket between the listing and the capture.
     */
    public void testTheCursorAdvances() throws Exception {
        Path shared = createTempDir();
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), backendOver(createTempDir()));

        String atStart = tailer.resumeFrom();
        assertNotNull(atStart);
        tailer.tailOnce();
        assertNotNull("and still names a bucket afterwards", tailer.resumeFrom());
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
        logOver(shared).append(created("serverless_tenant-a"));

        List<String> invalidated = new ArrayList<>();
        DescriptorBackend recording = new RecordingBackend(invalidated);

        DescriptorChangeTailer tailer = new DescriptorChangeTailer(logOver(shared), recording);
        tailer.tailOnce();

        assertEquals(List.of("serverless_tenant-a"), invalidated);
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
            logOver(shared).append(created("serverless_tenant-live"));
            logOver(shared).append(deleted("serverless_tenant-gone"));

            new DescriptorChangeTailer(logOver(shared), backendOver(createTempDir())).tailOnce();

            assertEquals("only the deleted name may be released", 1, released.size());
            assertEquals("serverless_tenant-gone", released.get(0).getName());
            assertEquals(
                "released by uuid as well as name, or a name reused moments later would close a live shard",
                "serverless_tenant-gone-uuid",
                released.get(0).getUUID()
            );
        } finally {
            org.opensearch.cluster.metadata.GatedIndexRelease.register(null);
        }
    }

    /** With nothing registered, which is a stock node, tailing must not attempt to close anything. */
    public void testNoReleaserMeansNoRelease() throws Exception {
        Path shared = createTempDir();
        logOver(shared).append(deleted("serverless_tenant-gone"));
        org.opensearch.cluster.metadata.GatedIndexRelease.register(null);

        // The assertion is that this does not throw: an unregistered seam is a no-op, not a failure.
        new DescriptorChangeTailer(logOver(shared), backendOver(createTempDir())).tailOnce();
    }

    /**
     * The cursor must not step over a bucket the read could not finish.
     *
     * <p>The read used to catch its own {@link java.io.IOException} mid-iteration and return the entries it
     * had reached, which is indistinguishable from a complete read of a shorter log. This then advanced the
     * cursor past everything it had asked for, including the buckets it never managed to list -- permanently,
     * since they are behind the cursor from then on. The tailer's own comment claimed the opposite: "left
     * un-advanced on purpose, so the next pass retries the same range rather than stepping over changes it
     * never read."
     *
     * <p>Driven by making one bucket unreadable for exactly one pass, which is the shape a transient object
     * store failure has.
     */
    public void testTheCursorDoesNotAdvancePastABucketItCouldNotRead() throws Exception {
        Path shared = createTempDir();
        org.opensearch.common.blobstore.BlobStore store = new FsBlobStore(1024, shared, false);
        java.util.concurrent.atomic.AtomicBoolean failing = new java.util.concurrent.atomic.AtomicBoolean(true);

        // Buckets are discovered through children() and still cannot be listed, which is the partial case
        // rather than a total failure: the tailer knows the bucket is there and cannot read what is in it.
        java.util.function.Function<BlobPath, org.opensearch.common.blobstore.BlobContainer> containers = path -> {
            org.opensearch.common.blobstore.BlobContainer real = store.blobContainer(path);
            return failing.get() ? new UnlistableContainer(real, failing) : real;
        };

        // A driven clock, because the loss this is about only shows once the bucket has rolled: an
        // unreadable bucket that is still the current one gets re-read by accident on the next pass. The
        // permanent version is a bucket that goes unread and is then behind the cursor forever.
        final long[] now = { java.util.concurrent.TimeUnit.HOURS.toMillis(1000) };
        new BlobDescriptorChangeLog(store::blobContainer, BlobPath.cleanPath(), () -> now[0]).append(created("serverless_tenant-a"));
        String entryBucket = BlobDescriptorChangeLog.bucketOf(now[0]);

        List<String> invalidated = new ArrayList<>();
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(
            new BlobDescriptorChangeLog(containers, BlobPath.cleanPath(), () -> now[0]),
            new RecordingBackend(invalidated)
        );

        // Three buckets later, so a cursor that advanced on a failed read would step over the entry's
        // bucket rather than happening to land back on it.
        now[0] += 3 * java.util.concurrent.TimeUnit.MINUTES.toMillis(1);

        assertEquals("nothing could be read", 0, tailer.tailOnce());
        assertEquals("and the pass must say so rather than looking quiet", 1, tailer.incompleteReadCount());
        assertTrue(
            "the cursor must not step over a bucket it never read, which is permanent: " + tailer.resumeFrom(),
            tailer.resumeFrom().compareTo(entryBucket) <= 0
        );

        failing.set(false);
        assertEquals("the change is still delivered once the bucket is readable", 1, tailer.tailOnce());
        assertEquals(List.of("serverless_tenant-a"), invalidated);
    }

    /**
     * An entry appended by a node whose clock is a little behind is still read.
     *
     * <p>Entries are bucketed by the appender's wall clock and the cursor is named by the reader's, and the
     * two agree only as well as NTP makes them. With the cursor at the current bucket, a reader a few
     * seconds ahead across a boundary names bucket N while an appender at the same instant is still writing
     * into N-1 -- which the cursor has passed. That entry is not read late, it is never read at all, and for
     * a gated index nothing else would ever mention it. The cursor is held back by more than any plausible
     * skew for that reason.
     */
    public void testAnEntryFromANodeWhoseClockIsBehindIsStillRead() throws Exception {
        Path shared = createTempDir();
        org.opensearch.common.blobstore.BlobStore store = new FsBlobStore(1024, shared, false);

        // The reader's clock, a bucket boundary plus a moment.
        final long readerNow = java.util.concurrent.TimeUnit.HOURS.toMillis(1000) + 200;
        // The appender's, five seconds behind it, which puts its entry in the previous bucket.
        final long appenderNow = readerNow - java.util.concurrent.TimeUnit.SECONDS.toMillis(5);
        assertNotEquals(
            "the test is only meaningful if the two clocks land in different buckets",
            BlobDescriptorChangeLog.bucketOf(readerNow),
            BlobDescriptorChangeLog.bucketOf(appenderNow)
        );

        BlobDescriptorChangeLog appenderLog = new BlobDescriptorChangeLog(store::blobContainer, BlobPath.cleanPath(), () -> appenderNow);
        appenderLog.append(created("serverless_tenant-skewed"));

        List<String> invalidated = new ArrayList<>();
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(
            new BlobDescriptorChangeLog(store::blobContainer, BlobPath.cleanPath(), () -> readerNow),
            new RecordingBackend(invalidated)
        );

        assertEquals("an entry a few seconds of skew behind the cursor must still arrive", 1, tailer.tailOnce());
        assertEquals(List.of("serverless_tenant-skewed"), invalidated);
        // And still exactly once, because the exclusion set spans every bucket the lagging cursor revisits.
        assertEquals(0, tailer.tailOnce());
        assertEquals(1, invalidated.size());
    }

    /**
     * A container whose listings fail, standing in for a bucket the object store cannot serve.
     *
     * <p>{@code children()} still answers, and wraps what it returns, because that is the shape that
     * exercises the bug: the bucket is discovered and then cannot be read. A container that could not even
     * be enumerated would fail the whole pass, which the tailer already handled.
     */
    private static final class UnlistableContainer implements org.opensearch.common.blobstore.BlobContainer {
        private final org.opensearch.common.blobstore.BlobContainer delegate;
        private final java.util.concurrent.atomic.AtomicBoolean failing;

        UnlistableContainer(org.opensearch.common.blobstore.BlobContainer delegate, java.util.concurrent.atomic.AtomicBoolean failing) {
            this.delegate = delegate;
            this.failing = failing;
        }

        @Override
        public java.util.Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobs() throws java.io.IOException {
            if (failing.get()) {
                throw new java.io.IOException("bucket unavailable");
            }
            return delegate.listBlobs();
        }

        @Override
        public java.util.Map<String, org.opensearch.common.blobstore.BlobContainer> children() throws java.io.IOException {
            java.util.Map<String, org.opensearch.common.blobstore.BlobContainer> wrapped = new java.util.LinkedHashMap<>();
            for (java.util.Map.Entry<String, org.opensearch.common.blobstore.BlobContainer> child : delegate.children().entrySet()) {
                wrapped.put(child.getKey(), new UnlistableContainer(child.getValue(), failing));
            }
            return wrapped;
        }

        @Override
        public java.util.Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobsByPrefix(String prefix)
            throws java.io.IOException {
            if (failing.get()) {
                throw new java.io.IOException("bucket unavailable");
            }
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

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws java.io.IOException {
            delegate.deleteBlobsIgnoringIfNotExists(blobNames);
        }
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
