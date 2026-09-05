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
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The tailer and the bytes.
 *
 * <h2>What an operator got by enabling only this</h2>
 *
 * Both reclamation paths are off by default and independent of each other. The tailer described itself as
 * "the cheap, event-driven replacement for the manifest-discovery half of the sweep", and an operator who
 * enabled it alone reclaimed every superseded manifest -- kilobytes -- while leaking every byte of segment
 * data those manifests exclusively referenced, permanently and with no signal. It also inverted the
 * documented "bundles before manifests, never the reverse" ordering, which was only safe on the assumption
 * that the periodic sweep was running underneath to pick up what it orphaned.
 */
public class GcCandidateTailerBundleReclaimTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;
    private static final long RETENTION_WINDOW_MILLIS = 1000;
    private static final long LOOKBACK_MILLIS = 10_000;

    private Path root;
    private AtomicLong clockMillis;
    private final Map<String, BlobContainer> shardContainers = new HashMap<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        root = createTempDir();
        clockMillis = new AtomicLong(0);
    }

    private BlobGcCandidateLog log() throws IOException {
        Path logRoot = root.resolve("log");
        FsBlobStore blobStore = new FsBlobStore(1024, logRoot, false);
        return new BlobGcCandidateLog(path -> {
            try {
                return blobStore.blobContainer(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, BlobPath.cleanPath(), clockMillis::get);
    }

    private BlobContainer shardContainer(String indexUuid, int shardId) throws IOException {
        String key = indexUuid + "/" + shardId;
        BlobContainer existing = shardContainers.get(key);
        if (existing != null) {
            return existing;
        }
        FsBlobStore blobStore = new FsBlobStore(1024, root.resolve("shards").resolve(key), false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        shardContainers.put(key, container);
        return container;
    }

    // Bundles are write-once, so a second manifest that shares one reuses the reference rather than
    // rewriting it -- which is exactly the shape a merge leaves behind and the case the reclaim must not
    // mistake for an orphan.
    private final Map<String, FileReference> writtenBundles = new HashMap<>();

    /** A real bundle plus a manifest referencing it, so "did the bytes go" is an actual question about actual blobs. */
    private CommitManifest publish(long generation, long createdAtMillis, String bundleName, String fileName) throws IOException {
        FileReference reference = writtenBundles.get(bundleName);
        if (reference == null) {
            BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(shardContainer(INDEX_UUID, SHARD_ID));
            var bundle = bundleStore.writeBundle(
                bundleName,
                List.of(new BundleFileContent(fileName, ("bytes-" + fileName).getBytes("UTF-8")))
            );
            var entry = bundle.entries().get(fileName);
            reference = new FileReference(bundleName, entry.offset(), entry.length(), entry.checksum());
            writtenBundles.put(bundleName, reference);
        }
        CommitManifest manifest = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            1,
            generation,
            fileName,
            Map.of(fileName, reference),
            generation,
            generation,
            null,
            1,
            PruningStats.empty(),
            createdAtMillis
        );
        new BlobContainerManifestStore(shardContainer(INDEX_UUID, SHARD_ID)).writeManifest(manifest);
        return manifest;
    }

    public void testDeletingASupersededManifestAlsoReclaimsTheBundlesOnlyItReferenced() throws Exception {
        BlobGcCandidateLog log = log();
        clockMillis.set(0);
        publish(1, 0, "bundle-gen1", "segments_1");
        publish(2, 0, "bundle-gen2", "segments_2");
        log.append(new GcCandidate(INDEX_UUID, SHARD_ID, 1, 1));

        clockMillis.set(RETENTION_WINDOW_MILLIS);
        new GcCandidateTailer(log, this::shardContainer, RETENTION_WINDOW_MILLIS, LOOKBACK_MILLIS, clockMillis::get).tailOnce();

        Set<String> bundles = new BlobContainerBundleStore(shardContainer(INDEX_UUID, SHARD_ID)).listBundleNames();
        assertFalse(
            "the deleted manifest's exclusively-referenced bundle is the whole point: reclaiming the manifest and "
                + "leaving the segment data behind reclaims kilobytes and leaks gigabytes",
            bundles.contains("bundle-gen1")
        );
        assertTrue("the surviving manifest's bundle must be untouched", bundles.contains("bundle-gen2"));
    }

    /** The sharing case, which is the ordinary one after a merge: a bundle another manifest still names must survive. */
    public void testABundleStillReferencedByAnotherManifestIsNotDeleted() throws Exception {
        BlobGcCandidateLog log = log();
        clockMillis.set(0);
        publish(1, 0, "bundle-shared", "segments_shared");
        publish(2, 0, "bundle-shared", "segments_shared");
        log.append(new GcCandidate(INDEX_UUID, SHARD_ID, 1, 1));

        clockMillis.set(RETENTION_WINDOW_MILLIS);
        new GcCandidateTailer(log, this::shardContainer, RETENTION_WINDOW_MILLIS, LOOKBACK_MILLIS, clockMillis::get).tailOnce();

        assertTrue(
            "generation 2 still names this bundle, so deleting generation 1 must not touch it",
            new BlobContainerBundleStore(shardContainer(INDEX_UUID, SHARD_ID)).listBundleNames().contains("bundle-shared")
        );
    }
}
