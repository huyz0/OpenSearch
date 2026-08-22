/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.e2e;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.format.SegmentBundle;
import org.opensearch.serverless.storage.gc.BundleReferenceCounter;
import org.opensearch.serverless.storage.gc.ManifestId;
import org.opensearch.serverless.storage.gc.ManifestRetentionPolicy;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wires every Phase 1/2.5 building block together against real (temp-directory) filesystem
 * storage &mdash; no mocks anywhere &mdash; to exercise the full write -&gt; publish -&gt; read
 * -&gt; garbage-collect lifecycle from rfc-serverless-opensearch.md end to end: segment bundles,
 * commit manifests, shard-head CAS activation/publication, and the GC reference-counting/
 * retention rules that decide what becomes safe to delete once a newer commit supersedes it.
 */
public class ServerlessStorageEndToEndTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "index-uuid-e2e";
    private static final int SHARD_ID = 0;

    private BlobContainer newFsBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testFullWritePublishReadGarbageCollectLifecycle() throws Exception {
        BlobContainer blobContainer = newFsBlobContainer();
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(newFsBlobContainer());

        // 1. Shard activation: a writer wins first-ever activation via put-if-absent CAS.
        assertEquals(
            CasResult.SUCCESS,
            shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, java.util.Optional.empty(), ShardHead.initial())
        );

        // 2. Commit 1: flush produces segment "_0.si" alongside "segments_1", bundled and uploaded.
        byte[] segment0Content = randomByteArrayOfLength(512);
        SegmentBundle bundle1 = bundleStore.writeBundle(
            "bundle-1-1",
            List.of(
                new BundleFileContent("segments_1", "seg1-marker".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("_0.si", segment0Content)
            )
        );
        CommitManifest manifest1 = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            1,
            1,
            "segments_1",
            bundle1.entries()
                .entrySet()
                .stream()
                .collect(
                    LinkedHashMap::new,
                    (m, e) -> m.put(
                        e.getKey(),
                        new FileReference("bundle-1-1", e.getValue().offset(), e.getValue().length(), e.getValue().checksum())
                    ),
                    Map::putAll
                ),
            0,
            0,
            null,
            0,
            new PruningStats(100, 1_000L, 2_000L, Map.of()),
            1000L
        );
        manifestStore.writeManifest(manifest1);

        VersionedShardHead afterActivation = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
        assertEquals(
            CasResult.SUCCESS,
            shardStateStore.compareAndSet(
                INDEX_UUID,
                SHARD_ID,
                java.util.Optional.of(afterActivation.version()),
                afterActivation.head().withPublishedGeneration(1)
            )
        );

        // 3. Commit 2: a merge supersedes "_0.si" with a new merged segment in a new bundle; the
        // new manifest's file map no longer references bundle-1-1 at all.
        byte[] mergedSegmentContent = randomByteArrayOfLength(512);
        SegmentBundle bundle2 = bundleStore.writeBundle(
            "bundle-1-2",
            List.of(
                new BundleFileContent("segments_2", "seg2-marker".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("_merged.si", mergedSegmentContent)
            )
        );
        CommitManifest manifest2 = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            1,
            2,
            "segments_2",
            bundle2.entries()
                .entrySet()
                .stream()
                .collect(
                    LinkedHashMap::new,
                    (m, e) -> m.put(
                        e.getKey(),
                        new FileReference("bundle-1-2", e.getValue().offset(), e.getValue().length(), e.getValue().checksum())
                    ),
                    Map::putAll
                ),
            0,
            0,
            null,
            0,
            new PruningStats(100, 1_000L, 3_000L, Map.of()),
            2000L
        );
        manifestStore.writeManifest(manifest2);

        VersionedShardHead afterCommit1 = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
        assertEquals(
            CasResult.SUCCESS,
            shardStateStore.compareAndSet(
                INDEX_UUID,
                SHARD_ID,
                java.util.Optional.of(afterCommit1.version()),
                afterCommit1.head().withPublishedGeneration(2)
            )
        );

        // 4. Reader path: open by reading the head, fetch the manifest it points at, read a file
        // from the bundle it references, and confirm the content is exactly what was written.
        VersionedShardHead currentHead = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
        assertEquals(2L, currentHead.head().latestManifestGeneration());

        CommitManifest currentManifest = manifestStore.readManifest(
            currentHead.head().primaryTerm(),
            currentHead.head().latestManifestGeneration()
        );
        assertEquals(manifest2, currentManifest);

        FileReference mergedRef = currentManifest.files().get("_merged.si");
        byte[] readBack = bundleStore.readFile("bundle-1-2", toBundleFileEntry("_merged.si", mergedRef));
        assertArrayEquals(mergedSegmentContent, readBack);

        // 5. GC path: with only manifest2 "live" for the purpose of this GC sweep (manifest1 is
        // superseded), manifest1 must be judged deletable by ManifestRetentionPolicy, and
        // bundle-1-1 (the bundle only manifest1 referenced) must be judged deletable by
        // BundleReferenceCounter -- while bundle-1-2 (still referenced by the live manifest) must
        // not be.
        List<CommitManifest> allManifestsForShard = List.of(manifest1, manifest2);
        List<CommitManifest> deletableManifests = ManifestRetentionPolicy.computeDeletableManifests(
            allManifestsForShard,
            Long.MAX_VALUE, // retention window fully elapsed
            Set.of(), // no lease pins
            Set.of()  // no durable (snapshot) pins
        );
        assertEquals(List.of(manifest1), deletableManifests);
        assertFalse(deletableManifests.contains(manifest2));

        List<CommitManifest> liveManifestsAfterGc = List.of(manifest2); // manifest1 has been judged deletable
        Set<String> liveBundles = BundleReferenceCounter.computeLiveBundles(liveManifestsAfterGc);
        Set<String> deletableBundles = BundleReferenceCounter.computeDeletableBundles(List.of("bundle-1-1", "bundle-1-2"), liveBundles);
        assertEquals(Set.of("bundle-1-1"), deletableBundles);

        // Actually perform the deletions against the real blob store and manifest store.
        blobContainer.deleteBlobsIgnoringIfNotExists(deletableBundles.stream().toList());
        blobContainer.deleteBlobsIgnoringIfNotExists(deletableManifests.stream().map(CommitManifest::manifestName).toList());

        assertFalse(blobContainer.blobExists("bundle-1-1"));
        assertFalse(blobContainer.blobExists("manifest-1-1"));

        // The still-live bundle and manifest must be completely unaffected: still present, and
        // still readable with correct content after the GC sweep ran.
        assertTrue(blobContainer.blobExists("bundle-1-2"));
        assertTrue(blobContainer.blobExists("manifest-1-2"));
        byte[] readAgainAfterGc = bundleStore.readFile("bundle-1-2", toBundleFileEntry("_merged.si", mergedRef));
        assertArrayEquals(mergedSegmentContent, readAgainAfterGc);
    }

    // Second scenario: a reader holds a lease pinning the older manifest (e.g. a long-running PIT
    // query still reading generation 1) -- GC must retain both the bundle and the manifest until
    // that lease is released, exactly per rfc-serverless-opensearch.md section 6.5.
    public void testLeasePinnedGenerationSurvivesGarbageCollectionSweep() throws Exception {
        BlobContainer blobContainer = newFsBlobContainer();
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);

        CommitManifest manifest1 = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            1,
            1,
            "segments_1",
            Map.of("segments_1", new FileReference("bundle-1-1", 0, 10, 1L)),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            0L
        );
        CommitManifest manifest2 = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            1,
            2,
            "segments_2",
            Map.of("segments_2", new FileReference("bundle-1-2", 0, 10, 1L)),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            0L
        );
        manifestStore.writeManifest(manifest1);
        manifestStore.writeManifest(manifest2);

        List<CommitManifest> deletable = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(manifest1, manifest2),
            Long.MAX_VALUE,
            Set.of(ManifestId.of(manifest1)), // a PIT context still pins generation 1
            Set.of()
        );

        assertEquals(List.of(), deletable);
        assertTrue(blobContainer.blobExists("manifest-1-1"));
    }

    private static org.opensearch.serverless.storage.format.BundleFileEntry toBundleFileEntry(String fileName, FileReference ref) {
        return new org.opensearch.serverless.storage.format.BundleFileEntry(fileName, ref.offset(), ref.length(), ref.checksum());
    }
}
