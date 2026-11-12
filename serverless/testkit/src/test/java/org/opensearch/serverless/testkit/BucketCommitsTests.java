/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.bucket.BucketCommits;
import org.opensearch.serverless.bucket.BucketManifest;
import org.opensearch.serverless.bucket.BucketStore;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.CommitRegister;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bucket members' commits share one register: many land in one swap, each judged against its own entry's version, so
 * a sibling's checkpoint is never mistaken for this member's commit moving.
 */
public class BucketCommitsTests extends OpenSearchTestCase {

    private static final String BUCKET = "s03-1";

    private static CommitManifest commit(long term, String file) {
        return new CommitManifest(term, Map.of(file, "t=" + term), "node-a", Map.of(file, 42L));
    }

    private static BucketStore withMembers(CountingBlobStore blobs, String... uuids) throws IOException {
        final BucketStore store = new BucketStore(blobs, BlobPath.cleanPath());
        BucketManifest manifest = BucketManifest.empty(BUCKET);
        for (String uuid : uuids) {
            manifest = manifest.withMember(uuid, new BucketManifest.Member("index-" + uuid, null));
        }
        store.swap(manifest, BlobRegister.ABSENT_GENERATION).orElseThrow();
        return store;
    }

    private static CommitManifest parse(BlobRegister register) throws IOException {
        try (InputStream in = register.value().streamInput()) {
            return CommitManifest.fromStream(in);
        }
    }

    private static Map<String, BucketCommits.Swap> swaps(Object... uuidVersionCommit) {
        final Map<String, BucketCommits.Swap> swaps = new LinkedHashMap<>();
        for (int i = 0; i < uuidVersionCommit.length; i += 3) {
            swaps.put(
                (String) uuidVersionCommit[i],
                new BucketCommits.Swap((Long) uuidVersionCommit[i + 1], (CommitManifest) uuidVersionCommit[i + 2])
            );
        }
        return swaps;
    }

    public void testMembersCheckpointTogetherInOneSwap() throws Exception {
        try (CountingBlobStore blobs = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final BucketStore store = withMembers(blobs, "a", "b", "c");
            final BucketCommits commits = new BucketCommits(store);
            assertTrue("a member with no commit reads as none", commits.member(BUCKET, "a").read().isEmpty());

            blobs.reset();
            final Map<String, BlobRegisterCasResult> first = commits.swapAll(
                BUCKET,
                swaps("a", 0L, commit(1, "_0.cfs"), "b", 0L, commit(1, "_1.cfs"))
            );
            assertTrue(first.get("a").applied());
            assertTrue(first.get("b").applied());
            assertEquals("each member's version is its own", 1L, first.get("a").currentGeneration());
            assertEquals("two members' commits, one conditional write", 1L, blobs.registerWrites());
            assertEquals(1L, blobs.registerReads());

            blobs.reset();
            final Map<String, BlobRegisterCasResult> second = commits.swapAll(
                BUCKET,
                swaps("a", 1L, commit(1, "_2.cfs"), "c", 0L, commit(1, "_3.cfs"))
            );
            assertEquals(2L, second.get("a").currentGeneration());
            assertEquals(1L, second.get("c").currentGeneration());
            assertEquals("the next swap goes over what this node wrote, without reading it again", 0L, blobs.registerReads());
            assertEquals(1L, blobs.registerWrites());

            final CommitRegister b = commits.member(BUCKET, "b");
            final BlobRegister read = b.read().orElseThrow();
            assertEquals("a sibling's swap leaves this member's version where it was", 1L, read.generation());
            assertEquals(Map.of("_1.cfs", "t=1"), parse(read).files());
            final BucketManifest stored = store.read(BUCKET).orElseThrow().manifest();
            assertEquals(2L, stored.member("a").orElseThrow().version());
            assertEquals("version survives the round trip", stored, BucketManifest.fromStream(stored.toBytes().streamInput()));
        }
    }

    public void testAMovedMemberIsRefusedAndItsSiblingsStillLand() throws Exception {
        try (CountingBlobStore blobs = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final BucketStore store = withMembers(blobs, "a", "b", "gone");
            final BucketCommits commits = new BucketCommits(store);
            commits.swapAll(BUCKET, swaps("a", 0L, commit(1, "_0.cfs")));
            final Map<String, BlobRegisterCasResult> results = commits.swapAll(
                BUCKET,
                swaps("a", 0L, commit(1, "_9.cfs"), "b", 0L, commit(1, "_1.cfs"))
            );
            assertFalse("a stale version is refused", results.get("a").applied());
            assertEquals(1L, results.get("a").currentGeneration());
            assertTrue("and its sibling lands", results.get("b").applied());
            assertEquals(Map.of("_0.cfs", "t=1"), parse(commits.member(BUCKET, "a").read().orElseThrow()).files());

            final var read = store.read(BUCKET).orElseThrow();
            store.swap(read.manifest().withoutMember("gone"), read.generation()).orElseThrow();
            commits.forget(BUCKET);
            final Map<String, BlobRegisterCasResult> out = commits.swapAll(
                BUCKET,
                swaps("gone", 0L, commit(1, "_5.cfs"), "b", 1L, commit(1, "_6.cfs"))
            );
            assertFalse("a member that left the bucket is not written", out.get("gone").applied());
            assertTrue(out.get("b").applied());
            expectThrows(IOException.class, () -> commits.member(BUCKET, "gone").read());
        }
    }

    public void testAnotherWriterLandingFirstCostsOneLostSwap() throws Exception {
        try (CountingBlobStore blobs = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final BucketStore store = withMembers(blobs, "a", "b");
            final BucketCommits mine = new BucketCommits(store);
            final BucketCommits theirs = new BucketCommits(store);
            mine.swapAll(BUCKET, swaps("a", 0L, commit(1, "_0.cfs")));
            theirs.swapAll(BUCKET, swaps("a", 1L, commit(2, "_1.cfs")));

            blobs.reset();
            final Map<String, BlobRegisterCasResult> results = mine.swapAll(BUCKET, swaps("b", 0L, commit(1, "_2.cfs")));
            assertTrue("a sibling written elsewhere is not this member moving", results.get("b").applied());
            assertEquals("one lost swap over the stale copy, one read, one swap that lands", 2L, blobs.registerWrites());
            assertEquals(1L, blobs.registerReads());
            assertEquals(Map.of("_1.cfs", "t=2"), parse(mine.member(BUCKET, "a").read().orElseThrow()).files());
        }
    }
}
