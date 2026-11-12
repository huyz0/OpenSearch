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
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.bucket.BucketIds;
import org.opensearch.serverless.bucket.BucketManifest;
import org.opensearch.serverless.bucket.BucketStore;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * The bucket manifest's format and register, and the descriptor's bucket field.
 */
public class BucketManifestTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private static CommitManifest commit(long term, String file) {
        return new CommitManifest(term, Map.of(file, "t=" + term), "node-a", Map.of(file, 42L));
    }

    private static BucketManifest parse(byte[] bytes) throws IOException {
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            return BucketManifest.fromStream(in);
        }
    }

    public void testMembersRoundTripWithTheirOwnCommits() throws Exception {
        final BucketManifest manifest = BucketManifest.empty("b-0001")
            .withMember("uuid-a", new BucketManifest.Member("logs-a", commit(3, "_0.cfs")))
            .withMember("uuid-b", new BucketManifest.Member("logs-b", null))
            .atTerm(7);
        final BucketManifest read = parse(org.opensearch.core.common.bytes.BytesReference.toBytes(manifest.toBytes()));
        assertEquals(manifest, read);
        assertEquals(7, read.term());
        assertEquals(2, read.size());
        assertEquals("logs-a", read.member("uuid-a").orElseThrow().indexName());
        final CommitManifest a = read.member("uuid-a").orElseThrow().commit();
        assertEquals(3, a.term());
        assertEquals(Long.valueOf(42L), a.lengthOf("_0.cfs"));
        assertNull("a member that never checkpointed has no commit", read.member("uuid-b").orElseThrow().commit());
    }

    public void testDroppingAMemberLeavesTheOthers() throws Exception {
        final BucketManifest manifest = BucketManifest.empty("b-0001")
            .withMember("uuid-a", new BucketManifest.Member("logs-a", commit(1, "_0.cfs")))
            .withMember("uuid-b", new BucketManifest.Member("logs-b", commit(1, "_1.cfs")));
        final BucketManifest dropped = manifest.withoutMember("uuid-a");
        assertEquals(List.of("uuid-b"), List.copyOf(dropped.members().keySet()));
        assertSame("dropping a non-member changes nothing", dropped, dropped.withoutMember("uuid-zzz"));
    }

    public void testUnknownFieldsFromANewerNodeAreSkipped() throws Exception {
        final String json = "{\"bucket\":\"b-1\",\"future\":{\"x\":[1,2]},\"term\":2,\"members\":{\"u\":{\"index\":\"i\",\"later\":[3],"
            + "\"commit\":{\"term\":2,\"files\":{\"_0.cfs\":\"t=2\"}}}},\"tail\":5}";
        final BucketManifest read = parse(json.getBytes(StandardCharsets.UTF_8));
        assertEquals("b-1", read.bucketId());
        assertEquals(2, read.member("u").orElseThrow().commit().term());
    }

    public void testMalformedManifestsAreRefused() {
        expectThrows(IOException.class, () -> parse("{\"term\":1,\"members\":{}}".getBytes(StandardCharsets.UTF_8)));
        expectThrows(IOException.class, () -> parse("{\"bucket\":\"b\",\"members\":{}}".getBytes(StandardCharsets.UTF_8)));
        expectThrows(
            IOException.class,
            () -> parse("{\"bucket\":\"b\",\"term\":1,\"members\":{\"u\":{}}}".getBytes(StandardCharsets.UTF_8))
        );
    }

    public void testBucketIdsAreOnePathSegment() {
        assertEquals("b-01_x", BucketIds.validate("b-01_x"));
        for (String bad : new String[] { "", "B", "a/b", "a.b", "x".repeat(BucketIds.MAX_LENGTH + 1) }) {
            expectThrows(IllegalArgumentException.class, () -> BucketIds.validate(bad));
        }
    }

    public void testTheRegisterIsCompareAndSwap() throws Exception {
        try (FsBlobStore blobs = new FsBlobStore(8192, createTempDir(), false)) {
            final BucketStore store = new BucketStore(blobs, BlobPath.cleanPath());
            assertTrue(store.read("b-1").isEmpty());
            final BucketManifest first = BucketManifest.empty("b-1").withMember("u1", new BucketManifest.Member("i1", null));
            final long created = store.swap(first, BlobRegister.ABSENT_GENERATION).orElseThrow();
            assertTrue("creating twice must fail", store.swap(first, BlobRegister.ABSENT_GENERATION).isEmpty());

            final BucketStore.Read read = store.read("b-1").orElseThrow();
            assertEquals(created, read.generation());
            assertEquals(first, read.manifest());
            final BucketManifest second = read.manifest().withMember("u2", new BucketManifest.Member("i2", null));
            assertTrue(store.swap(second, read.generation()).isPresent());
            assertTrue(
                "a change from a stale read must not land",
                store.swap(read.manifest().withoutMember("u1"), read.generation()).isEmpty()
            );
            assertEquals(2, store.read("b-1").orElseThrow().manifest().size());
            assertEquals(BlobPath.cleanPath().add("buckets").add("b-1").add("p"), store.packsPath("b-1"));
        }
    }

    public void testTheDescriptorCarriesItsBucket() throws Exception {
        final IndexDescriptor plain = new IndexDescriptor("small", "uuid-small", 1, MAPPING, null);
        assertNull(plain.bucket());
        assertFalse(
            "a descriptor with no bucket must serialize as it did before buckets",
            plain.toBytes().utf8ToString().contains("bucket")
        );
        final IndexDescriptor bucketed = plain.withBucket("b-0007");
        assertEquals("b-0007", bucketed.bucket());
        assertNotEquals(plain, bucketed);
        final IndexDescriptor read;
        try (InputStream in = bucketed.toBytes().streamInput()) {
            read = IndexDescriptor.fromStream(in);
        }
        assertEquals(bucketed, read);
        assertEquals("b-0007", read.bucket());
        assertNull("promotion takes it out of the bucket", read.withBucket(null).bucket());
        assertEquals("a mapping change keeps the bucket", "b-0007", read.withMapping(MAPPING).bucket());
    }

    public void testOnlyOneShardIndicesGoInABucket() {
        final IndexDescriptor wide = new IndexDescriptor("wide", "uuid-wide", 3, MAPPING, null);
        expectThrows(IllegalArgumentException.class, () -> wide.withBucket("b-1"));
        final IndexDescriptor small = new IndexDescriptor("small", "uuid-small", 1, MAPPING, null);
        expectThrows(IllegalArgumentException.class, () -> small.withBucket("Not/Valid"));
    }
}
