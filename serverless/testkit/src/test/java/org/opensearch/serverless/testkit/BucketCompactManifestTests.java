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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.serverless.bucket.BucketManifest;
import org.opensearch.serverless.bucket.BucketStore;
import org.opensearch.serverless.bucket.ManifestCodec;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.PruningDigest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The compact bucket manifest gives back exactly the commits it was given, in every codec, reads one member without the
 * rest, compresses only when that pays, and leaves the JSON manifests already written readable.
 */
public class BucketCompactManifestTests extends OpenSearchTestCase {

    /** Every shape of location a commit records, with lengths that match, differ from or are missing for them. */
    private static CommitManifest everyShape() {
        final Map<String, String> files = new LinkedHashMap<>();
        final Map<String, Long> lengths = new LinkedHashMap<>();
        files.put("_0.cfs", "@AlpgfwAAQACoTLx8AAAAAA/e=00000001794472211738/00000000000000000175#15985:3093");
        lengths.put("_0.cfs", 3093L);
        files.put("_0.cfe", "@AlpgfwAAQACoTLx8AAAAAA/e=00000001794472211738/00000000000000000175#19078:549");
        lengths.put("_0.cfe", 549L);
        files.put("_1.si", "@OtherNodeAAAAAAAAAAAAA/e=00000000000000000002/00000000000000000003#0:333");
        lengths.put("_1.si", 333L);
        files.put("_2_Lucene90_0.dvd", "t=4/pack-7#12:40");
        lengths.put("_2_Lucene90_0.dvd", 41L);
        files.put("_3.cfs", "t=4");
        lengths.put("_3.cfs", 1234L);
        files.put("segments_a", "t=4");
        final PruningDigest digest = new PruningDigest(Map.of("ts", new PruningDigest.FieldRange("date", 10L, 99L, "epoch_millis", null)));
        return new CommitManifest(4, files, "AlpgfwAAQACoTLx8AAAAAA", lengths, digest, 77).withStageSequence(12);
    }

    private static String json(PruningDigest digest) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            digest.toXContent(builder);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    private static void assertSameCommit(CommitManifest expected, CommitManifest actual) throws IOException {
        assertEquals(expected.term(), actual.term());
        assertEquals(expected.writer(), actual.writer());
        assertEquals(expected.files(), actual.files());
        assertEquals(expected.lengths(), actual.lengths());
        assertEquals(expected.walOrdinal(), actual.walOrdinal());
        assertEquals(expected.stageSequence(), actual.stageSequence());
        assertEquals(json(expected.digest()), json(actual.digest()));
    }

    private static BucketManifest bucket() {
        return BucketManifest.empty("s01-2")
            .withMember("uuid-a", new BucketManifest.Member("logs-a", everyShape(), 5))
            .withMember("uuid-b", new BucketManifest.Member("logs-b", null))
            .withMember("uuid-c", new BucketManifest.Member("logs-c", new CommitManifest(1, Map.of("_0.cfs", "t=1"), null, Map.of()), 1))
            .atTerm(9);
    }

    public void testEveryCodecGivesBackExactlyTheCommits() throws Exception {
        final BucketManifest manifest = bucket();
        for (ManifestCodec codec : ManifestCodec.values()) {
            final BytesReference bytes = manifest.toBytes(codec);
            final BucketManifest read = BucketManifest.parse(bytes);
            assertEquals(codec.name(), manifest, read);
            assertEquals(9, read.term());
            assertSameCommit(everyShape(), read.member("uuid-a").orElseThrow().commit());
            assertNull(read.member("uuid-b").orElseThrow().commit());
            assertEquals(0L, read.member("uuid-b").orElseThrow().version());
            assertNull(read.member("uuid-c").orElseThrow().commit().writer());

            final BucketManifest.Member one = BucketManifest.parseMember(bytes, "uuid-a").orElseThrow();
            assertEquals(5L, one.version());
            assertSameCommit(everyShape(), one.commit());
            assertTrue(BucketManifest.parseMember(bytes, "uuid-zzz").isEmpty());
        }
    }

    public void testAJsonManifestStillReads() throws Exception {
        final BucketManifest manifest = bucket();
        final BytesReference json = manifest.toBytes();
        assertEquals('{', json.get(0));
        assertEquals(manifest, BucketManifest.parse(json));
        assertSameCommit(everyShape(), BucketManifest.parseMember(json, "uuid-a").orElseThrow().commit());
    }

    public void testCompressionOnlyWhenItPays() throws Exception {
        final BytesReference small = bucket().toBytesCompressingIfWorthIt(ManifestCodec.ZSTD);
        assertEquals("a small manifest is left uncompressed", ManifestCodec.NONE.id(), small.get(5));
        BucketManifest large = BucketManifest.empty("s01-3");
        for (int i = 0; i < 400; i++) {
            large = large.withMember("uuid-" + i, new BucketManifest.Member("logs-" + i, everyShape(), 1));
        }
        final BytesReference compressed = large.toBytesCompressingIfWorthIt(ManifestCodec.ZSTD);
        assertEquals("a large one that zstd shrinks is compressed", ManifestCodec.ZSTD.id(), compressed.get(5));
        assertTrue(compressed.length() < large.toBytes(ManifestCodec.NONE).length() * 0.8);
        assertEquals(large, BucketManifest.parse(compressed));
    }

    public void testAStoreWritesItsCodecAndReadsAnyOther() throws Exception {
        try (FsBlobStore blobs = new FsBlobStore(8192, createTempDir(), false)) {
            final BucketStore jsonWriter = new BucketStore(blobs, BlobPath.cleanPath()).codec(null);
            final BucketStore compactWriter = new BucketStore(blobs, BlobPath.cleanPath());
            final long generation = jsonWriter.swap(bucket(), BlobRegister.ABSENT_GENERATION).orElseThrow();
            final BucketStore.Read read = compactWriter.read("s01-2").orElseThrow();
            assertEquals(bucket(), read.manifest());
            compactWriter.swap(read.manifest().atTerm(10), generation).orElseThrow();
            assertEquals(10, jsonWriter.read("s01-2").orElseThrow().manifest().term());
            assertSameCommit(everyShape(), jsonWriter.readMember("s01-2", "uuid-a").orElseThrow().commit());
        }
    }

    public void testMalformedCompactManifestsAreRefused() throws Exception {
        final byte[] good = BytesReference.toBytes(bucket().toBytes(ManifestCodec.NONE));
        final byte[] unknownCodec = good.clone();
        unknownCodec[5] = 99;
        expectThrows(IOException.class, () -> BucketManifest.parse(new BytesArray(unknownCodec)));
        final byte[] unknownFormat = good.clone();
        unknownFormat[4] = 9;
        expectThrows(IOException.class, () -> BucketManifest.parse(new BytesArray(unknownFormat)));
        final byte[] truncated = java.util.Arrays.copyOf(good, good.length / 2);
        expectThrows(IOException.class, () -> BucketManifest.parse(new BytesArray(truncated)));
    }
}
