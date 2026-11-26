/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.repository;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The two optional fields a record's index entry may carry for its aliases: that they survive every re-encoding of a
 * record, that a corrupt one never breaks a read that does not use it, and that the restore's reader refuses it.
 */
public class SnapshotAliasRecordTests extends OpenSearchTestCase {

    /** The exact bytes of a record with aliases; the same text is the fixture a Rust node is checked against. */
    static final String WITH_ALIASES =
        "{\"repo\":\"repo\",\"name\":\"view\",\"uuid\":\"uuid\",\"shallow\":false,\"start_time_in_millis\":1000,"
            + "\"end_time_in_millis\":2000,\"indices\":{\"index\":{\"uuid\":\"uuid-index\",\"number_of_shards\":1,"
            + "\"aliases\":{\"logs-read\":{},\"logs-write\":{\"is_write_index\":true}},\"aliases_truncated\":true,"
            + "\"shards\":[{\"shard\":0,\"term\":1,\"files\":{\"file\":\"t=1\"}}]}}}";

    private static SnapshotRecord.SnapshottedIndex index(Object aliases, Object truncated) {
        return new SnapshotRecord.SnapshottedIndex(
            "uuid-index",
            1,
            null,
            Map.of(),
            Map.of(0, new CommitManifest(1L, Map.of("file", "t=1"), null, Map.of())),
            aliases,
            truncated
        );
    }

    private static SnapshotRecord record(SnapshotRecord.SnapshottedIndex index) {
        return new SnapshotRecord("repo", "view", "uuid", false, 1000L, 2000L, Map.of("index", index));
    }

    private static SnapshotRecord parse(String json) throws IOException {
        return SnapshotRecord.fromStream(new org.opensearch.core.common.bytes.BytesArray(json).streamInput());
    }

    public void testARecordWithAliasesWritesTheDocumentedBytes() throws Exception {
        final Map<String, Object> aliases = new LinkedHashMap<>();
        aliases.put("logs-read", Map.of());
        aliases.put("logs-write", Map.of("is_write_index", true));
        assertEquals(WITH_ALIASES, record(index(aliases, true)).toBytes().utf8ToString());
    }

    public void testARecordWithoutAliasesIsByteForByteWhatItWasBefore() throws Exception {
        final String bytes = record(index(null, null)).toBytes().utf8ToString();
        assertFalse(bytes, bytes.contains("aliases"));
        assertFalse(bytes, bytes.contains("aliases_truncated"));
    }

    public void testAnOldRecordReadsAsAnIndexWithNoAliases() throws Exception {
        final var read = parse(record(index(null, null)).toBytes().utf8ToString()).indices().get("index");
        assertNull(read.aliases());
        assertNull(read.aliasesTruncated());
        final var none = SnapshotAliases.read(read);
        assertTrue(none.writeFlags().isEmpty());
        assertFalse(none.truncated());
    }

    public void testBothFieldsSurviveAReadAndARewrite() throws Exception {
        final var read = parse(WITH_ALIASES);
        assertEquals(WITH_ALIASES, read.toBytes().utf8ToString());
        assertEquals(WITH_ALIASES, read.withStorageGeneration(null).toBytes().utf8ToString());
        final var decoded = SnapshotAliases.read(read.indices().get("index"));
        assertEquals(Map.of("logs-read", false, "logs-write", true), decoded.writeFlags());
        assertTrue(decoded.truncated());
    }

    public void testTheStorageGenerationRewriteKeepsThem() throws Exception {
        final String generation = org.opensearch.common.UUIDs.randomBase64UUID();
        final var reserved = parse(WITH_ALIASES).withStorageGeneration(generation);
        final var back = parse(reserved.toBytes().utf8ToString()).indices().get("index");
        assertEquals(Map.of("logs-read", false, "logs-write", true), SnapshotAliases.read(back).writeFlags());
        assertTrue(SnapshotAliases.read(back).truncated());
    }

    public void testIsWriteIndexFalseAndAbsentBothMeanNotWrite() throws Exception {
        final var aliases = Map.of("a", Map.of("is_write_index", false), "b", Map.of());
        assertEquals(Map.of("a", false, "b", false), SnapshotAliases.read(index(aliases, null)).writeFlags());
    }

    /** What a summary parse (list, get, delete, a janitor's pins, adoption) reads must not depend on these fields. */
    public void testACorruptAliasEntryStillParsesAndIsCarriedThrough() throws Exception {
        final Object[][] corrupt = {
            { "not an object", null },
            { Map.of("a", "x"), null },
            { Map.of("a", Map.of("is_write_index", "yes")), null },
            { Map.of("", Map.of()), null },
            { Map.of("a", Map.of()), "yes" },
            { null, 7 } };
        for (Object[] entry : corrupt) {
            final var written = record(index(entry[0], entry[1]));
            final var read = parse(written.toBytes().utf8ToString());
            assertEquals(1, read.indices().get("index").shards().size());
            assertEquals(written.toBytes().utf8ToString(), read.toBytes().utf8ToString());
        }
    }

    public void testTheRestoreReaderRefusesACorruptEntry() throws Exception {
        final Map<String, Object> sixtyFive = new LinkedHashMap<>();
        for (int i = 0; i < 65; i++) {
            sixtyFive.put("a" + i, Map.of());
        }
        final Object[][] corrupt = {
            { "not an object", null },
            { java.util.List.of(), null },
            { Map.of("a", "x"), null },
            { Map.of("a", Map.of("is_write_index", "yes")), null },
            { Map.of("a", Map.of("is_write_index", 1)), null },
            { Map.of("", Map.of()), null },
            { Map.of("x".repeat(256), Map.of()), null },
            { Map.of("a", Map.of()), "yes" },
            { null, 7 },
            { sixtyFive, null } };
        for (Object[] entry : corrupt) {
            final var read = index(entry[0], entry[1]);
            final IOException refused = expectThrows(IOException.class, () -> SnapshotAliases.read(read));
            assertTrue(refused.getMessage(), refused.getMessage().startsWith("malformed snapshot record"));
        }
    }

    public void testTheLimitsThemselvesAreAccepted() throws Exception {
        final Map<String, Object> sixtyFour = new LinkedHashMap<>();
        for (int i = 0; i < 64; i++) {
            sixtyFour.put("a" + i, Map.of());
        }
        assertEquals(64, SnapshotAliases.read(index(sixtyFour, null)).writeFlags().size());
        assertEquals(1, SnapshotAliases.read(index(Map.of("é".repeat(127) + "x", Map.of()), null)).writeFlags().size());
        assertEquals(1, SnapshotAliases.read(index(Map.of("x".repeat(255), Map.of()), null)).writeFlags().size());
    }

    public void testTheEncoderWritesOnlyTheExplicitWriteMember() {
        final Map<String, Boolean> flags = new LinkedHashMap<>();
        flags.put("read", false);
        flags.put("write", true);
        assertEquals(Map.of("read", Map.of(), "write", Map.of("is_write_index", true)), SnapshotAliases.encode(flags));
    }

    /** Adoption of a record another node wrote straight into the inventory re-encodes it, and keeps the fields. */
    public void testAdoptingALegacyRecordKeepsTheFields() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
            plane.putRepository(new RepositoryDescriptor("repo", "fs", Map.of(), 1_000L));
            plane.createIndex(new IndexDescriptor("index", "uuid-index", 1, "{}", null));
            final var legacy = parse(WITH_ALIASES);
            final var bytes = legacy.toBytes();
            try (var input = bytes.streamInput()) {
                store.blobContainer(SnapshotPaths.snapshots(BlobPath.cleanPath()))
                    .writeBlobAtomic(legacy.key(), input, bytes.length(), true);
            }
            final var adopted = plane.snapshot("repo", "view").orElseThrow();
            assertEquals(WITH_ALIASES, adopted.toBytes().utf8ToString());
            // A second read goes through the register the adoption wrote, not the legacy blob.
            assertEquals(WITH_ALIASES, plane.snapshot("repo", "view").orElseThrow().toBytes().utf8ToString());
        }
    }

    /** A corrupt alias entry in a legacy record does not stop adoption either: only the restore reads it. */
    public void testAdoptingALegacyRecordWithACorruptEntryStillWorks() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
            plane.putRepository(new RepositoryDescriptor("repo", "fs", Map.of(), 1_000L));
            plane.createIndex(new IndexDescriptor("index", "uuid-index", 1, "{}", null));
            final var legacy = record(index("not an object", 3));
            final var bytes = legacy.toBytes();
            try (var input = bytes.streamInput()) {
                store.blobContainer(SnapshotPaths.snapshots(BlobPath.cleanPath()))
                    .writeBlobAtomic(legacy.key(), input, bytes.length(), true);
            }
            assertEquals(bytes.utf8ToString(), plane.snapshot("repo", "view").orElseThrow().toBytes().utf8ToString());
        }
    }

    /**
     * A duplicate key anywhere in a record, the alias member included, makes the whole record malformed: the shell's JSON
     * parser rejects duplicates before an entry can be kept opaque. A Rust writer never emits one.
     */
    public void testADuplicateAliasKeyMakesTheRecordMalformedLikeAnyOtherDuplicateKey() {
        final String duplicate = WITH_ALIASES.replace("\"logs-read\":{}", "\"logs-read\":{},\"logs-read\":{}");
        expectThrows(Exception.class, () -> parse(duplicate));
    }
}
