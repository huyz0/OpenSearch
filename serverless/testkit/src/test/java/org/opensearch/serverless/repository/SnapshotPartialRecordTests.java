/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.repository;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** An index entry may name the shards that had no commit; with the captured ones they must cover every shard once. */
public class SnapshotPartialRecordTests extends OpenSearchTestCase {

    private static String entry(int shardCount, String failed, String shards) {
        return "{\"repo\":\"repo\",\"name\":\"view\",\"uuid\":\"uuid\",\"shallow\":true,\"start_time_in_millis\":1000,"
            + "\"end_time_in_millis\":2000,\"indices\":{\"index\":{\"uuid\":\"uuid-index\",\"number_of_shards\":"
            + shardCount
            + (failed == null ? "" : ",\"failed_shards\":" + failed)
            + (shards == null ? "" : ",\"shards\":" + shards)
            + "}}}";
    }

    private static String shard(int number) {
        return "{\"shard\":" + number + ",\"term\":1,\"files\":{\"f\":\"t=1\"}}";
    }

    private static SnapshotRecord parse(String json) throws IOException {
        return SnapshotRecord.fromStream(new BytesArray(json).streamInput());
    }

    private static SnapshotRecord.SnapshottedIndex index(String json) throws IOException {
        return parse(json).indices().get("index");
    }

    public void testSomeShardsCapturedAndSomeFailedIsACompleteIndexAndRoundTrips() throws Exception {
        final String json = entry(3, "[1]", "[" + shard(0) + "," + shard(2) + "]");
        final SnapshotRecord record = parse(json);
        assertEquals(List.of(1), record.indices().get("index").failedShards());
        assertFalse(SnapshotRecord.capturing(record.indices().get("index")));
        assertFalse(SnapshotRecord.inProgress(record));
        assertTrue(SnapshotRecord.partial(record));
        assertEquals(List.of(1), parse(record.toBytes().utf8ToString()).indices().get("index").failedShards());
        assertTrue(record.toBytes().utf8ToString().contains("\"failed_shards\":[1]"));
    }

    public void testAnIndexWhoseEveryShardFailedIsNotTheInProgressMarker() throws Exception {
        final SnapshotRecord record = parse(entry(2, "[0,1]", null));
        final var index = record.indices().get("index");
        assertTrue(index.shards().isEmpty());
        assertFalse(SnapshotRecord.capturing(index));
        assertFalse(SnapshotRecord.inProgress(record));
    }

    public void testAnIndexWithNeitherIsStillTheInProgressMarker() throws Exception {
        final var index = index(entry(2, null, null));
        assertTrue(SnapshotRecord.capturing(index));
        assertTrue(index.failedShards().isEmpty());
    }

    public void testACompleteRecordWritesNoFailedShardsField() throws Exception {
        final String bytes = parse(entry(1, null, "[" + shard(0) + "]")).toBytes().utf8ToString();
        assertFalse(bytes, bytes.contains("failed_shards"));
        assertFalse(SnapshotRecord.partial(parse(bytes)));
    }

    public void testCorruptCoverageIsAMalformedRecord() throws Exception {
        final String[] corrupt = {
            entry(3, "[3]", "[" + shard(0) + "," + shard(1) + "," + shard(2) + "]"),
            entry(3, "[1,1]", "[" + shard(0) + "," + shard(2) + "]"),
            entry(3, "[2,1]", "[" + shard(0) + "]"),
            entry(3, "[1]", "[" + shard(0) + "," + shard(1) + "]"),
            entry(3, "[1]", "[" + shard(0) + "]"),
            entry(3, null, "[" + shard(0) + "," + shard(1) + "]"),
            entry(3, "[-1]", "[" + shard(0) + "," + shard(1) + "," + shard(2) + "]"),
            entry(3, "\"1\"", "[" + shard(0) + "]"),
            entry(3, "1", "[" + shard(0) + "]"),
            entry(2, "[0,1]", "[" + shard(0) + "]") };
        for (String json : corrupt) {
            final IOException refused = expectThrows(IOException.class, () -> parse(json));
            assertTrue(json + " -> " + refused.getMessage(), refused.getMessage().startsWith("malformed snapshot record"));
        }
    }

    public void testTheShardCountIsBoundedBeforeAnythingIsAllocated() throws Exception {
        final StringBuilder failed = new StringBuilder("[");
        for (int i = 0; i < 4096; i++) {
            failed.append(i == 0 ? "" : ",").append(i);
        }
        failed.append(']');
        assertEquals(4096, index(entry(4096, failed.toString(), null)).failedShards().size());
        expectThrows(IOException.class, () -> parse(entry(4097, "[0]", null)));
        expectThrows(IOException.class, () -> parse(entry(4097, null, null)));
    }

    public void testFailedShardsSurviveTheStorageGenerationRewriteAndAliasCopy() throws Exception {
        final var original = new SnapshotRecord.SnapshottedIndex(
            "uuid-index",
            2,
            null,
            Map.of(),
            Map.of(0, new CommitManifest(1L, Map.of("f", "t=1"), null, Map.of())),
            null,
            null,
            List.of(1)
        );
        final var record = new SnapshotRecord("repo", "view", "uuid", true, 1L, 2L, Map.of("index", original));
        final var rewritten = record.withStorageGeneration(org.opensearch.common.UUIDs.randomBase64UUID());
        assertEquals(List.of(1), parse(rewritten.toBytes().utf8ToString()).indices().get("index").failedShards());
        assertEquals(List.of(1), original.withAliases(Map.of("a", Map.of()), null).failedShards());
    }
}
