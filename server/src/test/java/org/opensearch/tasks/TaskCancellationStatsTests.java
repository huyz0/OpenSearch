/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class TaskCancellationStatsTests extends AbstractWireSerializingTestCase<TaskCancellationStats> {
    @Override
    protected Writeable.Reader<TaskCancellationStats> instanceReader() {
        return TaskCancellationStats::new;
    }

    @Override
    protected TaskCancellationStats createTestInstance() {
        return randomInstance();
    }

    public static TaskCancellationStats randomInstance() {
        return new TaskCancellationStats(
            SearchTaskCancellationStatsTests.randomInstance(),
            SearchShardTaskCancellationStatsTests.randomInstance()
        );
    }

    // -----------------------------------------------------------------------
    // Round-trip across the wire versions that matter.
    //
    // V_3_7_0 added an optional analytics-backend cancellation-stats payload to this
    // object. Core never read those counters -- it only forwarded them to XContent -- so
    // the field is gone and the backend plugin now contributes them itself through the
    // generic PluginNodeStats path (NodeStats.pluginStats). The wire slot survives as an
    // always-absent boolean so a V_3_7_0-or-later peer stays byte-aligned; the tests below
    // pin both halves of that contract.
    // -----------------------------------------------------------------------

    /** Round-trip at V_3_7_0, i.e. with the vestigial analytics-backend slot on the wire. */
    public void testRoundTripAtVersionWithVestigialSlot() throws IOException {
        for (int i = 0; i < 100; i++) {
            TaskCancellationStats original = randomInstance();

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(Version.V_3_7_0);
            original.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_3_7_0);
            TaskCancellationStats deserialized = new TaskCancellationStats(in);

            assertEquals("Round-trip at V_3_7_0 failed for instance " + i, original, deserialized);
            assertEquals("V_3_7_0 stream fully consumed", 0, in.available());
        }
    }

    /** Round-trip below V_3_7_0, i.e. with no analytics-backend slot on the wire at all. */
    public void testRoundTripAtVersionWithoutVestigialSlot() throws IOException {
        for (int i = 0; i < 100; i++) {
            TaskCancellationStats original = randomInstance();

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(Version.V_3_6_0);
            original.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_3_6_0);
            TaskCancellationStats deserialized = new TaskCancellationStats(in);

            assertEquals("Round-trip at V_3_6_0 failed for instance " + i, original, deserialized);
            assertEquals("V_3_6_0 stream fully consumed", 0, in.available());
        }
    }

    /**
     * The read path must still consume a REAL analytics-backend payload written by a peer that
     * predates the migration, not just the absent marker this node writes. Writes the pre-migration
     * V_3_7_0 encoding by hand (boolean true + four VLongs) followed by a sentinel, then asserts the
     * sentinel is still readable afterwards -- i.e. the discarded payload did not shift the stream.
     */
    public void testReadDiscardsRealAnalyticsBackendPayloadFromOlderPeer() throws IOException {
        SearchTaskCancellationStats searchStats = new SearchTaskCancellationStats(3, 10);
        SearchShardTaskCancellationStats shardStats = new SearchShardTaskCancellationStats(5, 20);

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_7_0);
        searchStats.writeTo(out);
        shardStats.writeTo(out);
        // Pre-migration payload: present marker + the four counters it carried.
        out.writeBoolean(true);
        out.writeVLong(2L);
        out.writeVLong(147L);
        out.writeVLong(5L);
        out.writeVLong(892L);
        // Sentinel that must survive the discard.
        out.writeString("still-aligned");

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_3_7_0);
        TaskCancellationStats deserialized = new TaskCancellationStats(in);

        assertEquals(searchStats, deserialized.getSearchTaskCancellationStats());
        assertEquals(shardStats, deserialized.getSearchShardTaskCancellationStats());
        assertEquals("stream stayed byte-aligned across the discarded payload", "still-aligned", in.readString());
        assertEquals(0, in.available());
    }

    /**
     * {@code task_cancellation} renders only the two core sub-objects. The analytics backend's
     * {@code analytics_search_task} / {@code analytics_search_shard_task} objects keep those exact
     * names but now render under the plugin's own top-level {@code analytics_task_cancellation} key
     * (see {@code AnalyticsBackendTaskCancellationStatsTests} in the DataFusion plugin), never here.
     */
    public void testToXContentRendersOnlyCoreCounters() throws IOException {
        SearchTaskCancellationStats searchStats = new SearchTaskCancellationStats(3, 10);
        SearchShardTaskCancellationStats shardStats = new SearchShardTaskCancellationStats(5, 20);

        TaskCancellationStats stats = new TaskCancellationStats(searchStats, shardStats);

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = BytesReference.bytes(builder).utf8ToString();

        assertTrue("JSON should contain search_task", json.contains("\"search_task\""));
        assertTrue("JSON should contain search_shard_task", json.contains("\"search_shard_task\""));
        assertFalse("JSON should NOT contain analytics_search_task", json.contains("\"analytics_search_task\""));
        assertFalse("JSON should NOT contain analytics_search_shard_task", json.contains("\"analytics_search_shard_task\""));
    }
}
