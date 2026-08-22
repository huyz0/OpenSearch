/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Property-based tests for {@link AnalyticsBackendNativeMemoryStats} XContent rendering correctness.
 *
 * <p>Rendered shape in {@code _nodes/stats}:
 * <pre>{@code
 * "analytics_backend": { "allocated_bytes": ..., "resident_bytes": ..., "purge_count": ... }
 * }</pre>
 *
 * <p>The {@code analytics_backend} object used to be opened by {@code toXContent} itself. Now that
 * this type is a {@code PluginNodeStats}, {@code NodeStats.toXContent} opens the object from
 * {@link AnalyticsBackendNativeMemoryStats#getWriteableName()} and the fragment renders only the
 * fields inside it. These tests therefore render through {@link #renderAsNodeStatsWould} — the exact
 * two lines {@code NodeStats} runs for every plugin contribution — and still assert the same
 * {@code analytics_backend.<field>} paths, because the whole point of the migration was to leave the
 * emitted JSON unchanged.
 */
public class AnalyticsBackendNativeMemoryStatsXContentTests extends OpenSearchTestCase {

    /**
     * Renders the stats exactly as {@code NodeStats.toXContent} renders a {@code PluginNodeStats}
     * contribution: open an object named after {@code getWriteableName()}, emit the fragment inside
     * it, close it.
     */
    private static Map<String, Object> renderAsNodeStatsWould(AnalyticsBackendNativeMemoryStats stats) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.startObject(stats.getWriteableName());
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);
    }

    /**
     * Property: any valid (allocatedBytes, residentBytes) pair renders as a single
     * {@code analytics_backend} object with both fields verbatim. Iterates 100 random
     * inputs, including the {@code -1} error sentinel.
     */
    public void testXContentRenderingCorrectness() throws Exception {
        for (int i = 0; i < 100; i++) {
            long allocatedBytes = randomBoolean() ? -1L : randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            long residentBytes = randomBoolean() ? -1L : randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);

            AnalyticsBackendNativeMemoryStats stats = new AnalyticsBackendNativeMemoryStats(allocatedBytes, residentBytes, 0);
            Map<String, Object> root = renderAsNodeStatsWould(stats);

            // Top-level key must be "analytics_backend" and nothing else.
            assertTrue("Expected 'analytics_backend' on iteration " + i + ", got: " + root.keySet(), root.containsKey("analytics_backend"));
            assertEquals("Expected exactly one top-level key on iteration " + i, 1, root.size());

            // analytics_backend must contain exactly the three fields.
            @SuppressWarnings("unchecked")
            Map<String, Object> analyticsBackend = (Map<String, Object>) root.get("analytics_backend");
            assertNotNull("analytics_backend should not be null on iteration " + i, analyticsBackend);
            assertEquals("Expected exactly 3 fields on iteration " + i, 3, analyticsBackend.size());
            assertEquals(
                "allocated_bytes mismatch on iteration " + i + " for value: " + allocatedBytes,
                allocatedBytes,
                ((Number) analyticsBackend.get("allocated_bytes")).longValue()
            );
            assertEquals(
                "resident_bytes mismatch on iteration " + i + " for value: " + residentBytes,
                residentBytes,
                ((Number) analyticsBackend.get("resident_bytes")).longValue()
            );
            assertEquals("purge_count mismatch on iteration " + i, 0L, ((Number) analyticsBackend.get("purge_count")).longValue());

            // Sanity: the parent-level fields are NOT emitted by this class.
            assertFalse("native_memory wrapper is owned by NodeStats, not this class", root.containsKey("native_memory"));
            assertFalse(
                "total_estimated_bytes is computed in NodeStats from OsProbe, not emitted here",
                root.containsKey("total_estimated_bytes")
            );
        }
    }

    /**
     * The object name is the contribution's writeable name, so the wire-framing key, the
     * {@code pluginStats} map key and the rendered JSON key can never drift apart.
     */
    public void testWriteableNameIsTheRenderedKey() {
        assertEquals("analytics_backend", AnalyticsBackendNativeMemoryStats.WRITEABLE_NAME);
        assertEquals(AnalyticsBackendNativeMemoryStats.WRITEABLE_NAME, new AnalyticsBackendNativeMemoryStats(1, 2, 3).getWriteableName());
    }

    /**
     * A non-zero purge count renders verbatim alongside the two byte counters. Carried over from
     * the {@code AnalyticsBackendNativeMemoryStatsPropertyTests} that used to sit beside this type
     * in {@code :server}; the rest of that class duplicated
     * {@link AnalyticsBackendNativeMemoryStatsSerializationTests} and went away with the move.
     */
    @SuppressWarnings("unchecked")
    public void testToXContentIncludesPurgeCount() throws Exception {
        Map<String, Object> root = renderAsNodeStatsWould(new AnalyticsBackendNativeMemoryStats(1024L, 2048L, 5));
        Map<String, Object> ab = (Map<String, Object>) root.get("analytics_backend");
        assertEquals(1024L, ((Number) ab.get("allocated_bytes")).longValue());
        assertEquals(2048L, ((Number) ab.get("resident_bytes")).longValue());
        assertEquals(5L, ((Number) ab.get("purge_count")).longValue());
    }

    /** Error sentinel (-1, -1) renders verbatim. */
    public void testXContentRenderingWithErrorState() throws Exception {
        Map<String, Object> root = renderAsNodeStatsWould(new AnalyticsBackendNativeMemoryStats(-1, -1, 0));

        @SuppressWarnings("unchecked")
        Map<String, Object> analyticsBackend = (Map<String, Object>) root.get("analytics_backend");
        assertNotNull("analytics_backend should be present", analyticsBackend);
        assertEquals(-1L, ((Number) analyticsBackend.get("allocated_bytes")).longValue());
        assertEquals(-1L, ((Number) analyticsBackend.get("resident_bytes")).longValue());
    }

    /** Zero values render as 0, not omitted. */
    public void testXContentRenderingWithZeroValues() throws Exception {
        Map<String, Object> root = renderAsNodeStatsWould(new AnalyticsBackendNativeMemoryStats(0L, 0L, 0));

        @SuppressWarnings("unchecked")
        Map<String, Object> analyticsBackend = (Map<String, Object>) root.get("analytics_backend");
        assertNotNull("analytics_backend should be present", analyticsBackend);
        assertEquals(0L, ((Number) analyticsBackend.get("allocated_bytes")).longValue());
        assertEquals(0L, ((Number) analyticsBackend.get("resident_bytes")).longValue());
    }
}
