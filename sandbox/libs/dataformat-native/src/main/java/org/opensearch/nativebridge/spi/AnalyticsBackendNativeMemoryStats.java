/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.PluginNodeStats;

import java.io.IOException;
import java.util.Objects;

/**
 * Immutable stats POJO holding jemalloc memory metrics from the native (Rust) layer.
 * <p>
 * Reports three counters:
 * <ul>
 *   <li>{@code allocated_bytes} – live malloc'd bytes tracked by jemalloc</li>
 *   <li>{@code resident_bytes} – physical RSS attributed to jemalloc arenas</li>
 *   <li>{@code purge_count} – number of times jemalloc arenas have been purged</li>
 * </ul>
 * A value of {@code -1} indicates an error reading the metric from the native layer.
 *
 * <p>A {@link PluginNodeStats}: produced by {@link NativeMemoryFetcher#fetch()} and contributed to
 * {@code _nodes/stats} by the plugin that owns the native runtime (today: {@code DataFusionPlugin})
 * via {@code Plugin#nodeStats()}, rendered by {@code NodeStats} under the top-level
 * {@link #WRITEABLE_NAME} key.
 *
 * <p>This class used to live in {@code :server} under {@code org.opensearch.plugin.stats}, where it
 * was a plugin-specific type on the core classpath reached through a product-named
 * {@code SearchBackEndPlugin} SPI method. It moved here — next to the FFM fetcher that produces it —
 * when it migrated onto the generic {@code PluginNodeStats} path, the same migration
 * {@code NativeAllocatorPoolStats} went through.
 */
public class AnalyticsBackendNativeMemoryStats implements PluginNodeStats {

    /**
     * The {@code getWriteableName()} of this contribution: the wire-framing key, the
     * {@code pluginStats} map key, and the top-level key it renders under in {@code _nodes/stats}.
     * Unchanged from the object name this type used to open inside its own {@code toXContent}, so
     * the rendered {@code analytics_backend.{allocated_bytes,resident_bytes,purge_count}} field
     * paths survive the migration.
     */
    public static final String WRITEABLE_NAME = "analytics_backend";

    private final long allocatedBytes;
    private final long residentBytes;
    private final long purgeCount;

    public AnalyticsBackendNativeMemoryStats(long allocatedBytes, long residentBytes, long purgeCount) {
        this.allocatedBytes = allocatedBytes;
        this.residentBytes = residentBytes;
        this.purgeCount = purgeCount;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public AnalyticsBackendNativeMemoryStats(StreamInput in) throws IOException {
        this.allocatedBytes = in.readLong();
        this.residentBytes = in.readLong();
        this.purgeCount = in.readLong();
    }

    /**
     * Returns the number of live malloc'd bytes tracked by jemalloc, or -1 on error.
     */
    public long getAllocatedBytes() {
        return allocatedBytes;
    }

    /**
     * Returns the physical RSS attributed to jemalloc arenas, or -1 on error.
     */
    public long getResidentBytes() {
        return residentBytes;
    }

    /**
     * Returns the number of times jemalloc arenas have been purged.
     */
    public long getPurgeCount() {
        return purgeCount;
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(allocatedBytes);
        out.writeLong(residentBytes);
        out.writeLong(purgeCount);
    }

    /**
     * Fragment body rendered inside the {@code "analytics_backend"} object {@code NodeStats} opens
     * for this contribution. The object itself is no longer opened here — {@code NodeStats} opens it
     * from {@link #getWriteableName()} — but the three field names are unchanged.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("allocated_bytes", allocatedBytes);
        builder.field("resident_bytes", residentBytes);
        builder.field("purge_count", purgeCount);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsBackendNativeMemoryStats that = (AnalyticsBackendNativeMemoryStats) o;
        return allocatedBytes == that.allocatedBytes && residentBytes == that.residentBytes && purgeCount == that.purgeCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(allocatedBytes, residentBytes, purgeCount);
    }
}
