/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.PluginNodeStats;

import java.io.IOException;
import java.util.Objects;

/**
 * Task cancellation counters from the analytics backend execution layer.
 *
 * <p>Contains 4 counters tracking search tasks and search shard tasks that
 * continue executing in the analytics backend after cancellation.
 *
 * <p>A {@link PluginNodeStats}: read out of the native runtime by
 * {@code NativeBridge#nativeNodeStats()} and contributed to {@code _nodes/stats} by
 * {@code DataFusionPlugin#nodeStats()}, rendered by {@code NodeStats} under the top-level
 * {@link #WRITEABLE_NAME} key.
 *
 * <p>This class used to live in {@code :server} under {@code org.opensearch.plugin.stats}, where
 * core's {@code TaskCancellationStats} carried it as a nullable field fed through a product-named
 * {@code SearchBackEndPlugin} SPI method and a {@code findFirst()} pick in {@code Node}. Core never
 * read the values — it only passed them through to XContent — so the whole field was dropped from
 * core and the type moved here, onto the generic {@code PluginNodeStats} path.
 */
public class AnalyticsBackendTaskCancellationStats implements PluginNodeStats {

    /**
     * The {@code getWriteableName()} of this contribution: the wire-framing key, the
     * {@code pluginStats} map key, and the top-level key it renders under in {@code _nodes/stats}.
     *
     * <p>The two objects this fragment emits keep the names they had when they rendered inside
     * core's {@code task_cancellation} object ({@code analytics_search_task} /
     * {@code analytics_search_shard_task}), as do their fields; only the parent key changed, from
     * {@code task_cancellation} to this one.
     */
    public static final String WRITEABLE_NAME = "analytics_task_cancellation";

    private final long searchTaskCurrent;
    private final long searchTaskTotal;
    private final long searchShardTaskCurrent;
    private final long searchShardTaskTotal;

    /**
     * Construct from individual counter values.
     *
     * @param searchTaskCurrent      current count of search tasks executing post-cancellation
     * @param searchTaskTotal        total count of search tasks that executed post-cancellation
     * @param searchShardTaskCurrent current count of search shard tasks executing post-cancellation
     * @param searchShardTaskTotal   total count of search shard tasks that executed post-cancellation
     */
    public AnalyticsBackendTaskCancellationStats(
        long searchTaskCurrent,
        long searchTaskTotal,
        long searchShardTaskCurrent,
        long searchShardTaskTotal
    ) {
        this.searchTaskCurrent = searchTaskCurrent;
        this.searchTaskTotal = searchTaskTotal;
        this.searchShardTaskCurrent = searchShardTaskCurrent;
        this.searchShardTaskTotal = searchShardTaskTotal;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public AnalyticsBackendTaskCancellationStats(StreamInput in) throws IOException {
        this.searchTaskCurrent = in.readVLong();
        this.searchTaskTotal = in.readVLong();
        this.searchShardTaskCurrent = in.readVLong();
        this.searchShardTaskTotal = in.readVLong();
    }

    /**
     * Returns the current count of search tasks executing post-cancellation.
     */
    public long getSearchTaskCurrent() {
        return searchTaskCurrent;
    }

    /**
     * Returns the total count of search tasks that executed post-cancellation.
     */
    public long getSearchTaskTotal() {
        return searchTaskTotal;
    }

    /**
     * Returns the current count of search shard tasks executing post-cancellation.
     */
    public long getSearchShardTaskCurrent() {
        return searchShardTaskCurrent;
    }

    /**
     * Returns the total count of search shard tasks that executed post-cancellation.
     */
    public long getSearchShardTaskTotal() {
        return searchShardTaskTotal;
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(searchTaskCurrent);
        out.writeVLong(searchTaskTotal);
        out.writeVLong(searchShardTaskCurrent);
        out.writeVLong(searchShardTaskTotal);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("analytics_search_task");
        builder.field("current_count_post_cancel", searchTaskCurrent);
        builder.field("total_count_post_cancel", searchTaskTotal);
        builder.endObject();
        builder.startObject("analytics_search_shard_task");
        builder.field("current_count_post_cancel", searchShardTaskCurrent);
        builder.field("total_count_post_cancel", searchShardTaskTotal);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsBackendTaskCancellationStats that = (AnalyticsBackendTaskCancellationStats) o;
        return searchTaskCurrent == that.searchTaskCurrent
            && searchTaskTotal == that.searchTaskTotal
            && searchShardTaskCurrent == that.searchShardTaskCurrent
            && searchShardTaskTotal == that.searchShardTaskTotal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchTaskCurrent, searchTaskTotal, searchShardTaskCurrent, searchShardTaskTotal);
    }
}
