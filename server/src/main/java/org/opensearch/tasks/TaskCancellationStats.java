/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds stats related to task cancellation.
 */
public class TaskCancellationStats implements ToXContentFragment, Writeable {

    private final SearchTaskCancellationStats searchTaskCancellationStats;
    private final SearchShardTaskCancellationStats searchShardTaskCancellationStats;

    public TaskCancellationStats(
        SearchTaskCancellationStats searchTaskCancellationStats,
        SearchShardTaskCancellationStats searchShardTaskCancellationStats
    ) {
        this.searchTaskCancellationStats = searchTaskCancellationStats;
        this.searchShardTaskCancellationStats = searchShardTaskCancellationStats;
    }

    public TaskCancellationStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            searchTaskCancellationStats = new SearchTaskCancellationStats(in);
        } else {
            searchTaskCancellationStats = new SearchTaskCancellationStats(0, 0);
        }
        searchShardTaskCancellationStats = new SearchShardTaskCancellationStats(in);
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            readAndDiscardAnalyticsBackendSlot(in);
        }
    }

    /**
     * Vestigial wire slot. V_3_7_0 added an optional {@code AnalyticsBackendTaskCancellationStats}
     * payload here — four VLongs behind a boolean — carrying an analytics backend's post-cancellation
     * counters. Core never read those values; it only forwarded them to XContent, so the field was
     * removed and the stats now travel the generic {@code PluginNodeStats} path
     * ({@code NodeStats.pluginStats}) contributed by the backend plugin itself.
     *
     * <p>This node always writes the slot absent (see {@link #writeTo}), but a V_3_7_0-or-later peer
     * that still has the old field can send a real payload, so consume it to stay byte-aligned
     * rather than shifting the rest of the stream. Removable, along with the write side, once the
     * minimum supported wire version is past V_3_7_0.
     */
    private static void readAndDiscardAnalyticsBackendSlot(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            in.readVLong(); // searchTaskCurrent
            in.readVLong(); // searchTaskTotal
            in.readVLong(); // searchShardTaskCurrent
            in.readVLong(); // searchShardTaskTotal
        }
    }

    // package private for testing
    protected SearchShardTaskCancellationStats getSearchShardTaskCancellationStats() {
        return this.searchShardTaskCancellationStats;
    }

    // package private for testing
    protected SearchTaskCancellationStats getSearchTaskCancellationStats() {
        return this.searchTaskCancellationStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("task_cancellation");
        builder.field("search_task", searchTaskCancellationStats);
        builder.field("search_shard_task", searchShardTaskCancellationStats);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            searchTaskCancellationStats.writeTo(out);
        }
        searchShardTaskCancellationStats.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            // Vestigial slot; see readAndDiscardAnalyticsBackendSlot. Always absent.
            out.writeBoolean(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskCancellationStats that = (TaskCancellationStats) o;
        return Objects.equals(searchTaskCancellationStats, that.searchTaskCancellationStats)
            && Objects.equals(searchShardTaskCancellationStats, that.searchShardTaskCancellationStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchTaskCancellationStats, searchShardTaskCancellationStats);
    }
}
