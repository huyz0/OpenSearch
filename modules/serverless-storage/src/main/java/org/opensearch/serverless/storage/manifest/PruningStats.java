/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Per-shard summary statistics carried in a {@link CommitManifest}, small enough to stay in the
 * manifest but rich enough for a coordinator to decide "this shard cannot match" without opening
 * it &mdash; the prune-before-activate protocol (rfc-serverless-metadata-plane.md &sect;5.1).
 */
public final class PruningStats implements Writeable {

    private final long documentCount;
    private final Long minTimestampMillis;
    private final Long maxTimestampMillis;
    private final Map<String, FieldRange> fieldRanges;

    public PruningStats(long documentCount, Long minTimestampMillis, Long maxTimestampMillis, Map<String, FieldRange> fieldRanges) {
        if (documentCount < 0) {
            throw new IllegalArgumentException("documentCount must be >= 0, got " + documentCount);
        }
        if (minTimestampMillis != null && maxTimestampMillis != null && minTimestampMillis > maxTimestampMillis) {
            throw new IllegalArgumentException(
                "minTimestampMillis (" + minTimestampMillis + ") must be <= maxTimestampMillis (" + maxTimestampMillis + ")"
            );
        }
        this.documentCount = documentCount;
        this.minTimestampMillis = minTimestampMillis;
        this.maxTimestampMillis = maxTimestampMillis;
        this.fieldRanges = Map.copyOf(Objects.requireNonNull(fieldRanges, "fieldRanges"));
    }

    public static PruningStats empty() {
        return new PruningStats(0, null, null, Map.of());
    }

    public PruningStats(StreamInput in) throws IOException {
        this.documentCount = in.readVLong();
        this.minTimestampMillis = in.readOptionalLong();
        this.maxTimestampMillis = in.readOptionalLong();
        this.fieldRanges = in.readMap(StreamInput::readString, FieldRange::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(documentCount);
        out.writeOptionalLong(minTimestampMillis);
        out.writeOptionalLong(maxTimestampMillis);
        out.writeMap(fieldRanges, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    public long documentCount() {
        return documentCount;
    }

    public Long minTimestampMillis() {
        return minTimestampMillis;
    }

    public Long maxTimestampMillis() {
        return maxTimestampMillis;
    }

    public Map<String, FieldRange> fieldRanges() {
        return fieldRanges;
    }

    /**
     * Whether the time range [{@code queryFromMillis}, {@code queryToMillis}] (inclusive) could
     * possibly overlap this shard's timestamp range. Returns {@code true} (i.e. "cannot prune")
     * when this shard carries no timestamp stats at all &mdash; the safe default per the
     * pruning-safety invariant: a stale or absent digest may only cause a false activation,
     * never a false skip.
     */
    public boolean mightOverlapTimeRange(long queryFromMillis, long queryToMillis) {
        if (minTimestampMillis == null || maxTimestampMillis == null) {
            return true;
        }
        return minTimestampMillis <= queryToMillis && maxTimestampMillis >= queryFromMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PruningStats)) return false;
        PruningStats that = (PruningStats) o;
        return documentCount == that.documentCount
            && Objects.equals(minTimestampMillis, that.minTimestampMillis)
            && Objects.equals(maxTimestampMillis, that.maxTimestampMillis)
            && fieldRanges.equals(that.fieldRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentCount, minTimestampMillis, maxTimestampMillis, fieldRanges);
    }

    @Override
    public String toString() {
        return "PruningStats{docs="
            + documentCount
            + ", ts=["
            + minTimestampMillis
            + ","
            + maxTimestampMillis
            + "], fields="
            + fieldRanges
            + '}';
    }

    /** Min/max of one numeric (post date/number coercion) field across a shard's documents. */
    public static final class FieldRange implements Writeable {
        private final long min;
        private final long max;

        public FieldRange(long min, long max) {
            if (min > max) {
                throw new IllegalArgumentException("min (" + min + ") must be <= max (" + max + ")");
            }
            this.min = min;
            this.max = max;
        }

        public FieldRange(StreamInput in) throws IOException {
            this(in.readLong(), in.readLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(min);
            out.writeLong(max);
        }

        public long min() {
            return min;
        }

        public long max() {
            return max;
        }

        public boolean mightOverlap(long queryMin, long queryMax) {
            return min <= queryMax && max >= queryMin;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FieldRange)) return false;
            FieldRange that = (FieldRange) o;
            return min == that.min && max == that.max;
        }

        @Override
        public int hashCode() {
            return Objects.hash(min, max);
        }

        @Override
        public String toString() {
            return "[" + min + "," + max + "]";
        }
    }

    /** Builder-free helper to keep field-range maps deterministically ordered for tests/logging. */
    public static Map<String, FieldRange> orderedFieldRanges(Map<String, FieldRange> ranges) {
        return new TreeMap<>(ranges);
    }
}
