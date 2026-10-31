/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.ratis.server.raftlog.segmented;

import org.apache.ratis.util.SizeInBytes;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.InternalApi;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.regex.Matcher;

/** Bounded native-format prefix checks before the host starts any voting service. */
@InternalApi
public final class OpenSearchLogRecovery {
    private OpenSearchLogRecovery() {}

    private record Segment(Path path, LogSegmentStartEnd range) {
    }

    /**
     * Verifies contiguous unpurged segment ranges and the required persisted position. Uses the
     * pinned native CRC reader for the required segment and latest closed/open tails. Other closed
     * segments remain subject to the SDK's full open-time verification before its division starts.
     * Forces an existing valid open tail and directory before the host can adopt its end as durable.
     * No log contents are repaired, rewritten, or removed. Snapshots and purged prefixes are unsupported.
     * @param current native group's current directory, exclusively leased by the host
     * @param required minimum previously persisted physical index, or -1 for an empty history
     * @param maximumOperationBytes bounded native entry decoder allocation
     * @param maximumSegmentBytes maximum permitted physical segment size including padding
     * @param maximumDirectoryEntries bounded startup directory population
     * @return actual greatest valid physical index, which the host must persist before starting Raft
     * @throws IOException on a missing required prefix, invalid ranges/CRC, excess bounds or failed force
     */
    public static long verifyAndForce(
        Path current,
        long required,
        int maximumOperationBytes,
        long maximumSegmentBytes,
        long maximumDirectoryEntries
    ) throws IOException {
        if (required < -1 || maximumOperationBytes < 1 || maximumSegmentBytes < 1 || maximumDirectoryEntries < 1) {
            throw new IllegalArgumentException("invalid recovery verification bounds");
        }
        final var segments = new ArrayList<Segment>();
        long entries = 0;
        try (var paths = Files.newDirectoryStream(current)) {
            for (var path : paths) {
                if (++entries > maximumDirectoryEntries) {
                    throw new IOException("journal recovery directory exceeds its entry bound");
                }
                final var range = parse(path);
                if (range != null) {
                    if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) == false || Files.size(path) > maximumSegmentBytes) {
                        throw new IOException("journal segment is not regular or exceeds its size bound");
                    }
                    segments.add(new Segment(path, range));
                }
            }
        }
        segments.sort(Comparator.comparingLong(segment -> segment.range().getStartIndex()));
        Segment requiredSegment = null;
        Segment lastClosed = null;
        Segment open = null;
        long nextStart = 0;
        for (int i = 0; i < segments.size(); i++) {
            final var segment = segments.get(i);
            final var range = segment.range();
            if (range.getStartIndex() != nextStart) {
                throw new IOException("journal recovery requires a contiguous prefix starting at zero");
            }
            if (range.isOpen()) {
                if (i != segments.size() - 1) {
                    throw new IOException("only the final journal segment may be open");
                }
                open = segment;
            } else {
                lastClosed = segment;
                if (range.getEndIndex() == Long.MAX_VALUE) {
                    throw new IOException("journal segment index is exhausted");
                }
                nextStart = range.getEndIndex() + 1;
            }
            if (range.getStartIndex() <= required && (range.isOpen() || required <= range.getEndIndex())) {
                requiredSegment = segment;
            }
        }
        if (required >= 0 && requiredSegment == null) {
            throw new IOException("previously persisted journal boundary is missing; voter replacement is required");
        }
        long last = -1;
        if (lastClosed != null) {
            last = readThrough(lastClosed, Long.MAX_VALUE, maximumOperationBytes);
            if (last != lastClosed.range().getEndIndex()) {
                throw new IOException("closed journal segment does not contain its declared end");
            }
        }
        if (open != null) {
            // An empty newly created open file cannot have contributed an acknowledged position.
            final long openLast = Files.size(open.path()) == 0
                ? open.range().getStartIndex() - 1
                : readThrough(open, Long.MAX_VALUE, maximumOperationBytes);
            if (openLast >= open.range().getStartIndex()) {
                last = openLast;
            }
        }
        if (last < required) {
            throw new IOException("previously persisted journal tail is missing; voter replacement is required");
        }
        if (requiredSegment != null && requiredSegment != lastClosed && requiredSegment != open) {
            if (readThrough(requiredSegment, required, maximumOperationBytes) != required) {
                throw new IOException("required journal boundary is absent from its segment");
            }
        }
        if (open != null) {
            try (var channel = FileChannel.open(open.path(), StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                channel.force(true);
            }
        }
        try (var directory = FileChannel.open(current, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            directory.force(true);
        }
        return last;
    }

    private static LogSegmentStartEnd parse(Path path) throws IOException {
        final String name = path.getFileName().toString();
        Matcher matcher = LogSegmentStartEnd.getClosedSegmentPattern().matcher(name);
        try {
            if (matcher.matches()) {
                final long start = Long.parseLong(matcher.group(1));
                final long end = Long.parseLong(matcher.group(2));
                if (end < start) {
                    throw new IOException("invalid closed journal segment range");
                }
                return LogSegmentStartEnd.valueOf(start, end);
            }
            matcher = LogSegmentStartEnd.getOpenSegmentPattern().matcher(name);
            return matcher.matches() ? LogSegmentStartEnd.valueOf(Long.parseLong(matcher.group(1))) : null;
        } catch (NumberFormatException failure) {
            throw new IOException("journal segment index exceeds its encoding", failure);
        }
    }

    @SuppressForbidden(reason = "Pinned native CRC reader constructor requires java.io.File")
    private static long readThrough(Segment segment, long through, int maximumOperationBytes) throws IOException {
        long next = segment.range().getStartIndex();
        long last = next - 1;
        try (
            var reader = new SegmentedRaftLogInputStream(
                segment.path().toFile(),
                segment.range(),
                SizeInBytes.valueOf(maximumOperationBytes),
                null
            )
        ) {
            for (var entry = reader.nextEntry(); entry != null; entry = reader.nextEntry()) {
                if (entry.getIndex() != next || next == Long.MAX_VALUE) {
                    throw new IOException("journal segment contains a gap or exhausted index");
                }
                last = next++;
                if (last >= through) {
                    break;
                }
            }
        }
        return last;
    }
}
