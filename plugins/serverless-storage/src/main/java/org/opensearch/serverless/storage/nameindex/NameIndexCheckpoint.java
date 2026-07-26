/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A12. Reads and writes a {@link CompactNameIndex} as a stream, so the tier can restart without
 * reconstructing itself from the manifest.
 *
 * <p>A18 is what made this the simpler of the two options. The concern was that a rebuild needs headroom
 * proportional to the index, which would have argued for streaming persistence over checkpointing; the
 * measurement showed retained memory across a rebuild is near zero and only allocation churn is high. So
 * the choice comes down to start-up time, and loading packed arrays beats replaying a 100M-entry manifest.
 *
 * <h2>Entries, not arrays</h2>
 *
 * The obvious format is the packed arrays themselves, which would load with no parsing at all. This
 * writes entries instead, and reads them back through {@link CompactNameIndexBuilder#buildFromSorted}.
 * Two reasons. The packed layout is an implementation detail that has already changed once, when A15
 * split the name blob into chunks; a checkpoint written before that change would be unreadable after it,
 * for a structure that is supposed to survive restarts. And entries are what the wire format already
 * describes, so there is one serialisation of an index name rather than two that can drift.
 *
 * <p>Load is still a single sequential pass with no sort, because entries are written in the order the
 * structure already holds them, which is the order the builder requires.
 */
public final class NameIndexCheckpoint {

    /**
     * Format version, independent of any codec elsewhere. A reader that meets a version it does not know
     * must refuse rather than guess, because a half-understood name index is a cluster that cannot find
     * its own indices.
     */
    public static final int VERSION = 1;

    private NameIndexCheckpoint() {}

    /**
     * Writes the structure as a version, a count, and that many entries in ascending byte order.
     *
     * <p>The count is written up front so a truncated checkpoint is detectable. Without it a stream that
     * ended early would load as a smaller but perfectly valid index, which is the failure mode where a
     * cluster silently loses indices rather than refusing to start.
     */
    public static void write(CompactNameIndex index, StreamOutput out) throws IOException {
        out.writeVInt(VERSION);
        out.writeVInt(index.size());
        for (int ordinal = 0; ordinal < index.size(); ordinal++) {
            index.entryAt(ordinal).writeTo(out);
        }
    }

    /** Reads a checkpoint written by {@link #write}. */
    public static CompactNameIndex read(StreamInput in) throws IOException {
        int version = in.readVInt();
        if (version != VERSION) {
            throw new IOException("unsupported name index checkpoint version " + version + ", expected " + VERSION);
        }
        int count = in.readVInt();

        // Materialised rather than streamed straight into the builder, because buildFromSorted takes two
        // passes and a StreamInput cannot be rewound. The list holds entries, not the packed form, so
        // this is the one place the load path pays per-entry overhead; it is bounded and transient.
        List<IndexNameEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(new IndexNameEntry(in));
        }
        if (entries.size() != count) {
            throw new IOException("checkpoint declared " + count + " entries but held " + entries.size());
        }

        return CompactNameIndexBuilder.buildFromSorted(entries::iterator);
    }

    /** Round-trips through the entry stream without touching a stream, for tests and for rebuild reuse. */
    static CompactNameIndex copyOf(CompactNameIndex index) {
        return CompactNameIndexBuilder.buildFromSorted(() -> new Iterator<>() {
            private int ordinal = 0;

            @Override
            public boolean hasNext() {
                return ordinal < index.size();
            }

            @Override
            public IndexNameEntry next() {
                return index.entryAt(ordinal++);
            }
        });
    }
}
