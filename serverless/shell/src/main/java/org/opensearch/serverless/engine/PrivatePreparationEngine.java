/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.SegmentsStats;

/** Native private engine with constant-space writer/version-map accounting, without walking readers. */
final class PrivatePreparationEngine extends InternalEngine {
    private final SegmentsStats bufferStats = new SegmentsStats();

    PrivatePreparationEngine(EngineConfig configuration) {
        super(configuration);
    }

    /** Estimates native buffers only, including tombstones and bytes moving through refresh/flush. */
    record Buffers(long writerBytes, long versionMapBytes, long refreshingMapBytes) {
        Buffers {
            if (writerBytes < 0 || versionMapBytes < 0 || refreshingMapBytes < 0) {
                throw new IllegalArgumentException("negative private buffer accounting");
            }
        }

        long totalBytes() {
            return Math.addExact(Math.addExact(writerBytes, versionMapBytes), refreshingMapBytes);
        }
    }

    Buffers buffers() {
        ensureOpen();
        // Reuse the accumulator instead of allocating segment/stat maps for every mutation.
        // This lock only serializes statistics; no indexing, refresh or lifecycle work holds it.
        synchronized (bufferStats) {
            bufferStats.addIndexWriterMemoryInBytes(-bufferStats.getIndexWriterMemoryInBytes());
            bufferStats.addVersionMapMemoryInBytes(-bufferStats.getVersionMapMemoryInBytes());
            writerSegmentStats(bufferStats);
            // Writer RAM already includes its flushing bytes. Only the old version map is
            // additional to the writer/current-map/tombstone estimates sampled above.
            final long refreshingMapBytes = Math.max(0L, getWritingBytes() - documentIndexWriter.getFlushingBytes());
            return new Buffers(bufferStats.getIndexWriterMemoryInBytes(), bufferStats.getVersionMapMemoryInBytes(), refreshingMapBytes);
        }
    }
}
