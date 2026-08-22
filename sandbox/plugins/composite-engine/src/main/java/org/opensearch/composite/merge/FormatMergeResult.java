/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.Optional;

/**
 * Result of merging a single data format's files.
 * <p>
 * Deliberately has no {@code cleanup()} of its own. Deleting merge output on failure is the
 * store layer's job and is done by {@link CompositeMergeExecutor} through
 * {@link CompositeMergeExecutor.MergeOutputCleaner}; a raw {@code Files.deleteIfExists} here
 * bypassed each format's {@code DataFormatStoreHandler} — notably parquet's native
 * {@code TieredObjectStore} registry, which was left holding entries for deleted files.
 */
@ExperimentalApi
public record FormatMergeResult(DataFormat format, WriterFileSet mergedFiles, RowIdMapping rowIdMapping) {

    public Optional<RowIdMapping> rowIdMappingOpt() {
        return Optional.ofNullable(rowIdMapping);
    }
}
