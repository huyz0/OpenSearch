/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Executes a composite merge: primary format first, then secondaries using the
 * row-ID mapping from the primary. Stateless — all state comes from the
 * {@link MergePlan} and the merger map.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeMergeExecutor {

    private static final Logger logger = LogManager.getLogger(CompositeMergeExecutor.class);

    /**
     * Deletes merge output files through the store layer rather than the filesystem.
     * <p>
     * Implemented by {@code CompositeIndexingExecutionEngine::deleteFiles}, which fans the
     * request out to every per-format engine and therefore through each format's
     * {@code DataFormatStoreHandler}. That routing is required, not cosmetic: parquet's handler
     * owns a native {@code TieredObjectStore} registry, and deleting its files with raw NIO
     * behind the handler's back leaves stale registry entries pointing at files that no longer
     * exist.
     */
    @FunctionalInterface
    public interface MergeOutputCleaner {
        /**
         * Deletes {@code filesByFormat} (keyed by {@link DataFormat#name()}) and returns whatever
         * could not be deleted yet, in the same shape.
         */
        Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesByFormat) throws IOException;
    }

    private final Map<DataFormat, Merger> mergers;
    private final MergeOutputCleaner cleaner;

    public CompositeMergeExecutor(Map<DataFormat, Merger> mergers, MergeOutputCleaner cleaner) {
        this.mergers = Map.copyOf(mergers);
        this.cleaner = Objects.requireNonNull(cleaner, "cleaner must not be null");
    }

    /**
     * Executes the merge described by the plan.
     * <p>
     * Failures propagate with their original type: an {@link IOException} raised by a per-format
     * merger leaves this method as an {@link IOException}, matching {@link CompositeMerger#merge}'s
     * declared {@code throws IOException}. (It used to be rewrapped as an
     * {@code UncheckedIOException}, so every {@code catch (IOException)} around a composite merge
     * silently missed it.)
     *
     * @param plan the pre-validated merge plan
     * @return the combined merge result across all formats
     * @throws IOException if any per-format merge fails with an I/O error
     */
    public MergeResult execute(MergePlan plan) throws IOException {
        List<FormatMergeResult> completed = new ArrayList<>();
        try {
            FormatMergeResult primaryResult = mergeFormat(plan, plan.primaryFormat(), null);
            completed.add(primaryResult);

            RowIdMapping mapping = plan.hasSecondaries()
                ? primaryResult.rowIdMappingOpt()
                    .orElseThrow(() -> new IllegalStateException("Primary merge did not produce row-ID mapping required by secondaries"))
                : null;

            for (DataFormat secondary : plan.secondaryFormats()) {
                FormatMergeResult secondaryResult = mergeFormat(plan, secondary, mapping);
                // Track it before the cross-format checks below: the merger has already written
                // this format's output, so if a check fails its files must be cleaned up too.
                completed.add(secondaryResult);
                // Verify secondary produced output when primary did
                if (primaryResult.mergedFiles() != null && secondaryResult.mergedFiles() == null) {
                    throw new IllegalStateException(
                        "Primary format ["
                            + plan.primaryFormat().name()
                            + "] produced merged output but secondary format ["
                            + secondary.name()
                            + "] returned null — possible concurrent merge consumed segments"
                    );
                }
                // Verify secondary merged row count matches primary
                if (primaryResult.mergedFiles() != null && secondaryResult.mergedFiles() != null) {
                    long primaryRows = primaryResult.mergedFiles().numRows();
                    long secondaryRows = secondaryResult.mergedFiles().numRows();
                    if (primaryRows != secondaryRows) {
                        throw new IllegalStateException(
                            "Row count mismatch after merge: primary format ["
                                + plan.primaryFormat().name()
                                + "] has "
                                + primaryRows
                                + " rows but secondary format ["
                                + secondary.name()
                                + "] has "
                                + secondaryRows
                                + " rows"
                        );
                    }
                }
            }

            return toMergeResult(completed, mapping);
        } catch (Exception e) {
            cleanup(completed, e);
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            // {@link Merger#merge} declares only IOException, so nothing else checked can reach
            // here unless a sub-format violates that contract. Wrap it rather than blind-casting
            // to IOException, which would surface as a ClassCastException that hides the real
            // failure.
            throw new IOException("Composite merge failed with an unexpected checked exception", e);
        }
    }

    /**
     * Best-effort removal of merge output already written by formats that completed before the
     * failure. Routed through {@link MergeOutputCleaner} so each format's store handler observes
     * the deletion. A cleanup failure never replaces the merge failure — it is logged and attached
     * as a suppressed exception.
     */
    private void cleanup(List<FormatMergeResult> completed, Exception mergeFailure) {
        Map<String, Collection<String>> filesByFormat = new LinkedHashMap<>();
        for (FormatMergeResult result : completed) {
            WriterFileSet merged = result.mergedFiles();
            if (merged == null || merged.files().isEmpty()) {
                continue;
            }
            filesByFormat.computeIfAbsent(result.format().name(), k -> new ArrayList<>()).addAll(merged.files());
        }
        if (filesByFormat.isEmpty()) {
            return;
        }
        try {
            Map<String, Collection<String>> undeleted = cleaner.deleteFiles(filesByFormat);
            if (undeleted != null && undeleted.isEmpty() == false) {
                logger.warn("merge cleanup left stale merge output behind; will be retried on the next refresh: {}", undeleted);
            }
        } catch (Exception cleanupFailure) {
            mergeFailure.addSuppressed(cleanupFailure);
            logger.warn("failed to delete stale merge output after a failed composite merge", cleanupFailure);
        }
    }

    private FormatMergeResult mergeFormat(MergePlan plan, DataFormat format, RowIdMapping mapping) throws IOException {
        Merger merger = mergers.get(format);
        List<WriterFileSet> files = plan.filesFor(format);
        List<Segment> segments = new ArrayList<>();
        for (WriterFileSet wfs : files) {
            segments.add(Segment.builder(wfs.writerGeneration()).addSearchableFiles(format, wfs).build());
        }
        MergeResult result = merger.merge(new MergeInput(segments, mapping, plan.mergedWriterGeneration()));
        return new FormatMergeResult(format, result.getMergedWriterFileSetForDataformat(format), result.rowIdMapping().orElse(null));
    }

    private static MergeResult toMergeResult(List<FormatMergeResult> results, RowIdMapping mapping) {
        Map<DataFormat, WriterFileSet> merged = new HashMap<>();
        for (FormatMergeResult r : results) {
            merged.put(r.format(), r.mergedFiles());
        }
        return new MergeResult(merged, mapping);
    }
}
