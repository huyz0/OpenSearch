/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

/** How many a delete-by-query operation visited and how many it actually removed. */
public final class DeleteByQueryOutcome {

    private final long matched;
    private final long deleted;
    private final int batches;
    private final long versionConflicts;
    private final String abortedOn;
    private final org.opensearch.index.reindex.BulkByScrollResponse response;
    private final org.opensearch.index.engine.VersionConflictEngineException conflict;

    DeleteByQueryOutcome(long matched, long deleted) {
        this(matched, deleted, 0L, null);
    }

    DeleteByQueryOutcome(long matched, long deleted, long versionConflicts, String abortedOn) {
        this(matched, deleted, versionConflicts, abortedOn, null, null, matched == 0 ? 0 : 1);
    }

    DeleteByQueryOutcome(
        long matched,
        long deleted,
        long versionConflicts,
        String abortedOn,
        org.opensearch.index.reindex.BulkByScrollResponse response,
        org.opensearch.index.engine.VersionConflictEngineException conflict,
        int batches
    ) {
        this.batches = batches;
        this.response = response;
        this.conflict = conflict;
        this.versionConflicts = versionConflicts;
        this.abortedOn = abortedOn;
        this.matched = matched;
        this.deleted = deleted;
    }

    /** Returns the native result as the action filters left it.
     * @return the response, including failures, timing and counters
     */
    public org.opensearch.index.reindex.BulkByScrollResponse response() {
        return response;
    }

    DeleteByQueryOutcome withResponse(String index, long nanos) {
        final var status = new org.opensearch.index.reindex.BulkByScrollTask.Status(
            null,
            matched,
            0L,
            0L,
            deleted,
            batches,
            versionConflicts,
            0L,
            0L,
            0L,
            org.opensearch.common.unit.TimeValue.ZERO,
            Float.POSITIVE_INFINITY,
            null,
            org.opensearch.common.unit.TimeValue.ZERO
        );
        final java.util.List<org.opensearch.action.bulk.BulkItemResponse.Failure> failures = conflict == null
            ? java.util.List.of()
            : java.util.List.of(new org.opensearch.action.bulk.BulkItemResponse.Failure(index, abortedOn, conflict));
        final var nativeResponse = new org.opensearch.index.reindex.BulkByScrollResponse(
            org.opensearch.common.unit.TimeValue.timeValueNanos(nanos),
            status,
            failures,
            java.util.List.of(),
            false
        );
        return new DeleteByQueryOutcome(matched, deleted, versionConflicts, abortedOn, nativeResponse, conflict, batches);
    }

    /**
     * Returns how many documents the frozen view matched.
     *
     * @return the count
     */
    public long matched() {
        return response == null ? matched : response.getTotal();
    }

    /**
     * Returns how many matched documents had changed since the view was taken and were left alone.
     *
     * @return the count
     */
    public long versionConflicts() {
        return response == null ? versionConflicts : response.getVersionConflicts();
    }

    /**
     * Returns the id of the conflict the operation stopped at, or null if it did not stop.
     *
     * @return the id, or null
     */
    public String abortedOn() {
        if (response == null) {
            return abortedOn;
        }
        for (var failure : response.getBulkFailures()) {
            if (failure.getStatus() == org.opensearch.core.rest.RestStatus.CONFLICT) {
                return failure.getId();
            }
        }
        return null;
    }

    /**
     * Returns how many were actually removed.
     *
     * <p>Can differ from {@link #matched()}: a document deleted between the query freezing and this
     * operation reaching it (by an unrelated, ordinary delete) was already gone, and removing something
     * already gone is not counted twice.
     *
     * @return the count
     */
    public long deleted() {
        return response == null ? deleted : response.getDeleted();
    }
}
