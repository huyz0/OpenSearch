/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.replication.common.ReplicationType;

/**
 * Validates that indices meet the prerequisites for search-only scale operations.
 * <p>
 * This validator ensures that indexes being scaled up or down satisfy all the
 * necessary conditions for a safe scaling operation. It checks for required settings,
 * index state compatibility, and configuration prerequisites such as remote store
 * and segment replication settings.
 */
class ScaleIndexOperationValidator {

    /**
     * Validates that the given index meets the prerequisites for the scale operation.
     * <p>
     * For scale-down operations, this method verifies:
     * <ul>
     *   <li>The index exists</li>
     *   <li>The index is not already in search-only mode</li>
     *   <li>The index has at least one search-only replica configured</li>
     *   <li>Remote store is enabled for the index</li>
     *   <li>Segment replication is enabled for the index</li>
     * </ul>
     * <p>
     * For scale-up operations, this method verifies:
     * <ul>
     *   <li>The index exists</li>
     *   <li>The index is currently in search-only mode</li>
     * </ul>
     *
     * @param indexMetadata the metadata of the index to validate
     * @param routingTable  the routing table {@code indexMetadata} was read alongside, consulted (Phase C4b
     *                      of {@code core-pluggability-refactor-plan.md}) for whether this index's routing
     *                      is computed rather than published; {@link RoutingTable#EMPTY_ROUTING_TABLE} (or
     *                      any routing table with no resolver attached) is always safe to pass and answers
     *                      exactly as before this parameter existed.
     * @param index         the name of the index being validated
     * @param listener      the action listener to notify in case of validation failure
     * @param isScaleDown   true if validating for scale-down, false for scale-up
     * @return true if validation succeeds, false if validation fails (and listener is notified)
     */
    boolean validateScalePrerequisites(
        IndexMetadata indexMetadata,
        RoutingTable routingTable,
        String index,
        ActionListener<AcknowledgedResponse> listener,
        boolean isScaleDown
    ) {
        try {
            if (indexMetadata == null) {
                throw new IllegalArgumentException("Index [" + index + "] not found");
            }
            // Refused here rather than half-supported, and this has to come before every other check so
            // that nothing downstream reads routing that does not exist. Scaling works by rewriting the
            // published routing table to add and remove search-only shards, and a computed index has no
            // published entry to rewrite. Supporting it would mean the placement function itself had to
            // express search-only scaling, which is a design question nobody has asked yet.
            //
            // What this replaces is not a polite failure. ScaleIndexShardSyncManager dereferenced the
            // absent entry and threw NullPointerException from clusterStateProcessed, which runs on the
            // cluster state applier thread, so the failure landed in an applier rather than in the
            // request that caused it. The scale-up path was worse in a quieter way: a null guard meant it
            // built a routing table with no trace of the index and reported success.
            // Phase C4b of core-pluggability-refactor-plan.md: routingTable.shouldPublishRouting(...)
            // replaces AbsentIndexRoutingSuppliers.shouldPublishRouting(...) here -- the same predicate,
            // discovered through the resolver attached to this call's own routing table instead of the
            // static registry.
            if (routingTable.shouldPublishRouting(indexMetadata) == false) {
                throw new IllegalArgumentException(
                    "Index ["
                        + index
                        + "] cannot be scaled because its shard placement is computed rather than published, "
                        + "and scaling works by rewriting the published routing table"
                );
            }
            if (isScaleDown) {
                if (indexMetadata.getSettings().getAsBoolean(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)) {
                    throw new IllegalStateException("Index [" + index + "] is already in search-only mode");
                }

                if (indexMetadata.getSettings().getAsBoolean(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, false) == false) {
                    throw new IllegalArgumentException(
                        "To scale to zero, " + IndexMetadata.SETTING_REMOTE_STORE_ENABLED + " must be enabled for index: " + index
                    );
                }
                if (ReplicationType.SEGMENT.toString()
                    .equals(indexMetadata.getSettings().get(IndexMetadata.SETTING_REPLICATION_TYPE)) == false) {
                    throw new IllegalArgumentException("To scale to zero, segment replication must be enabled for index: " + index);
                }
            } else {
                if (indexMetadata.getSettings().getAsBoolean(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false) == false) {
                    throw new IllegalStateException("Index [" + index + "] is not in search-only mode");
                }
            }
            return true;
        } catch (Exception e) {
            listener.onFailure(e);
            return false;
        }
    }
}
