/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidateEntry;
import org.opensearch.transport.client.Client;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The "do the work" half of scale-up (see the RFC's scale-up autoscaling subsection), consuming
 * {@link ScaleUpCandidateEntry} the same way {@code
 * org.opensearch.serverless.storage.scaletozero.ShardSuspensionCoordinator} consumes {@code
 * ScaleToZeroCandidateEntry} -- this class makes no eligibility decision of its own, it only acts
 * on a candidate already flagged eligible.
 *
 * <p>Unlike {@code ShardSuspensionCoordinator}, which mutates {@link IndexMetadata} custom data
 * directly via a {@link org.opensearch.cluster.ClusterStateUpdateTask}, replica count is ordinary
 * index configuration ({@link IndexMetadata#SETTING_NUMBER_OF_SEARCH_REPLICAS}, exposed as {@link
 * IndexMetadata#getNumberOfSearchOnlyReplicas()}), so there is no dedicated builder setter for it
 * -- core only ever derives it from settings during {@code IndexMetadata.Builder#build()}. Bumping
 * it is therefore a plain {@link UpdateSettingsRequest}, the same sanctioned path an operator's own
 * {@code PUT /index/_settings} would use, rather than a bespoke cluster-state mutation.
 *
 * <p>Deliberately per-index, not per-shard: {@code index.number_of_search_replicas} is an
 * index-wide setting, so one busy shard's candidacy expands every shard's search-replica count for
 * that index together -- {@link #expandCandidates} dedupes multiple candidate shards of the same
 * index down to a single settings update.
 */
public final class ReaderReplicaExpansionCoordinator {

    private static final Logger logger = LogManager.getLogger(ReaderReplicaExpansionCoordinator.class);

    private final Client client;
    private final int maxSearchReplicas;

    /**
     * Creates a coordinator.
     *
     * @param client dispatches the {@link UpdateSettingsRequest} that actually expands the index.
     * @param maxSearchReplicas the cap {@link #expandCandidates} never bumps a candidate index past,
     *                          even if called repeatedly -- normally already enforced by {@link
     *                          ScaleUpCandidateEntry#candidate()} itself, this is a second,
     *                          coordinator-local guard against acting on a stale entry.
     */
    public ReaderReplicaExpansionCoordinator(Client client, int maxSearchReplicas) {
        this.client = client;
        this.maxSearchReplicas = maxSearchReplicas;
    }

    /**
     * Bumps every distinct {@link ScaleUpCandidateEntry#candidate()} index's {@code
     * index.number_of_search_replicas} by one, capped at {@link #maxSearchReplicas} -- idempotent
     * in the sense that calling this on every scheduled evaluation tick is safe, since each call
     * only ever bumps by one step past whatever {@link ScaleUpCandidateEntry#currentSearchReplicaCount()}
     * the evaluation itself observed.
     *
     * @param candidates one evaluation's worth of scale-up candidates; only entries with {@link
     *                   ScaleUpCandidateEntry#candidate()} {@code true} are acted on.
     */
    public void expandCandidates(List<ScaleUpCandidateEntry> candidates) {
        Set<String> alreadyExpanded = new HashSet<>();
        for (ScaleUpCandidateEntry entry : candidates) {
            if (entry.candidate() == false) {
                continue;
            }
            if (alreadyExpanded.add(entry.indexName()) == false) {
                continue; // another shard of the same index already triggered this index's expansion this tick.
            }
            expandIndex(entry.indexName(), entry.currentSearchReplicaCount());
        }
    }

    private void expandIndex(String indexName, int currentSearchReplicaCount) {
        int target = Math.min(currentSearchReplicaCount + 1, maxSearchReplicas);
        if (target <= currentSearchReplicaCount) {
            return; // already at (or somehow past) the cap -- nothing to do.
        }
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
        request.settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, target).build());
        client.admin()
            .indices()
            .updateSettings(
                request,
                ActionListener.wrap(
                    response -> logger.info("expanded serverless-storage reader index [{}] to {} search replica(s)", indexName, target),
                    e -> logger.warn("failed to expand serverless-storage reader index [" + indexName + "]", e)
                )
            );
    }
}
