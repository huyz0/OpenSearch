/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.coordination.ClusterStatePublisher;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.core.action.ActionListener;

/**
 * Applies a cluster state locally and completes. There are no peers to publish to.
 *
 * <p>In classic OpenSearch this role belongs to {@code Coordinator}, which publishes a consensus-agreed
 * state to every node. Here each node computes its own view from object-store truth, so "publish" means
 * "apply to myself". See {@code rfc-serverless-shell.md} §5.
 */
public final class LocalOnlyPublisher implements ClusterStatePublisher {

    private final ClusterApplierService applierService;

    /**
     * Creates a publisher that applies to the given local applier.
     *
     * @param applierService the node's own applier
     */
    public LocalOnlyPublisher(ClusterApplierService applierService) {
        this.applierService = applierService;
    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {
        applierService.onNewClusterState("serverless-local-publish", clusterChangedEvent::state, new ClusterApplier.ClusterApplyListener() {
            @Override
            public void onSuccess(String source) {
                publishListener.onResponse(null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                publishListener.onFailure(e);
            }
        });
    }
}
