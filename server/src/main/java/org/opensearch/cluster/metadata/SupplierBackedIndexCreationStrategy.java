/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;

/**
 * Phase D2 of {@code core-pluggability-refactor-plan.md}: a generic {@link IndexCreationStrategy} adapter
 * delegating to {@link DescriptorOnlyCreation}'s existing static registry, mirroring {@code
 * SupplierBackedIndexMetadataResolver}/{@code SupplierBackedIndexRoutingResolver} (Phase C5) exactly --
 * both exist so a plugin can expose an already-registered static mechanism through a newer SPI without
 * writing bespoke adapter logic of its own, or changing what the underlying mechanism actually decides.
 *
 * <p><b>Behaviorally identical to the check every existing call site performs today</b> ({@code
 * DescriptorOnlyCreation.isRegistered() && DescriptorOnlyCreation.namesAServerlessIndex(name)}), not a new
 * decision -- migrating a call site from that direct check to {@link IndexCreationStrategyRegistry#claims}
 * (which resolves to this adapter, once a plugin returns it from {@code getIndexCreationStrategy()}) is
 * therefore a change in <em>how</em> the same answer is discovered, not <em>what</em> the answer is. Safe
 * to install unconditionally, including before {@link DescriptorOnlyCreation#register} has ever run
 * (nothing gated yet) or on a node with descriptor gating disabled entirely (nothing ever registers) --
 * both cases already resolve to {@code false} today, and continue to.
 */
public final class SupplierBackedIndexCreationStrategy implements IndexCreationStrategy {

    @Override
    public boolean claims(String indexName, CreateIndexClusterStateUpdateRequest request) {
        return DescriptorOnlyCreation.isRegistered() && DescriptorOnlyCreation.namesAServerlessIndex(indexName);
    }
}
