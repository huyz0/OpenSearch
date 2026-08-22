/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.IndexCreationStrategy;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;

/**
 * Phase D2 of {@code core-pluggability-refactor-plan.md}: a generic {@link IndexCreationStrategy} adapter
 * delegating to {@link DescriptorOnlyCreation}'s existing static registry, mirroring {@code
 * SupplierBackedIndexMetadataResolver}/{@code SupplierBackedIndexRoutingResolver} (Phase C5) exactly --
 * both exist so a plugin can expose an already-registered static mechanism through a newer SPI without
 * writing bespoke adapter logic of its own, or changing what the underlying mechanism actually decides.
 *
 * <p><b>Behaviorally identical to the check every existing call site performs today</b> ({@code
 * DescriptorOnlyCreation.isRegistered() && DescriptorOnlyCreation.namesAServerlessIndex(name)}), not a new
 * decision -- migrating a call site from that direct check to {@code IndexCreationStrategyRegistry#claims}
 * (which resolves to this adapter, once a plugin returns it from {@code getIndexCreationStrategy()}) is
 * therefore a change in <em>how</em> the same answer is discovered, not <em>what</em> the answer is. Safe
 * to install unconditionally, including before {@link DescriptorOnlyCreation#register} has ever run
 * (nothing gated yet) or on a node with descriptor gating disabled entirely (nothing ever registers) --
 * both cases already resolve to {@code false} today, and continue to.
 *
 * <p><b>Extended for D2's final two call sites</b> ({@code MetadataCreateIndexService#clusterStateCreateIndex}'s
 * {@code skipsClusterState} branch and its two-plane-collision refusal): {@link #claims(String)} (not the
 * two-arg overload, which now defaults to it) carries the actual name-only check that used to live directly
 * on the two-arg method -- nothing about the check itself changed, only which method expresses it, matching
 * this whole adapter's own "same answer, second path" contract. {@link #skipsClusterState} and {@link
 * #describeClaimedNamespace()} delegate to {@link DescriptorOnlyCreation}'s existing gate and namespace
 * constant the same way.
 *
 * <p><b>Phase D3: relocated from {@code server/src/main} into this plugin</b>, alongside {@link
 * DescriptorOnlyCreation} itself -- once that class was plugin-resident, the "bridge to a core-resident
 * class" framing this adapter's own name and javadoc describe no longer applied literally (both classes are
 * now in the same module), but the shape was kept unchanged rather than redesigned: same two classes, same
 * delegation, only the package moved.
 */
public final class SupplierBackedIndexCreationStrategy implements IndexCreationStrategy {

    @Override
    public boolean claims(String indexName) {
        return DescriptorOnlyCreation.isRegistered() && DescriptorOnlyCreation.namesAServerlessIndex(indexName);
    }

    @Override
    public boolean skipsClusterState(IndexMetadata indexMetadata) {
        return DescriptorOnlyCreation.skipsClusterState(indexMetadata);
    }

    @Override
    public String describeClaimedNamespace() {
        return "the serverless namespace [" + DescriptorOnlyCreation.SERVERLESS_NAME_PREFIX + "]";
    }

    /**
     * Phase J3 of {@code core-pluggability-refactor-plan.md}: the 50-target cap on multi-index open/close,
     * which {@code MetadataIndexStateService} used to hardcode along with a message naming Object Storage
     * as the reason. Both belong here: a gated index's open/close is a separate object-store operation, so
     * the cost this bounds is this plugin's storage model, not a property of multi-index requests in
     * general. Core's default is no cap.
     */
    @Override
    public int maxMultiIndexStateChangeTargets() {
        return 50;
    }

    /**
     * The per-index opt-in switch is how this plugin marks an index as belonging to its plane;
     * declaring it here lets descriptor synthesis in core round-trip the marker without core
     * hard-coding this plugin's setting name.
     */
    @Override
    public String claimedIndexSettingKey() {
        return ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey();
    }
}
