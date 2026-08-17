/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.core.index.Index;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Phase D1 of {@code core-pluggability-refactor-plan.md}: an SPI a plugin implements to take over index
 * creation, deletion, opening, and closing entirely for the indices it claims, replacing the fork's
 * name-prefix convention (previously {@link DescriptorOnlyCreation}'s {@code namesAServerlessIndex}-style
 * check) with an explicit, plugin-owned decision.
 *
 * <p><b>Definition only, not yet consulted anywhere.</b> This phase is deliberately split: defining this
 * interface and its registration point is pure additive API surface -- a new type plus a new
 * default-empty {@link org.opensearch.plugins.ClusterPlugin} hook -- with the exact same "zero behavior
 * change until something implements it" shape Phase C1's {@link IndexMetadataResolver}/{@link
 * org.opensearch.cluster.routing.IndexRoutingResolver} had. Migrating the five core services that would
 * actually consult a registered strategy (Phase D2: {@code MetadataCreateIndexService}, {@code
 * MetadataDeleteIndexService}, {@code MetadataIndexStateService}, {@code MetadataMappingService}, {@code
 * TransportCreateIndexAction}) is a separate, larger, higher-risk piece of work -- see
 * {@code core-pluggability-refactor-plan.md}'s own risk assessment for Phase D2/D3 for why that part is
 * deliberately not attempted in the same pass as this definition.
 *
 * <p>Registered via a new {@code ClusterPlugin.getIndexCreationStrategy()} default-empty hook, the same
 * registration point Phase C1's resolvers use.
 *
 * <p><b>Deliberately not annotated {@code @ExperimentalApi} at the type level.</b> That annotation requires
 * every type this interface exposes as a public member to itself be {@code @PublicApi}/{@code
 * @ExperimentalApi}/{@code @DeprecatedApi} (enforced at compile time) -- but {@link
 * CreateIndexClusterStateUpdateRequest} and its whole {@code ClusterStateUpdateRequest} hierarchy carry
 * none of those, and were never designed as a tracked API-compatibility surface. Retrofitting that
 * annotation onto a core internal request DTO -- unrelated to this SPI, and owned by services this phase
 * doesn't touch -- just to satisfy this interface would be a bigger and wrong-owner change than "define an
 * empty extension point" should require. This mirrors {@code ClusterPlugin}'s own existing convention:
 * its experimental hooks (e.g. {@code getIndexMetadataResolver()}) are documented experimental via
 * {@code @opensearch.experimental} javadoc only, without a compiled type-level annotation on {@code
 * ClusterPlugin} itself. D2's call-site migration is the point where the real parameter shape gets
 * verified against actual callers; that is also the natural point to revisit whether {@code
 * CreateIndexClusterStateUpdateRequest} (or a narrower purpose-built replacement) should carry a formal
 * API-compatibility annotation.
 *
 * @opensearch.experimental
 */
public interface IndexCreationStrategy {

    /**
     * Whether this strategy claims the given index name/request -- e.g. a naming convention or a request
     * attribute a plugin recognizes. When this answers {@code true}, core's normal
     * {@code MetadataCreateIndexService#clusterStateCreateIndex} is never called for the index; this
     * strategy owns creation entirely.
     */
    boolean claims(String indexName, CreateIndexClusterStateUpdateRequest request);

    /**
     * Handles creation entirely for an index this strategy {@link #claims(String,
     * CreateIndexClusterStateUpdateRequest)}. The returned {@link CompletionStage} is what the caller awaits
     * directly, replacing the side-channel field ({@code CreateIndexClusterStateUpdateRequest#descriptorWrite}
     * in the fork's current shape) Phase D2 removes from the shared request DTO.
     */
    CompletionStage<ClusterStateUpdateResponse> createIndex(ClusterState state, CreateIndexClusterStateUpdateRequest request);

    /**
     * Whether this strategy claims the given index for deletion -- the mirror of {@link #claims(String,
     * CreateIndexClusterStateUpdateRequest)} for the delete path, since an index's creation-time name/request
     * shape is not available again at delete time; only the {@link Index} itself is.
     */
    boolean claimsForDeletion(ClusterState state, Index index);

    /** Handles deletion entirely for every index in {@code claimed} that {@link #claimsForDeletion} accepted. */
    CompletionStage<AcknowledgedResponse> deleteIndex(ClusterState state, List<Index> claimed);

    /*
     * Deliberately not yet defined here: open/close mirror hooks (the plan's own D1 sketch left these as a
     * placeholder, "...open/close equivalents", rather than a concrete signature) and
     * canExecuteOffClusterManager() (identified during D2's scoping as needed by
     * MetadataCreateIndexService#certainlyGated()/TransportCreateIndexAction#localExecute(), not part of
     * D1's original sketch). Adding either now, without the D2 call-site work in hand to get the exact
     * shape right, risks guessing wrong the same way earlier phases' own investigations found real gaps in
     * speculative signatures before verifying them against the actual call site. D2 adds them alongside the
     * migration that needs them.
     */
}
