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
 * Phase D1 of {@code core-pluggability-refactor-plan.md}: an SPI a plugin implements to decide which index
 * names/requests belong to it, replacing the fork's name-prefix convention (previously {@link
 * DescriptorOnlyCreation}'s {@code namesAServerlessIndex}-style check) with an explicit, plugin-owned
 * decision.
 *
 * <p><b>Definition only, not yet consulted anywhere.</b> Pure additive API surface -- a new type plus a new
 * default-empty {@link org.opensearch.plugins.ClusterPlugin} hook -- with the exact same "zero behavior
 * change until something implements it" shape Phase C1's {@link IndexMetadataResolver}/{@link
 * org.opensearch.cluster.routing.IndexRoutingResolver} had.
 *
 * <p><b>Revised down to a single method after a dedicated D2 scoping investigation, correcting this
 * interface's own first draft.</b> That draft additionally declared {@code createIndex(ClusterState,
 * CreateIndexClusterStateUpdateRequest)} and {@code claimsForDeletion}/{@code deleteIndex}, modeled on the
 * assumption that a plugin needs to take over creation and deletion *orchestration* entirely, the way the
 * plan's own D2 sketch described it. Reading the real call sites before migrating any of them (see the "D2
 * scoping investigation" status-log row) found that assumption wrong on both counts:
 *
 * <ul>
 *   <li>Deletion mechanics are already generic, plugin-independent infrastructure: {@link
 *       MetadataDeleteIndexService} never consults {@link DescriptorOnlyCreation} at all (gated deletion is
 *       decided entirely by Phase C's {@code Metadata#indexOrResolved} null-ness signal), and the actual
 *       tombstone write is handed off through {@code DurableTombstones}'s own {@code register(Writer)}
 *       static hook -- a working, already-appropriately-scoped extension point this interface would have
 *       duplicated, not replaced.
 *   <li>Creation mechanics are the same story: {@code MetadataCreateIndexService#createGatedIndex}'s actual
 *       body (off-cluster-manager-thread admission, {@code applyCreateIndexRequest} validation, {@code
 *       op_type=create} atomicity reasoning, GENERIC-threadpool dispatch to avoid blocking a transport
 *       worker) hands the low-level descriptor write off through {@code IndexDescriptorPublisher}'s own
 *       {@code registerCreator}/{@code registerUpdater} static hooks -- again already generic, already
 *       working, and never touched by {@link DescriptorOnlyCreation} or by anything this interface would
 *       have added.
 * </ul>
 *
 * <p>What {@link DescriptorOnlyCreation} actually is, once separated from those two already-solved
 * concerns, is narrower than a full creation/deletion strategy: a single gating <em>decision</em>, "does
 * this index name/request belong to the gated plane," hardcoded today as a string-prefix check. That
 * decision is the one piece with no existing generic extension point -- and the one thing left for this
 * interface to model. Keeping the wider, orchestration-shaped interface this file started with would have
 * meant either re-deriving {@code createGatedIndex}'s hard-won concurrency reasoning generically (real risk
 * of silently dropping one of the several previously-fixed bugs its own comments document) or leaving a
 * {@code createIndex()} method on the interface that no real migration would ever call -- dead API surface
 * mimicking a capability nothing needs. Narrowing to {@link #claims} instead means a real D2 migration only
 * ever replaces the *predicate* deciding whether {@code createGatedIndex}/{@code skipsClusterState}'s
 * existing, untouched bodies run -- not those bodies themselves.
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
 * doesn't touch -- would be a bigger and wrong-owner change than "define an empty extension point" should
 * require. This mirrors {@code ClusterPlugin}'s own existing convention: its experimental hooks (e.g. {@code
 * getIndexMetadataResolver()}) are documented experimental via {@code @opensearch.experimental} javadoc
 * only, without a compiled type-level annotation on {@code ClusterPlugin} itself.
 *
 * @opensearch.experimental
 */
public interface IndexCreationStrategy {

    /**
     * Whether this strategy claims the given index name/request -- e.g. a naming convention or a request
     * attribute a plugin recognizes. When this answers {@code true}, the index belongs to this strategy's
     * plane: {@code MetadataCreateIndexService}'s creation, deletion, and shard-lifecycle handling for it
     * follow whatever generic hooks that behavior already runs through ({@code IndexDescriptorPublisher},
     * {@code DurableTombstones}, {@code Metadata#indexOrResolved}) rather than the ordinary cluster-state
     * -entry path.
     */
    boolean claims(String indexName, CreateIndexClusterStateUpdateRequest request);
}
