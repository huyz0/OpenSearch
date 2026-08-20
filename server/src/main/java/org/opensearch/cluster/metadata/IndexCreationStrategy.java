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
 * names/requests belong to it, replacing the fork's name-prefix convention (previously {@code
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
 *       MetadataDeleteIndexService} never consults {@code DescriptorOnlyCreation} at all (gated deletion is
 *       decided entirely by Phase C's {@code Metadata#indexOrResolved} null-ness signal), and the actual
 *       tombstone write is handed off through {@code DurableTombstones}'s own {@code register(Writer)}
 *       static hook -- a working, already-appropriately-scoped extension point this interface would have
 *       duplicated, not replaced.
 *   <li>Creation mechanics are the same story: {@code MetadataCreateIndexService#createGatedIndex}'s actual
 *       body (off-cluster-manager-thread admission, {@code applyCreateIndexRequest} validation, {@code
 *       op_type=create} atomicity reasoning, GENERIC-threadpool dispatch to avoid blocking a transport
 *       worker) hands the low-level descriptor write off through {@code IndexDescriptorPublisher}'s own
 *       {@code registerCreator}/{@code registerUpdater} static hooks -- again already generic, already
 *       working, and never touched by {@code DescriptorOnlyCreation} or by anything this interface would
 *       have added.
 * </ul>
 *
 * <p>What {@code DescriptorOnlyCreation} actually is, once separated from those two already-solved
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
 * <p><b>Extended to close out D2's own two remaining {@code clusterStateCreateIndex} call sites</b> (the
 * ones the D2-scoping status-log row identified as the genuinely dangerous, nine-line region and deliberately
 * left alone during the first two D2 slices). Reading those two sites found they ask two questions {@link
 * #claims(String, CreateIndexClusterStateUpdateRequest)} cannot answer as originally shaped:
 *
 * <ul>
 *   <li>{@code skipsClusterState}'s gate consults a <em>finished</em> {@link IndexMetadata}, not a
 *       pre-build {@link CreateIndexClusterStateUpdateRequest} -- and is a strictly narrower question than
 *       {@code claims()} (the real gate additionally checks placement ownership and descriptor
 *       representability, both unknowable before the index is built). Three shapes were considered for
 *       this: (a) broaden {@code claims()} itself to accept {@link IndexMetadata}, rejected because it
 *       would conflate a pre-admission decision with a post-build one that can legitimately disagree with
 *       it; (b) leave the underlying static predicate where it is and only rename it out of core's
 *       vocabulary, rejected because it keeps a second, parallel plugin-registration point alive for no
 *       reason once one already exists; (c) add a dedicated, symmetric {@link #skipsClusterState} method
 *       to this same interface, the same "narrow, purpose-built method" shape {@link #claims} itself was
 *       narrowed to during D1 -- chosen, since it keeps exactly one registration point per plugin while
 *       being honest that the two questions are genuinely different.
 *   <li>The two-plane-collision refusal has no {@link CreateIndexClusterStateUpdateRequest} in scope at
 *       all -- only a name. Three shapes were considered: (a) pass a fabricated {@code null} request into
 *       the existing two-arg {@link #claims}, rejected because a future implementation that legitimately
 *       reads the request would silently NPE, and the plan's own D2 text already flagged a fabricated null
 *       as the wrong answer; (b) a distinctly-named method (e.g. {@code reservesNamespace}) kept
 *       independent of {@code claims()}, rejected as speculative -- nothing today needs the two concepts to
 *       ever disagree, and the one real implementation would answer both identically; (c) a name-only
 *       {@link #claims(String)} overload, with the richer, request-aware overload defaulting to it -- chosen,
 *       because every real implementation of this interface today (see {@code SupplierBackedIndexCreationStrategy},
 *       a plugin-owned implementation since Phase D3 relocated it out of {@code server/})
 *       already ignores the request entirely, so the name-only question is the actually-primitive one and
 *       the request-aware overload is the derived convenience, not the other way around.
 * </ul>
 *
 * <p>A third, smaller gap the same two call sites' error messages exposed: both interpolate {@code
 * DescriptorOnlyCreation.SERVERLESS_NAME_PREFIX} directly into user-facing text, which is exactly the kind
 * of core-names-the-plugin's-vocabulary-by-name coupling this whole plan exists to remove, just in a
 * message string rather than control flow. {@link #describeClaimedNamespace()} lets the registered strategy
 * supply its own description instead.
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
     *
     * <p>Defaults to {@link #claims(String)} -- every real implementation of this interface answers this
     * question from the name alone (see this type's own javadoc for why that overload, not this one, is
     * the primitive a plugin should implement). Override this directly only if a claim genuinely depends on
     * request content a name alone can't express.
     */
    default boolean claims(String indexName, CreateIndexClusterStateUpdateRequest request) {
        return claims(indexName);
    }

    /**
     * Whether this strategy claims the given index name, with no request in scope. Used at call sites that
     * only ever have a name (or a finished {@link IndexMetadata}, which carries one) to consult, not the
     * original creation request -- e.g. {@code MetadataCreateIndexService}'s two-plane-collision refusal,
     * which runs after the index has already been built.
     *
     * <p>Defaults to {@code false}, matching this interface's overall "unregistered/unclaimed changes
     * nothing" shape: a node without a plugin overriding this sees every name as unclaimed.
     */
    default boolean claims(String indexName) {
        return false;
    }

    /**
     * Whether a finished {@link IndexMetadata} should skip its cluster-state entry entirely -- a strictly
     * narrower, post-build question than {@link #claims}, since a name can be admitted to this strategy's
     * plane in general (worth taking off the ordinary road) yet still fail this check for a reason only
     * knowable once the index is fully built (e.g. it isn't representable the way this strategy's
     * off-cluster-state storage requires). {@code MetadataCreateIndexService#clusterStateCreateIndex}
     * consults this, not {@link #claims}, to decide whether to write a cluster-state entry at all.
     *
     * <p>Defaults to {@code false}: an unclaimed or unregistered index always keeps its cluster-state
     * entry, exactly today's behavior for a node without this strategy installed.
     */
    default boolean skipsClusterState(IndexMetadata indexMetadata) {
        return false;
    }

    /**
     * A human-readable description of the namespace/condition this strategy claims, for use in
     * user-facing error text (e.g. "the index cannot have a cluster state entry because it is in
     * &lt;this&gt;"). Only ever read after {@link #claims} has already answered {@code true}, so a plugin
     * whose claim is unconditional can return a fixed string; a plugin whose claim depends on more than the
     * name can still describe it generically here.
     *
     * <p>Defaults to a generic description that names no product/plugin concept, matching this interface's
     * own core-side vocabulary.
     */
    default String describeClaimedNamespace() {
        return "a namespace claimed by a registered index-creation strategy";
    }

    /**
     * The most claimed indices a single multi-index state change (open/close) may target, or {@link
     * Integer#MAX_VALUE} for no limit.
     *
     * <p>Phase J3 of {@code core-pluggability-refactor-plan.md}. {@code MetadataIndexStateService} used to
     * hardcode {@code 50} here, in a message that additionally named the specific storage technology whose
     * per-operation cost motivated the number ("to prevent high-cost Object Storage operations"). Both the
     * limit and the reason for it are properties of a particular strategy's storage model, not of core:
     * a strategy whose backing store makes bulk state changes cheap has no reason to be capped at all,
     * which is why the default is "no limit" rather than 50.
     *
     * <p>Defaults to {@link Integer#MAX_VALUE}, so a node with no strategy registered -- or one whose
     * strategy does not override this -- applies no cap, exactly the behavior of any index that isn't
     * claimed in the first place.
     */
    default int maxMultiIndexStateChangeTargets() {
        return Integer.MAX_VALUE;
    }
}
