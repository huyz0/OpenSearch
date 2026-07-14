/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;

/**
 * Resolves rfc-serverless-opensearch.md &sect;7.1.1's own open policy question ("operator-triggered
 * legacy snapshot to a non-serverless repository ... if ever allowed on a serverless index, which
 * is a separate open question"): never allowed, for any serverless-storage-enabled index, regardless
 * of which repository the snapshot request targets.
 *
 * <p><b>Why reject outright, not merely leave unsupported and see what happens</b>: classic
 * snapshot/restore ({@code SnapshotShardsService}/{@code IndexShard#snapshot}) is built entirely on
 * {@code CombinedDeletionPolicy#acquireIndexCommit} pinning a real local Lucene commit long enough to
 * copy its files -- an assumption this plugin's own shard model breaks in more than one place: reader
 * shards materialize segments lazily (some may have no complete local {@code Directory} at all, see
 * {@code LazyBundleDirectory}), and both writer and reader shards can be fully suspended (zero local
 * footprint, &sect;7.3) with nothing to pin in the first place. Even on a shard that happens to have
 * a complete local commit right now, honoring the request would silently duplicate this plugin's own
 * dedicated, object-store-native, credential-scoped PITR/clone mechanism (&sect;6.5/&sect;14) with a
 * second, entirely ungoverned backup path that bypasses every one of this plugin's tiered
 * credential-scoping guarantees (&sect;15). This is a coherence/security decision, not merely "not
 * yet implemented" -- there is no shard state in which honoring it would be both correct and
 * consistent with the rest of this plugin's model, so this rejects unconditionally rather than
 * trying to detect "safe" cases.
 *
 * <p>Deliberately index-granular and pre-emptive, mirroring {@code ShardReactivationActionFilter}'s
 * own shape: {@link CreateSnapshotAction} has not yet resolved which shards it will touch by the time
 * an {@link ActionFilter} sees it, so this rejects the whole request the moment <em>any</em> index it
 * targets (after wildcard/alias resolution via {@link IndexNameExpressionResolver#concreteIndexNames}
 * -- a literal name-only check would miss a bare {@code "*"} or an alias, letting an operator route
 * around this check trivially) has {@code serverless_storage.enabled=true}, before any shard-level
 * snapshot work starts. Resolution failures (e.g. a genuinely nonexistent index/alias in the
 * request) are deliberately swallowed here and left for the real transport action to report -- this
 * filter's own job is only to veto serverless-storage indices, not to duplicate index-resolution
 * error reporting.
 *
 * <p>Scope note: only {@link CreateSnapshotAction} is intercepted. Restoring a legacy snapshot
 * <em>into</em> a serverless-storage index is a related but distinct question this filter does not
 * address -- restoring classic per-shard Lucene commit data into an index whose engine expects
 * object-store-native manifests is a separate, not-yet-analyzed scenario, deliberately left out of
 * this pass's scope rather than bundled in without its own dedicated analysis.
 */
public final class ServerlessStorageLegacySnapshotActionFilter implements ActionFilter {

    private volatile ClusterService clusterService;
    private volatile IndexNameExpressionResolver indexNameExpressionResolver;

    /** Creates the filter with no dependencies yet -- see {@link #setDependencies}. */
    public ServerlessStorageLegacySnapshotActionFilter() {}

    /**
     * Supplies the dependencies this filter needs, once available -- {@link
     * org.opensearch.plugins.ActionPlugin#getActionFilters()} is called before {@code
     * createComponents} runs, the same "instantiate early, wire in late" shape {@code
     * ShardReactivationActionFilter} already uses.
     *
     * @param clusterService used to read each targeted index's real settings.
     * @param indexNameExpressionResolver resolves the request's (possibly wildcarded/aliased) index
     *                                     expressions to concrete index names.
     */
    public void setDependencies(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE; // run first: cheapest possible check, veto before any real snapshot work starts.
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        ClusterService currentClusterService = this.clusterService;
        IndexNameExpressionResolver currentResolver = this.indexNameExpressionResolver;
        if (currentClusterService == null
            || currentResolver == null
            || CreateSnapshotAction.NAME.equals(action) == false
            || (request instanceof IndicesRequest) == false) {
            chain.proceed(task, action, request, listener);
            return;
        }

        ClusterState state = currentClusterService.state();
        String[] concreteIndexNames;
        try {
            concreteIndexNames = currentResolver.concreteIndexNames(state, (IndicesRequest) request);
        } catch (RuntimeException indexResolutionFailure) {
            // Not this filter's job to report -- let the real transport action re-resolve and
            // surface whatever the actual problem is (e.g. no matching index/alias).
            chain.proceed(task, action, request, listener);
            return;
        }

        for (String indexName : concreteIndexNames) {
            IndexMetadata indexMetadata = state.metadata().index(indexName);
            if (indexMetadata == null) {
                continue;
            }
            if (ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexMetadata.getSettings())) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "legacy snapshot is not supported for serverless-storage-enabled index ["
                            + indexMetadata.getIndex().getName()
                            + "] -- use this plugin's own point-in-time recovery / zero-copy clone mechanism "
                            + "instead (rfc-serverless-opensearch.md sections 6.5 and 14)"
                    )
                );
                return;
            }
        }
        chain.proceed(task, action, request, listener);
    }
}
