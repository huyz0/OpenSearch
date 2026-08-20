/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.shrink.ResizeType;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.ActiveShardsObserver;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.ack.CreateIndexClusterStateUpdateResponse;
import org.opensearch.cluster.applicationtemplates.SystemTemplatesService;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.ValidationException;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexCreationValidator;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.CompositeIndexValidator;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.remote.RemoteStoreCustomMetadataResolver;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndexCreationException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.InvalidIndexContextException;
import org.opensearch.indices.InvalidIndexNameException;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.indices.SystemIndices;
import org.opensearch.indices.pollingingest.mappers.IngestionMessageMapper;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SEARCH_REPLICAS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_REPLICATION_TYPE_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.cluster.metadata.Metadata.DEFAULT_REPLICA_COUNT_SETTING;
import static org.opensearch.cluster.metadata.MetadataIndexTemplateService.findContextTemplateName;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_REMOTE_CAPABLE_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.cluster.service.ClusterManagerTask.CREATE_INDEX;
import static org.opensearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteDataAttributePresent;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.isMigratingToRemoteStore;

/**
 * Service responsible for submitting create index requests
 *
 * @opensearch.internal
 */
public class MetadataCreateIndexService {
    private static final Logger logger = LogManager.getLogger(MetadataCreateIndexService.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(MetadataCreateIndexService.class);

    public static final int MAX_INDEX_NAME_BYTES = 255;

    private final Settings settings;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final AliasValidator aliasValidator;
    private final Environment env;
    private final IndexScopedSettings indexScopedSettings;
    private final ActiveShardsObserver activeShardsObserver;
    /**
     * Kept as a field, unlike before, because a gated creation has to be admitted somewhere other than the
     * cluster state update thread and this is what dispatches it there.
     */
    private final ThreadPool threadPool;
    private final NamedXContentRegistry xContentRegistry;
    private final SystemIndices systemIndices;
    private final ShardLimitValidator shardLimitValidator;
    private final boolean forbidPrivateIndexSettings;
    private final Set<IndexSettingProvider> indexSettingProviders = new HashSet<>();
    private final List<IndexCreationValidator> indexCreationValidators = new ArrayList<>();
    private final ClusterManagerTaskThrottler.ThrottlingKey createIndexTaskKey;
    private AwarenessReplicaBalance awarenessReplicaBalance;

    @Nullable
    private final RemoteStoreCustomMetadataResolver remoteStoreCustomMetadataResolver;

    public MetadataCreateIndexService(
        final Settings settings,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final AllocationService allocationService,
        final AliasValidator aliasValidator,
        final ShardLimitValidator shardLimitValidator,
        final Environment env,
        final IndexScopedSettings indexScopedSettings,
        final ThreadPool threadPool,
        final NamedXContentRegistry xContentRegistry,
        final SystemIndices systemIndices,
        final boolean forbidPrivateIndexSettings,
        final AwarenessReplicaBalance awarenessReplicaBalance,
        final RemoteStoreSettings remoteStoreSettings,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.aliasValidator = aliasValidator;
        this.env = env;
        this.indexScopedSettings = indexScopedSettings;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        this.systemIndices = systemIndices;
        this.forbidPrivateIndexSettings = forbidPrivateIndexSettings;
        this.shardLimitValidator = shardLimitValidator;
        this.awarenessReplicaBalance = awarenessReplicaBalance;

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        createIndexTaskKey = clusterService.registerClusterManagerTask(CREATE_INDEX, true);
        Supplier<Version> minNodeVersionSupplier = () -> clusterService.state().nodes().getMinNodeVersion();
        remoteStoreCustomMetadataResolver = RemoteStoreNodeAttribute.isSegmentRepoConfigured(settings)
            && RemoteStoreNodeAttribute.isTranslogRepoConfigured(settings)
                ? new RemoteStoreCustomMetadataResolver(remoteStoreSettings, minNodeVersionSupplier, repositoriesServiceSupplier, settings)
                : null;
    }

    public IndexScopedSettings getIndexScopedSettings() {
        return indexScopedSettings;
    }

    /**
     * Add a provider to be invoked to get additional index settings prior to an index being created
     */
    public void addAdditionalIndexSettingProvider(IndexSettingProvider provider) {
        if (provider == null) {
            throw new IllegalArgumentException("provider must not be null");
        }
        if (indexSettingProviders.contains(provider)) {
            throw new IllegalArgumentException("provider already added");
        }
        this.indexSettingProviders.add(provider);
    }

    public void addIndexCreationValidator(IndexCreationValidator validator) {
        if (validator == null) {
            throw new IllegalArgumentException("validator must not be null");
        }
        indexCreationValidators.add(validator);
    }

    /**
     * Validate the name for an index against some static rules and a cluster state.
     */
    public void validateIndexName(String index, ClusterState state) {
        validateIndexOrAliasName(index, InvalidIndexNameException::new);
        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new InvalidIndexNameException(index, "must be lowercase");
        }

        // NOTE: dot-prefixed index names are validated after template application, not here

        if (state.routingTable().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.routingTable().index(index).getIndex());
        }
        if (state.metadata().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.metadata().index(index).getIndex());
        }
        if (state.metadata().hasAlias(index)) {
            throw new InvalidIndexNameException(index, "already exists as alias");
        }
    }

    /**
     * Validates (if this index has a dot-prefixed name) whether it follows the rules for dot-prefixed indices.
     * @param index The name of the index in question
     * @param isHidden Whether or not this is a hidden index
     */
    public boolean validateDotIndex(String index, @Nullable Boolean isHidden) {
        if (index.charAt(0) == '.') {
            if (systemIndices.validateSystemIndex(index)) {
                return true;
            } else if (isHidden) {
                logger.trace("index [{}] is a hidden index", index);
            } else {
                DEPRECATION_LOGGER.deprecate(
                    "index_name_starts_with_dot",
                    "index name [{}] starts with a dot '.', in the next major version, index names "
                        + "starting with a dot are reserved for hidden indices and system indices",
                    index
                );
            }
        }

        return false;
    }

    /**
     * Validate the name for an index or alias against some static rules.
     */
    public static void validateIndexOrAliasName(String index, BiFunction<String, String, ? extends RuntimeException> exceptionCtor) {
        if (Strings.validFileName(index) == false) {
            throw exceptionCtor.apply(index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (index.isEmpty()) {
            throw exceptionCtor.apply(index, "must not be empty");
        }
        if (index.contains("#")) {
            throw exceptionCtor.apply(index, "must not contain '#'");
        }
        if (index.contains(":")) {
            throw exceptionCtor.apply(index, "must not contain ':'");
        }
        if (index.charAt(0) == '_' || index.charAt(0) == '-' || index.charAt(0) == '+') {
            throw exceptionCtor.apply(index, "must not start with '_', '-', or '+'");
        }
        int byteCount = 0;
        try {
            byteCount = index.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new OpenSearchException("Unable to determine length of index name", e);
        }
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            throw exceptionCtor.apply(index, "index name is too long, (" + byteCount + " > " + MAX_INDEX_NAME_BYTES + ")");
        }
        if (index.equals(".") || index.equals("..")) {
            throw exceptionCtor.apply(index, "must not be '.' or '..'");
        }
    }

    /**
     * Creates an index in the cluster state and waits for the specified number of shard copies to
     * become active (as specified in {@link CreateIndexClusterStateUpdateRequest#waitForActiveShards()})
     * before sending the response on the listener. If the index creation was successfully applied on
     * the cluster state, then {@link CreateIndexClusterStateUpdateResponse#isAcknowledged()} will return
     * true, otherwise it will return false and no waiting will occur for started shards
     * ({@link CreateIndexClusterStateUpdateResponse#isShardsAcknowledged()} will also be false).  If the index
     * creation in the cluster state was successful and the requisite shard copies were started before
     * the timeout, then {@link CreateIndexClusterStateUpdateResponse#isShardsAcknowledged()} will
     * return true, otherwise if the operation timed out, then it will return false.
     *
     * @param request the index creation cluster state update request
     * @param listener the listener on which to send the index creation cluster state update response
     */

    public void createIndex(
        final CreateIndexClusterStateUpdateRequest request,
        final ActionListener<CreateIndexClusterStateUpdateResponse> listener
    ) {
        // The road, chosen from the name before any of the work that would once have been needed to choose
        // it. Unregistered answers false, so an ordinary cluster never reaches the branch below.
        //
        // Phase D2 of core-pluggability-refactor-plan.md: IndexCreationStrategyRegistry.claims(...) replaces
        // DescriptorOnlyCreation.isRegistered() && namesAServerlessIndex(...) here -- the same predicate,
        // discovered through the new SPI instead of the static registry directly. createGatedIndex's own
        // body, and everything downstream of this branch, is unchanged.
        if (IndexCreationStrategyRegistry.claims(request.index(), request)) {
            // Exact rather than admitted-on-a-guess: the name says which plane this belongs in, so there is
            // no road to fall back from and no template to resolve first.
            createGatedIndex(request, listener);
            return;
        }
        onlyCreateIndex(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                activeShardsObserver.waitForActiveShards(
                    new String[] { request.index() },
                    request.waitForActiveShards(),
                    request.ackTimeout(),
                    shardsAcknowledged -> {
                        if (shardsAcknowledged == false) {
                            logger.debug(
                                "[{}] index created, but the operation timed out while waiting for " + "enough shards to be started.",
                                request.index()
                            );
                        }
                        listener.onResponse(new CreateIndexClusterStateUpdateResponse(response.isAcknowledged(), shardsAcknowledged));
                    },
                    listener::onFailure
                );
            } else {
                listener.onResponse(new CreateIndexClusterStateUpdateResponse(false, false));
            }
        }, listener::onFailure));
    }

    /**
     * Whether this request will certainly be gated, and can therefore be executed on whatever node received
     * it rather than on the elected cluster manager.
     *
     * <h4>What this used to have to do, and why it no longer does</h4>
     *
     * Gating followed the settings, so this could not know the answer. It resolved the request's templates to
     * find out whether the gating setting would be contributed, resolved them a second time to find out
     * whether a template's aliases would make the finished index unrepresentable, and still only reached
     * <em>probably</em>: the real gate ran later, on finished metadata, and could decline. Declining meant
     * falling back to a cluster state update task, which only the cluster manager can publish, so a request
     * routed away on a maybe was a request whose failure mode was "the fallback cannot run". Hence the list
     * of conditions that each answered "not certain" rather than "not gated".
     *
     * <p>The namespace removes the question. A name in it is gated or the creation fails -- {@code
     * clusterStateCreateIndex} refuses it a cluster state entry, so there is no ordinary road to fall back
     * onto and nothing that needs the cluster manager. The conditions that used to decline here are refused
     * outright by {@link #validateClaimedNamespaceRequest}, and they are refused identically on every node,
     * because the check reads the request rather than the cluster.
     *
     * <h4>The same-name race, which the namespace also settles</h4>
     *
     * A gated creation off the cluster manager sees a cluster state snapshot that can be a publication
     * behind, which used to widen the window for an ordinary index of the same name. There is no such index
     * now: no cluster state entry may bear a name in this namespace, so the only competitor for the name is
     * another gated creation, and those are resolved by the descriptor store's register compare-and-swap
     * rather than by whichever snapshot either node happened to hold.
     */
    public boolean certainlyGated(final CreateIndexClusterStateUpdateRequest request, final ClusterState state) {
        // Phase D2 of core-pluggability-refactor-plan.md: same migration as createIndex()'s own top branch --
        // see that call site's comment.
        return IndexCreationStrategyRegistry.claims(request.index(), request);
    }

    /**
     * Creates a gated index without going through the cluster state update thread.
     *
     * <h4>Why this is a separate road rather than a branch inside the old one</h4>
     *
     * A gated creation writes nothing to cluster state: {@link #clusterStateCreateIndex} returns the state it
     * was given, unchanged. It has done so since gating was turned on, and yet every such creation was still
     * submitted as an URGENT cluster state update task, ran on the single {@code clusterManagerService#
     * updateTask} thread, and did all of its validation there before reaching the branch that decided it had
     * nothing to publish.
     *
     * <p>Profiling put a number on that. Twenty-seven percent of all on-CPU samples across a three node
     * cluster were that one thread, about 3.9 ms of single-threaded CPU per creation, of which the largest
     * part was {@code IndicesService.withTempIndexService} building a whole throwaway {@code IndexService} to
     * validate a mapping that {@link IndexDescriptor#from} then discards. Batching does not help: the batch
     * executor folds tasks one at a time, so it amortises the diff and the publication -- the costs gating had
     * already reduced to zero -- and not the per-index validation, which is what remains. So N concurrent
     * creations were N times 3.9 ms on one thread however they arrived.
     *
     * <p>Externalising the record without externalising the admission left the ceiling exactly where it was.
     * This is the other half.
     *
     * <h4>What still has to be true</h4>
     *
     * Uniqueness does not come from here and never did. H3 put it in the store: {@code op_type=create} is what
     * makes a name unique, which is precisely what makes the cluster manager unnecessary for this. Two nodes
     * admitting the same name concurrently is now possible and is resolved the way it was always designed to
     * be -- one write wins, the loser's future completes false, and the client gets
     * {@link ResourceAlreadyExistsException}.
     *
     * <p>What is needed from cluster state is a <em>read</em>: templates, scoped settings and the name
     * collision check against ordinary indices. A snapshot is enough for that, and every node has one.
     *
     * <h4>When it turns out not to be gated</h4>
     *
     * The admission check reads request settings; the real gate reads finished metadata and can still decline,
     * for instance because {@code DescriptorRepresentable} refuses an index with a filtered alias. That is
     * detected here by the descriptor write never being handed over, and the request falls back to the
     * ordinary path, which redoes the work correctly on the update thread. Paid for twice and correct, rather
     * than fast and wrong.
     */
    private void createGatedIndex(
        final CreateIndexClusterStateUpdateRequest request,
        final ActionListener<CreateIndexClusterStateUpdateResponse> listener
    ) {
        // GENERIC rather than the calling thread, and that is not a preference. TransportCreateIndexAction
        // declares Names.SAME, so clusterManagerOperation runs on a transport worker; doing the descriptor
        // work there is the same defect this branch found in the read path, where a blocking descriptor read
        // on node_t0's only transport worker stalled the connection until it was dropped.
        threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            protected void doRun() {
                final ClusterState snapshot = clusterService.state();
                normalizeRequestSetting(request);
                try {
                    // The state this returns is discarded. For a gated index it is the snapshot unchanged,
                    // and for one that turns out not to be gated it is a state built against a snapshot this
                    // thread has no right to publish -- which is why that case falls back rather than
                    // applying what it just computed.
                    applyCreateIndexRequest(snapshot, request, false);
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }

                final java.util.concurrent.CompletableFuture<Boolean> write = request.descriptorWrite();
                if (write == null) {
                    // Unreachable by construction, and kept because "unreachable by construction" is a claim
                    // this branch has had to withdraw before. applyCreateIndexRequest above either gated the
                    // index -- which is what hands a descriptor write over -- or refused it a cluster state
                    // entry and threw, which the catch has already turned into a failure. Reaching here means
                    // one of those two stopped being true, and the request must fail rather than take a road
                    // that would claim this name in the other plane.
                    listener.onFailure(
                        new IllegalStateException(
                            // Phase J3: namespace description from the registered strategy, not core's
                            // own words for one product's namespace.
                            "index ["
                                + request.index()
                                + "] is in "
                                + IndexCreationStrategyRegistry.describeClaimedNamespace()
                                + " and was neither gated nor refused, which "
                                + "should not be possible; creating it anywhere now would claim the name in "
                                + "both planes"
                        )
                    );
                    return;
                }

                write.whenComplete((created, failure) -> {
                    if (failure != null) {
                        listener.onFailure(unwrapCompletion(failure));
                    } else if (Boolean.TRUE.equals(created) == false) {
                        // H3's uniqueness gate reporting a lost race, which is the same answer an ordinary
                        // duplicate gets.
                        listener.onFailure(new ResourceAlreadyExistsException(request.index()));
                    } else {
                        // Shards are reported acknowledged without consulting the observer, because for a
                        // gated index the observer can only agree: ActiveShardCount#enoughShardsActive finds
                        // no metadata entry for the name and treats that as nothing left to wait for. Asking
                        // it would be a round trip to be told what is already known here.
                        listener.onResponse(new CreateIndexClusterStateUpdateResponse(true, true));
                    }
                });
            }
        });
    }

    /** Strips the wrapper the future stage adds, so the client sees the cause rather than the plumbing. */
    private static Exception unwrapCompletion(Throwable failure) {
        Throwable cause = failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null
            ? failure.getCause()
            : failure;
        return cause instanceof Exception e ? e : new OpenSearchException(cause);
    }

    private void onlyCreateIndex(
        final CreateIndexClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        normalizeRequestSetting(request);
        final CreateIndexTask task = new CreateIndexTask(request, listener);
        clusterService.submitStateUpdateTasks(
            "create-index [" + request.index() + "], cause [" + request.cause() + "]",
            Map.of(task, task),
            ClusterStateTaskConfig.build(Priority.URGENT, request.clusterManagerNodeTimeout()),
            createIndexExecutor
        );
    }

    /**
     * One pending index creation. Serves as both the batch element and its own
     * {@link org.opensearch.cluster.AckedClusterStateTaskListener}, so each request keeps its own
     * acknowledgement timeout and listener even though many are applied in a single cluster-state
     * update.
     *
     * <p>The batch path in {@link #createIndexExecutor} is what actually runs; {@code execute} is
     * implemented to perform the same single-request transition so the task remains correct if it is
     * ever submitted without that executor.
     */
    // Package-private rather than private so tests can drive a genuine multi-task batch; no test
    // path otherwise submits more than one creation per executor invocation.
    class CreateIndexTask extends AckedClusterStateUpdateTask<ClusterStateUpdateResponse> {
        final CreateIndexClusterStateUpdateRequest request;

        CreateIndexTask(CreateIndexClusterStateUpdateRequest request, ActionListener<ClusterStateUpdateResponse> listener) {
            super(Priority.URGENT, request, listener);
            this.request = request;
        }

        @Override
        protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
            return new ClusterStateUpdateResponse(acknowledged);
        }

        @Override
        public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
            return createIndexTaskKey;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            return applyCreateIndexRequest(currentState, request, false);
        }

        @Override
        public void onFailure(String source, Exception e) {
            if (e instanceof ResourceAlreadyExistsException) {
                logger.trace(() -> new ParameterizedMessage("[{}] failed to create", request.index()), e);
            } else {
                logger.debug(() -> new ParameterizedMessage("[{}] failed to create", request.index()), e);
            }
            super.onFailure(source, e);
        }

        /**
         * Answers the client, and for a gated index answers it only once the descriptor write has landed.
         *
         * <p>T18. For an ordinary index the cluster state update is the creation, so acknowledging it is
         * the truth and this defers to the parent unchanged. For a gated index the update deliberately
         * changes nothing and therefore always succeeds, so acknowledging it says only that nothing
         * happened. The descriptor write is the creation, and its outcome is what the client is owed.
         *
         * <p>The cluster state thread has already returned by the time this runs, which is what keeps W4's
         * deadlock closed: nothing waits on the thread that would have to route the write.
         *
         * <p>A completed-false future means a competing creation won the name, which is H3's uniqueness
         * gate reporting rather than an error, so the client gets the same
         * {@link ResourceAlreadyExistsException} an ordinary duplicate would produce.
         */
        @Override
        public void onAllNodesAcked(@Nullable Exception e) {
            java.util.concurrent.CompletableFuture<Boolean> write = request.descriptorWrite();
            if (write == null) {
                super.onAllNodesAcked(e);
                return;
            }
            write.whenComplete((created, failure) -> {
                if (failure != null) {
                    onFailure("create-index [" + request.index() + "] descriptor write", unwrap(failure));
                } else if (Boolean.TRUE.equals(created) == false) {
                    onFailure(
                        "create-index [" + request.index() + "] descriptor write",
                        new ResourceAlreadyExistsException(request.index())
                    );
                } else {
                    super.onAllNodesAcked(e);
                }
            });
        }

        /**
         * The same deferral for the timeout path, since a gated creation that times out its acknowledgement
         * has still not been told whether its descriptor landed.
         */
        @Override
        public void onAckTimeout() {
            java.util.concurrent.CompletableFuture<Boolean> write = request.descriptorWrite();
            if (write == null) {
                super.onAckTimeout();
                return;
            }
            write.whenComplete((created, failure) -> {
                if (failure != null) {
                    onFailure("create-index [" + request.index() + "] descriptor write", unwrap(failure));
                } else if (Boolean.TRUE.equals(created) == false) {
                    onFailure(
                        "create-index [" + request.index() + "] descriptor write",
                        new ResourceAlreadyExistsException(request.index())
                    );
                } else {
                    super.onAckTimeout();
                }
            });
        }

        /** Unwraps the CompletionException the future stage adds, so the client sees the real cause. */
        private Exception unwrap(Throwable failure) {
            Throwable cause = failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null
                ? failure.getCause()
                : failure;
            return cause instanceof Exception exception ? exception : new OpenSearchException(cause);
        }
    }

    /**
     * Applies a batch of index creations as a single cluster-state update.
     *
     * <p>Index creation previously submitted an {@code AckedClusterStateUpdateTask} that was its own
     * executor, and {@code ClusterStateUpdateTask}'s executor implementation asserts a batch size of
     * one -- so N concurrent creations cost N full cluster-state cycles, each with its own diff,
     * publication and cluster-wide acknowledgement round. Provisioning many indices at once is
     * exactly the workload that made this expensive.
     *
     * <p>{@link #applyCreateIndexRequest} both takes and returns a {@link ClusterState}, so the batch
     * is a straightforward fold. Each task is recorded as an individual success or failure, so one
     * bad request (a duplicate name, a validation error) fails only itself and the rest of the batch
     * still applies -- matching the per-request behaviour callers had before.
     */
    final ClusterStateTaskExecutor<CreateIndexTask> createIndexExecutor = (currentState, tasks) -> {
        final ClusterStateTaskExecutor.ClusterTasksResult.Builder<CreateIndexTask> builder = ClusterStateTaskExecutor.ClusterTasksResult
            .builder();
        ClusterState state = currentState;
        for (CreateIndexTask task : tasks) {
            try {
                state = applyCreateIndexRequest(state, task.request, false);
                builder.success(task);
            } catch (Exception e) {
                builder.failure(task, e);
            }
        }
        return builder.build(state);
    };

    private void normalizeRequestSetting(CreateIndexClusterStateUpdateRequest createIndexClusterStateRequest) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        Settings build = updatedSettingsBuilder.put(createIndexClusterStateRequest.settings())
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
            .build();
        indexScopedSettings.validate(build, true);
        createIndexClusterStateRequest.settings(build);
    }

    /**
     * Handles the cluster state transition to a version that reflects the {@link CreateIndexClusterStateUpdateRequest}.
     * All the requested changes are firstly validated before mutating the {@link ClusterState}.
     */
    public ClusterState applyCreateIndexRequest(
        ClusterState currentState,
        CreateIndexClusterStateUpdateRequest request,
        boolean silent,
        BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer
    ) throws Exception {

        normalizeRequestSetting(request);
        logger.trace("executing IndexCreationTask for [{}] against cluster state version [{}]", request, currentState.version());

        validate(request, currentState);

        final Index recoverFromIndex = request.recoverFrom();
        final IndexMetadata sourceMetadata = recoverFromIndex == null ? null : currentState.metadata().getIndexSafe(recoverFromIndex);

        if (sourceMetadata != null) {
            // If source metadata was provided, it means we're recovering from an existing index,
            // in which case templates don't apply, so create the index from the source metadata
            return applyCreateIndexRequestWithExistingMetadata(currentState, request, silent, sourceMetadata, metadataTransformer);
        } else {
            // The backing index may have a different name or prefix than the data stream name.
            final String name = request.dataStreamName() != null ? request.dataStreamName() : request.index();

            // Do not apply any templates to system indices
            if (systemIndices.isSystemIndex(name)) {
                return applyCreateIndexRequestWithNoTemplates(currentState, request, silent, metadataTransformer);
            }

            // Hidden indices apply templates slightly differently (ignoring wildcard '*'
            // templates), so we need to check to see if the request is creating a hidden index
            // prior to resolving which templates it matches
            final Boolean isHiddenFromRequest = IndexMetadata.INDEX_HIDDEN_SETTING.exists(request.settings())
                ? IndexMetadata.INDEX_HIDDEN_SETTING.get(request.settings())
                : null;

            // Check to see if a v2 template matched
            final String v2Template = MetadataIndexTemplateService.findV2Template(
                currentState.metadata(),
                name,
                isHiddenFromRequest == null ? false : isHiddenFromRequest
            );

            if (v2Template != null) {
                // If a v2 template was found, it takes precedence over all v1 templates, so create
                // the index using that template and the request's specified settings
                return applyCreateIndexRequestWithV2Template(currentState, request, silent, v2Template, metadataTransformer);
            } else {
                // A v2 template wasn't found, check the v1 templates, in the event no templates are
                // found creation still works using the request's specified index settings
                final List<IndexTemplateMetadata> v1Templates = MetadataIndexTemplateService.findV1Templates(
                    currentState.metadata(),
                    request.index(),
                    isHiddenFromRequest
                );

                if (v1Templates.size() > 1) {
                    DEPRECATION_LOGGER.deprecate(
                        "index_template_multiple_match",
                        "index [{}] matches multiple legacy templates [{}], composable templates will only match a single template",
                        request.index(),
                        v1Templates.stream().map(IndexTemplateMetadata::name).sorted().collect(Collectors.joining(", "))
                    );
                }

                return applyCreateIndexRequestWithV1Templates(currentState, request, silent, v1Templates, metadataTransformer);
            }
        }
    }

    public ClusterState applyCreateIndexRequest(ClusterState currentState, CreateIndexClusterStateUpdateRequest request, boolean silent)
        throws Exception {
        return applyCreateIndexRequest(currentState, request, silent, null);
    }

    /**
     * Given the state and a request as well as the metadata necessary to build a new index,
     * validate the configuration with an actual index service as return a new cluster state with
     * the index added (and rerouted)
     * @param currentState the current state to base the new state off of
     * @param request the create index request
     * @param silent a boolean for whether logging should be at a lower or higher level
     * @param sourceMetadata when recovering from an existing index, metadata that should be copied to the new index
     * @param temporaryIndexMeta metadata for the new index built from templates, source metadata, and request settings
     * @param mappings a list of all mapping definitions to apply, in order
     * @param aliasSupplier a function that takes the real {@link IndexService} and returns a list of {@link AliasMetadata} aliases
     * @param templatesApplied a list of the names of the templates applied, for logging
     * @param metadataTransformer if provided, a function that may alter cluster metadata in the same cluster state update that
     *                            creates the index
     * @return a new cluster state with the index added
     */
    private ClusterState applyCreateIndexWithTemporaryService(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final IndexMetadata sourceMetadata,
        final IndexMetadata temporaryIndexMeta,
        final List<Map<String, Object>> mappings,
        final BiFunction<IndexService, Map<String, AliasMetadata>, List<AliasMetadata>> aliasSupplier,
        final List<String> templatesApplied,
        final List<Map<String, AliasMetadata>> templateAliases,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer
    ) throws Exception {
        if (needsNothingFromATemporaryIndexService(
            request,
            sourceMetadata,
            temporaryIndexMeta,
            mappings,
            templatesApplied,
            templateAliases
        )) {
            String needsAMapperService = whyTheseMappingsNeedAMapperService(mappings);
            if (needsAMapperService == null) {
                return applyCreateIndexWithoutTemporaryService(currentState, request, temporaryIndexMeta, mappings, metadataTransformer);
            }
            // Everything except the mapping is satisfied, so what is missing is a mapper service and not an
            // index service. T60 measured the difference: building the whole thing serialises every
            // concurrent creation on the node behind one monitor and costs about 21.6 ms of service time
            // each, which is seven times what the creation itself costs.
            logger.debug("[{}] validating with a mapper service alone: {}", request.index(), needsAMapperService);
            ClusterState withAMapperService = applyCreateIndexWithOnlyAMapperService(
                currentState,
                request,
                temporaryIndexMeta,
                mappings,
                metadataTransformer,
                silent
            );
            if (withAMapperService != null) {
                return withAMapperService;
            }
            // A composite index, which is the one thing a mapper service cannot finish validating on its
            // own. Nothing was published and the merge happened in a throwaway service, so falling through
            // to the full path below repeats the work rather than continuing from half-done state.
        }
        // create the index here (on the master) to validate it can be created, as well as adding the mapping
        return indicesService.<ClusterState, Exception>withTempIndexService(temporaryIndexMeta, indexService -> {
            Settings.Builder tmpSettingsBuilder = Settings.builder().put(temporaryIndexMeta.getSettings());

            List<Map<String, Object>> updatedMappings = new ArrayList<>();
            updatedMappings.addAll(mappings);

            Template contextTemplate = applyContext(request, currentState, updatedMappings, tmpSettingsBuilder);

            try {
                updateIndexMappingsAndBuildSortOrder(indexService, request, updatedMappings, sourceMetadata, indexCreationValidators);
            } catch (Exception e) {
                logger.log(silent ? Level.DEBUG : Level.INFO, "failed on parsing mappings on index creation [{}]", request.index(), e);
                throw e;
            }

            final List<AliasMetadata> aliases = aliasSupplier.apply(
                indexService,
                Optional.ofNullable(contextTemplate).map(Template::aliases).orElse(Map.of())
            );

            final IndexMetadata indexMetadata;
            try {
                indexMetadata = buildIndexMetadata(
                    request.index(),
                    aliases,
                    indexService.mapperService()::documentMapper,
                    tmpSettingsBuilder.build(),
                    temporaryIndexMeta.getRoutingNumShards(),
                    sourceMetadata,
                    temporaryIndexMeta.isSystem(),
                    temporaryIndexMeta.getCustomData(),
                    temporaryIndexMeta.context()
                );
            } catch (Exception e) {
                logger.info("failed to build index metadata [{}]", request.index());
                throw e;
            }

            logger.log(
                silent ? Level.DEBUG : Level.INFO,
                "[{}] creating index, cause [{}], templates {}, shards [{}]/[{}]",
                request.index(),
                request.cause(),
                templatesApplied,
                indexMetadata.getNumberOfShards(),
                indexMetadata.getNumberOfReplicas()
            );

            indexService.getIndexEventListener().beforeIndexAddedToCluster(indexMetadata.getIndex(), indexMetadata.getSettings());
            return clusterStateCreateIndex(
                currentState,
                request.blocks(),
                indexMetadata,
                allocationService::reroute,
                metadataTransformer,
                request::descriptorWrite
            );
        });
    }

    /**
     * Whether a creation can be finished without building a throwaway {@link IndexService} to validate it.
     *
     * <h4>Why this exists</h4>
     *
     * {@link IndicesService#withTempIndexService} builds a complete {@code IndexService} -- settings,
     * analysis, mappers, caches, the engine factory, every plugin's {@code onIndexModule} hook -- runs the
     * validation against it, and throws it away. For an index with a mapping that is what pays for the
     * mapping being checked before anything is published. For a gated index it buys nothing twice over:
     * there is no mapping to check, and {@link IndexDescriptor#from} discards mappings anyway, so the object
     * is built to produce a result that is then dropped.
     *
     * <p>It is also built under a lock. {@code IndicesService.createIndexService} is {@code synchronized},
     * so every concurrent creation queues on one monitor on the cluster manager. Profiling 215,000 gated
     * creations at concurrency 16 recorded 122,222 blocking events on that monitor totalling 1,453 seconds
     * of blocked thread time in a 120 second run -- around 60% of all generic-thread time -- with a further
     * 41.5% of on-CPU samples inside the same call tree. That is the ceiling, and it is spent on an object
     * whose output is discarded.
     *
     * <p>The lock is not touched. It is inherited from upstream with no recorded rationale, and changing it
     * would change concurrency for every index in every cluster, which is exactly what R1 forbids. Not
     * entering it is a decision this path can make for itself.
     *
     * <h4>Every condition, and what would go wrong without it</h4>
     *
     * <ul>
     *   <li><b>Admitted as gated.</b> Restricts the whole bypass to indices the descriptor gate already
     *       claimed, so an ordinary cluster cannot reach it at all.</li>
     *   <li><b>No mappings, from the request or a template.</b> Otherwise there is a mapping to merge and
     *       validate, and skipping it would accept a malformed mapping silently.</li>
     *   <li><b>No aliases, from the request or a template.</b> Alias resolution validates a filter against a
     *       {@link org.opensearch.index.query.QueryShardContext} that only an {@code IndexService} can make.
     *       <p>This condition is about aliases rather than about templates, and the difference is the whole
     *       value of the change. The first version declined whenever any template applied at all, which is
     *       simpler and made the bypass almost unreachable: a template that carries only settings needs
     *       nothing validated, and settings-only templates are the normal way to configure a large tenant
     *       population. It is also how the defect was found -- every internal cluster test runs under a
     *       wildcard {@code random_index_template} that sets settings and nothing else, so the fast path
     *       never once fired and the test asserting it fires is what said so.</li>
     *   <li><b>No index sort.</b> {@code getIndexSortSupplier} resolves sort fields against the merged
     *       mappings, which is a real check with no cheaper form.</li>
     *   <li><b>No source metadata and no data stream.</b> Both add validation of their own further in.</li>
     *   <li><b>No context.</b> {@code applyContext} can add mappings and settings after this point, so a
     *       request carrying one has not finished being assembled.</li>
     *   <li><b>Every registered {@link IndexCreationValidator} answers {@code requiresMappings() == false}.</b>
     *       They still run below, against real {@link IndexSettings} and a null mapper service, which is the
     *       contract that method exists to state. The default is true, so a plugin that says nothing keeps
     *       the temporary service and its own behaviour unchanged.</li>
     * </ul>
     *
     * <p>Fails closed: every condition has to hold, and anything unrecognised takes the ordinary path.
     */
    private boolean needsNothingFromATemporaryIndexService(
        final CreateIndexClusterStateUpdateRequest request,
        final IndexMetadata sourceMetadata,
        final IndexMetadata temporaryIndexMeta,
        final List<Map<String, Object>> mappings,
        final List<String> templatesApplied,
        final List<Map<String, AliasMetadata>> templateAliases
    ) {
        String declined = whyATemporaryIndexServiceIsStillNeeded(
            request,
            sourceMetadata,
            temporaryIndexMeta,
            mappings,
            templatesApplied,
            templateAliases
        );
        if (declined != null) {
            logger.debug("[{}] keeping the temporary index service: {}", request.index(), declined);
            return false;
        }
        return true;
    }

    /**
     * The first condition that fails, named, or null when none do.
     *
     * <p>Separated from the predicate so a decline has a reason rather than a boolean. Eight conditions
     * reduced to one {@code false} is a thing that can be quietly wrong for months: the bypass would simply
     * never fire, every measurement would look like the change did nothing, and there would be nothing to
     * read. That is not hypothetical -- the first run of
     * {@code GatedCreationWithoutTemporaryIndexServiceIT} failed exactly this way, and this method is what
     * turned it from a guess into a lookup.
     */
    private String whyATemporaryIndexServiceIsStillNeeded(
        final CreateIndexClusterStateUpdateRequest request,
        final IndexMetadata sourceMetadata,
        final IndexMetadata temporaryIndexMeta,
        final List<Map<String, Object>> mappings,
        final List<String> templatesApplied,
        final List<Map<String, AliasMetadata>> templateAliases
    ) {
        // The same question the road above asked, asked the same way, and since the namespace it is a
        // string comparison rather than a template resolution. It used to read the request's own settings,
        // which missed a template-gated creation and made it pay for the throwaway IndexService this exists
        // to avoid; then it read template-merged settings, which cost a resolution here and another there.
        // A name costs neither and cannot disagree with the gate, because the gate reads the same name.
        //
        // Phase D2 of core-pluggability-refactor-plan.md: same migration as createIndex()'s own top branch,
        // negated -- isRegistered()==false || namesAServerlessIndex(...)==false is De Morgan's equivalent of
        // !claims(...).
        if (IndexCreationStrategyRegistry.claims(request.index(), request) == false) {
            // Phase J3: description from the registered strategy, not core's own words for one product's.
            return "the index is not in " + IndexCreationStrategyRegistry.describeClaimedNamespace();
        }
        if (sourceMetadata != null) {
            return "it is being built from an existing index";
        }
        if (request.dataStreamName() != null) {
            return "it backs the data stream [" + request.dataStreamName() + "]";
        }
        if (request.context() != null) {
            return "it carries a context, which can still add mappings and settings";
        }
        if (request.aliases().isEmpty() == false) {
            return "it has " + request.aliases().size() + " alias(es), whose filters need a query shard context";
        }
        // Every map, not the list. resolveAliases returns one entry per template, so a template that
        // declares no aliases still contributes an empty map and leaves the outer list non-empty. Reading
        // the outer list is the same predicate as "any template applied", which is exactly the condition
        // this replaced -- so getting the level wrong here silently reverts the change while looking correct.
        for (Map<String, AliasMetadata> fromOneTemplate : templateAliases) {
            if (fromOneTemplate.isEmpty() == false) {
                return "templates " + templatesApplied + " contribute alias(es) " + fromOneTemplate.keySet();
            }
        }
        // The mapping is deliberately not one of these conditions. It used to be: a declared mapping meant a
        // throwaway IndexService, and T18 measured what that cost once mapped indices became the common
        // kind. What a mapping needs is a *mapper service*, which T60 separated from an index service --
        // see whyTheseMappingsNeedAMapperService and applyCreateIndexWithOnlyAMapperService. What the
        // conditions here have in common is that each one needs something a mapper service does not have:
        // a query shard context, a sort supplier, an existing index's metadata.
        if (org.opensearch.index.IndexSortConfig.INDEX_SORT_FIELD_SETTING.exists(temporaryIndexMeta.getSettings())) {
            return "it configures an index sort, which resolves against the merged mappings";
        }
        for (IndexCreationValidator validator : indexCreationValidators) {
            if (validator.requiresMappings()) {
                return "the validator " + validator.getClass().getName() + " reads the mapper service";
            }
        }
        return null;
    }

    /**
     * The same creation, with the temporary {@link IndexService} not built.
     *
     * <p>Reachable only when {@link #needsNothingFromATemporaryIndexService} holds, which is what makes the
     * omissions here safe rather than merely cheap. There are no mappings so the document mapper is null and
     * {@link #buildIndexMetadata} puts none; there are no aliases so the list is empty; the settings are
     * {@code temporaryIndexMeta}'s unchanged, because the only thing that rewrites them at this point is
     * {@code applyContext}, and a request carrying a context does not get here.
     *
     * <p><b>One thing is genuinely not done, and it is worth naming rather than leaving to be discovered.</b>
     * {@code IndexEventListener.beforeIndexAddedToCluster} is not called, because the listener chain belongs
     * to the {@code IndexService} that is no longer built. Nothing in this repository implements it -- the
     * interface's own method is an empty default and {@code CompositeIndexEventListener} only forwards -- so
     * this changes no behaviour here. A third-party plugin that implements it would not see the hook for a
     * gated creation. That is the one behavioural difference on this path, and it is confined to indices the
     * descriptor gate has already claimed.
     */
    /**
     * Why this mapping cannot be validated without a mapper service, or null when it can.
     *
     * <p>Answers only for the shape it can answer completely: every top-level property whose definition is
     * exactly a declared type, and every one of those types registered on this node. That is not a subset
     * of validation, it is all of it for such a field -- a definition with one key has nothing else that
     * could be wrong.
     *
     * <p>Everything else is refused rather than half-checked. A parameter needs its own validation (an
     * analyzer has to resolve against the index's analysis configuration, a date format has to parse), an
     * object field needs its sub-properties walked, and a shorthand needs interpreting. Accepting any of
     * those here on the strength of the type name would be a mapping accepted and not really checked,
     * which is the failure this area has produced repeatedly.
     */
    private String whyThisMappingNeedsAMapperService(Map<String, Object> mapping) {
        Object properties = propertiesOf(mapping).get("properties");
        if (properties instanceof Map == false) {
            // No properties to validate. Metadata fields and mapping-level settings are not covered by the
            // registry check, so anything other than a plain properties object goes the long way.
            return mapping.isEmpty() ? null : "its mapping declares more than field properties";
        }
        for (Map.Entry<?, ?> property : ((Map<?, ?>) properties).entrySet()) {
            Object definition = property.getValue();
            if (definition instanceof Map == false) {
                return "field [" + property.getKey() + "] uses a shorthand definition, which needs interpreting";
            }
            Map<?, ?> asMap = (Map<?, ?>) definition;
            Object type = asMap.get("type");
            if (type == null || asMap.size() != 1) {
                return "field [" + property.getKey() + "] declares more than a type, which needs a mapper service to validate";
            }
            if (indicesService.hasFieldTypeParser(String.valueOf(type)) == false) {
                // Not a refusal of the fast path but of the request. Left to the ordinary path so the
                // caller gets the same error it has always got, from the code that owns that message.
                return "field [" + property.getKey() + "] declares unknown type [" + type + "]";
            }
        }
        return null;
    }

    /**
     * Why these mappings cannot be validated from the mapper registry alone, or null when they can.
     *
     * <p>Two sources of mapping -- a template's and the request's -- used to send a creation down the full
     * path, on the reasoning that their precedence is what the index service implements. It is not: the
     * merge is {@code mapperService.merge} once per mapping in order, which is a mapper service's job and
     * nothing an {@code IndexService} contributes to. So more than one mapping needs a mapper service,
     * which is what this answers, rather than needing an index service, which is what it used to be read as.
     */
    private String whyTheseMappingsNeedAMapperService(List<Map<String, Object>> mappings) {
        List<Map<String, Object>> declared = mappings.stream().filter(mapping -> mapping.isEmpty() == false).collect(toList());
        if (declared.isEmpty()) {
            return null;
        }
        if (declared.size() > 1) {
            return "it has " + declared.size() + " mappings to merge in order";
        }
        return whyThisMappingNeedsAMapperService(declared.get(0));
    }

    /**
     * The same creation, validated against a mapper service rather than a whole {@link IndexService}.
     *
     * <p>Reachable only when every condition in {@link #whyATemporaryIndexServiceIsStillNeeded} holds and
     * the mapping is the one thing left, so what this omits is what those conditions already established is
     * not needed: no aliases to resolve against a query shard context, no index sort to build, no source
     * index, no context rewriting the settings, and no validator that reads mappings beyond the ones run
     * here. {@code beforeIndexAddedToCluster} is not called, for the same reason and with the same
     * consequence the fast path documents: the listener chain belongs to the index service that is not
     * built, nothing in this repository implements the hook, and a third-party plugin that does would not
     * see it for a gated creation.
     *
     * @return the new cluster state, or null when the merged mapping turns out to declare a composite index
     *         -- the one thing whose validation needs settings a mapper service does not carry, and rare
     *         enough to be worth paying the full path for rather than reimplementing here.
     */
    private ClusterState applyCreateIndexWithOnlyAMapperService(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final IndexMetadata temporaryIndexMeta,
        final List<Map<String, Object>> mappings,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer,
        final boolean silent
    ) throws Exception {
        try (MapperService mapperService = indicesService.createMapperServiceForValidation(temporaryIndexMeta)) {
            for (Map<String, Object> mapping : mappings) {
                if (mapping.isEmpty() == false) {
                    // In order and with the same merge reason as the full path, because this is the whole of
                    // what that path did with the mappings.
                    mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mapping, MergeReason.INDEX_TEMPLATE);
                }
            }
            if (mapperService.isCompositeIndexPresent()) {
                return null;
            }
            for (IndexCreationValidator validator : indexCreationValidators) {
                // A real mapper service, unlike the fast path's null one, so a validator that reads mappings
                // is no longer a reason to decline this road -- it is served here exactly as it would have
                // been through the index service.
                validator.validate(mapperService, mapperService.getIndexSettings());
            }

            IndexMetadata indexMetadata = buildIndexMetadata(
                request.index(),
                List.of(),
                mapperService::documentMapper,
                temporaryIndexMeta.getSettings(),
                temporaryIndexMeta.getRoutingNumShards(),
                null,
                temporaryIndexMeta.isSystem(),
                temporaryIndexMeta.getCustomData(),
                temporaryIndexMeta.context()
            );

            logger.log(
                silent ? Level.DEBUG : Level.INFO,
                "[{}] creating index, cause [{}], shards [{}]/[{}]",
                request.index(),
                request.cause(),
                indexMetadata.getNumberOfShards(),
                indexMetadata.getNumberOfReplicas()
            );

            return clusterStateCreateIndex(
                currentState,
                request.blocks(),
                indexMetadata,
                allocationService::reroute,
                metadataTransformer,
                request::descriptorWrite
            );
        }
    }

    /**
     * A parsed mapping with its single type key removed, if it has one.
     *
     * <p>The type key is excluded by name rather than by position, because "the only key" is ambiguous: a
     * mapping written as {@code {"properties": {...}}} is also a single-entry map, and unwrapping it yields
     * the field list where the caller expects the mapping body. That mistake does not fail loudly -- it
     * reads as a mapping with no properties, so the fast path simply declines every request and the change
     * appears to do nothing.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> propertiesOf(Map<String, Object> mapping) {
        if (mapping.size() == 1) {
            Map.Entry<String, Object> only = mapping.entrySet().iterator().next();
            if (only.getValue() instanceof Map && "properties".equals(only.getKey()) == false) {
                return (Map<String, Object>) only.getValue();
            }
        }
        return mapping;
    }

    private ClusterState applyCreateIndexWithoutTemporaryService(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final IndexMetadata temporaryIndexMeta,
        final List<Map<String, Object>> mappings,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer
    ) {
        if (indexCreationValidators.isEmpty() == false) {
            // Real settings, so a validator sees exactly what it would have seen through the index service.
            // Null mapper service, which every validator reaching this point has declared it does not read.
            IndexSettings indexSettings = new IndexSettings(temporaryIndexMeta, settings, indexScopedSettings);
            for (IndexCreationValidator validator : indexCreationValidators) {
                validator.validate(null, indexSettings);
            }
        }

        IndexMetadata indexMetadata = buildIndexMetadata(
            request.index(),
            List.of(),
            () -> null,
            temporaryIndexMeta.getSettings(),
            temporaryIndexMeta.getRoutingNumShards(),
            null,
            temporaryIndexMeta.isSystem(),
            temporaryIndexMeta.getCustomData(),
            temporaryIndexMeta.context()
        );

        // The declared mapping, put on the metadata by hand because there is no document mapper to take it
        // from -- buildIndexMetadata is given () -> null above, since building one is the cost this path
        // exists to avoid.
        //
        // This is not a nicety. Everything downstream reads an index's declared fields off IndexMetadata:
        // DescriptorRepresentable decides gating from it, and clusterStateCreateIndex writes them to the
        // mapping store from it. Without this the fast path would create a gated index whose mapping was
        // parsed, validated against the registry, and then present nowhere -- acknowledged, with the fields
        // silently absent. That is the loss T11, T13 and T15 were spent removing, and a performance change
        // is exactly the kind of change that reintroduces it quietly.
        for (Map<String, Object> mapping : mappings) {
            if (mapping.isEmpty() == false) {
                indexMetadata = IndexMetadata.builder(indexMetadata)
                    .putMapping(new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mapping))
                    .build();
                break;
            }
        }

        logger.debug(
            "[{}] creating index off the temporary index service path, cause [{}], shards [{}]/[{}]",
            request.index(),
            request.cause(),
            indexMetadata.getNumberOfShards(),
            indexMetadata.getNumberOfReplicas()
        );

        return clusterStateCreateIndex(
            currentState,
            request.blocks(),
            indexMetadata,
            allocationService::reroute,
            metadataTransformer,
            request::descriptorWrite
        );
    }

    Template applyContext(
        CreateIndexClusterStateUpdateRequest request,
        ClusterState currentState,
        List<Map<String, Object>> mappings,
        Settings.Builder settingsBuilder
    ) throws IOException {
        if (request.context() != null) {
            ComponentTemplate componentTemplate = MetadataIndexTemplateService.findComponentTemplate(
                currentState.metadata(),
                request.context()
            );

            if (componentTemplate.template().mappings() != null) {
                // Mappings added at last (priority to mappings provided)
                mappings.add(MapperService.parseMapping(xContentRegistry, componentTemplate.template().mappings().toString()));
            }

            if (componentTemplate.template().settings() != null) {
                validateOverlap(settingsBuilder.keys(), componentTemplate.template().settings(), request.index()).ifPresent(message -> {
                    ValidationException validationException = new ValidationException();
                    validationException.addValidationError(message);
                    throw validationException;
                });
                // Settings applied at last
                settingsBuilder.put(componentTemplate.template().settings());
            }

            settingsBuilder.put(IndexSettings.INDEX_CONTEXT_CREATED_VERSION.getKey(), componentTemplate.version());
            settingsBuilder.put(IndexSettings.INDEX_CONTEXT_CURRENT_VERSION.getKey(), componentTemplate.version());

            return componentTemplate.template();
        }
        return null;
    }

    static Optional<String> validateOverlap(Set<String> requestSettings, Settings contextTemplateSettings, String indexName) {
        if (requestSettings.stream().anyMatch(contextTemplateSettings::hasValue)) {
            return Optional.of(
                "Cannot apply context template as user provide settings have overlap with the included context template."
                    + "Please remove the settings ["
                    + Sets.intersection(requestSettings, contextTemplateSettings.keySet())
                    + "] to continue using the context for index: "
                    + indexName
            );
        }
        return Optional.empty();
    }

    /**
     * Given a state and index settings calculated after applying templates, validate metadata for
     * the new index, returning an {@link IndexMetadata} for the new index.
     * <p>
     * The access level of the method changed to default level for visibility to test.
     */
    IndexMetadata buildAndValidateTemporaryIndexMetadata(
        final Settings aggregatedIndexSettings,
        final CreateIndexClusterStateUpdateRequest request,
        final int routingNumShards,
        final ClusterState clusterState
    ) {

        final boolean isHiddenAfterTemplates = IndexMetadata.INDEX_HIDDEN_SETTING.get(aggregatedIndexSettings);
        final boolean isSystem = validateDotIndex(request.index(), isHiddenAfterTemplates);

        final IndexMetadata.Builder tmpImdBuilder = IndexMetadata.builder(request.index());
        tmpImdBuilder.setRoutingNumShards(routingNumShards);
        tmpImdBuilder.settings(aggregatedIndexSettings);
        tmpImdBuilder.system(isSystem);
        addRemoteStoreCustomMetadata(tmpImdBuilder, true, clusterState);

        if (request.context() != null) {
            tmpImdBuilder.context(request.context());
        }

        // Set up everything, now locally create the index to see that things are ok, and apply
        IndexMetadata tempMetadata = tmpImdBuilder.build();
        validateActiveShardCount(request.waitForActiveShards(), tempMetadata);

        return tempMetadata;
    }

    /**
     * Adds the 1) remote store path type 2) ckp as translog metadata information in custom data of index metadata.
     *
     * @param tmpImdBuilder     index metadata builder.
     * @param assertNullOldType flag to verify that the old remote store path type is null
     */
    public void addRemoteStoreCustomMetadata(IndexMetadata.Builder tmpImdBuilder, boolean assertNullOldType, ClusterState clusterState) {

        boolean isRestoreFromSnapshot = !assertNullOldType;
        if (remoteStoreCustomMetadataResolver == null) {
            return;
        }
        // It is possible that remote custom data exists already. In such cases, we need to only update the path type
        // in the remote store custom data map.
        Map<String, String> existingCustomData = tmpImdBuilder.removeCustom(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        assert assertNullOldType == false || Objects.isNull(existingCustomData);

        Map<String, String> remoteCustomData = new HashMap<>();

        // Determine if the ckp would be stored as translog metadata
        boolean isTranslogMetadataEnabled = remoteStoreCustomMetadataResolver.isTranslogMetadataEnabled();
        remoteCustomData.put(IndexMetadata.TRANSLOG_METADATA_KEY, Boolean.toString(isTranslogMetadataEnabled));

        Optional<DiscoveryNode> remoteNode = clusterState.nodes()
            .getNodes()
            .values()
            .stream()
            .filter(DiscoveryNode::isRemoteStoreNode)
            .findFirst();

        String sseEnabledIndex = existingCustomData == null
            ? null
            : existingCustomData.get(IndexMetadata.REMOTE_STORE_SSE_ENABLED_INDEX_KEY);
        if (isRestoreFromSnapshot && sseEnabledIndex != null) {
            remoteCustomData.put(IndexMetadata.REMOTE_STORE_SSE_ENABLED_INDEX_KEY, sseEnabledIndex);
        } else if (remoteNode.isPresent()
            && !isRestoreFromSnapshot
            && remoteStoreCustomMetadataResolver.isRemoteStoreRepoServerSideEncryptionEnabled()) {
                remoteCustomData.put(IndexMetadata.REMOTE_STORE_SSE_ENABLED_INDEX_KEY, Boolean.toString(true));
            }

        // Determine the path type for use using the remoteStorePathResolver.
        RemoteStorePathStrategy newPathStrategy = remoteStoreCustomMetadataResolver.getPathStrategy();
        remoteCustomData.put(PathType.NAME, newPathStrategy.getType().name());
        if (Objects.nonNull(newPathStrategy.getHashAlgorithm())) {
            remoteCustomData.put(PathHashAlgorithm.NAME, newPathStrategy.getHashAlgorithm().name());
        }
        logger.trace(
            () -> new ParameterizedMessage("Added newCustomData={}, replaced oldCustomData={}", remoteCustomData, existingCustomData)
        );
        tmpImdBuilder.putCustom(IndexMetadata.REMOTE_STORE_CUSTOM_KEY, remoteCustomData);
    }

    private ClusterState applyCreateIndexRequestWithNoTemplates(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer
    ) throws Exception {
        // Using applyCreateIndexRequestWithV1Templates with empty list instead of applyCreateIndexRequestWithV2Template
        // with null template as applyCreateIndexRequestWithV2Template has assertions when template is null
        return applyCreateIndexRequestWithV1Templates(currentState, request, silent, Collections.emptyList(), metadataTransformer);
    }

    private ClusterState applyCreateIndexRequestWithV1Templates(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final List<IndexTemplateMetadata> templates,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer
    ) throws Exception {
        logger.debug(
            "applying create index request using legacy templates {}",
            templates.stream().map(IndexTemplateMetadata::name).collect(Collectors.toList())
        );

        final Map<String, Object> mappings = Collections.unmodifiableMap(
            parseV1Mappings(
                request.mappings(),
                templates.stream().map(IndexTemplateMetadata::getMappings).collect(toList()),
                xContentRegistry
            )
        );
        final Settings aggregatedIndexSettings = aggregateIndexSettings(
            currentState,
            request,
            MetadataIndexTemplateService.resolveSettings(templates),
            null,
            settings,
            indexScopedSettings,
            shardLimitValidator,
            indexSettingProviders,
            clusterService.getClusterSettings()
        );
        int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, null);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(aggregatedIndexSettings, request, routingNumShards, currentState);

        return applyCreateIndexWithTemporaryService(
            currentState,
            request,
            silent,
            null,
            tmpImd,
            Collections.singletonList(mappings),
            (indexService, contextAlias) -> resolveAndValidateAliases(
                request.index(),
                request.aliases(),
                Stream.concat(Stream.of(contextAlias), MetadataIndexTemplateService.resolveAliases(templates).stream()).collect(toList()),
                currentState.metadata(),
                aliasValidator,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry,
                indexService.newQueryShardContext(0, null, () -> 0L, null)
            ),
            templates.stream().map(IndexTemplateMetadata::getName).collect(toList()),
            MetadataIndexTemplateService.resolveAliases(templates),
            metadataTransformer
        );
    }

    private ClusterState applyCreateIndexRequestWithV2Template(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final String templateName,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer
    ) throws Exception {
        logger.debug("applying create index request using composable template [{}]", templateName);

        ComposableIndexTemplate template = currentState.getMetadata().templatesV2().get(templateName);
        if (request.dataStreamName() == null && template.getDataStreamTemplate() != null) {
            throw new IllegalArgumentException(
                "cannot create index with name ["
                    + request.index()
                    + "], because it matches with template ["
                    + templateName
                    + "] that creates data streams only, "
                    + "use create data stream api instead"
            );
        }

        final List<Map<String, Object>> mappings = collectV2Mappings(
            request.mappings(),
            currentState,
            templateName,
            xContentRegistry,
            request.index()
        );
        final Settings aggregatedIndexSettings = aggregateIndexSettings(
            currentState,
            request,
            MetadataIndexTemplateService.resolveSettings(currentState.metadata(), templateName),
            null,
            settings,
            indexScopedSettings,
            shardLimitValidator,
            indexSettingProviders,
            clusterService.getClusterSettings()
        );
        int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, null);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(aggregatedIndexSettings, request, routingNumShards, currentState);

        return applyCreateIndexWithTemporaryService(
            currentState,
            request,
            silent,
            null,
            tmpImd,
            mappings,
            (indexService, contextAlias) -> resolveAndValidateAliases(
                request.index(),
                request.aliases(),
                Stream.concat(
                    Stream.of(contextAlias),
                    MetadataIndexTemplateService.resolveAliases(currentState.metadata(), templateName).stream()
                ).collect(toList()),
                currentState.metadata(),
                aliasValidator,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry,
                indexService.newQueryShardContext(0, null, () -> 0L, null)
            ),
            Collections.singletonList(templateName),
            MetadataIndexTemplateService.resolveAliases(currentState.metadata(), templateName),
            metadataTransformer
        );
    }

    public static List<Map<String, Object>> collectV2Mappings(
        final String requestMappings,
        final ClusterState currentState,
        final String templateName,
        final NamedXContentRegistry xContentRegistry,
        final String indexName
    ) throws Exception {
        List<CompressedXContent> templateMappings = MetadataIndexTemplateService.collectMappings(currentState, templateName, indexName);
        return collectV2Mappings(requestMappings, templateMappings, xContentRegistry);
    }

    public static List<Map<String, Object>> collectV2Mappings(
        final String requestMappings,
        final List<CompressedXContent> templateMappings,
        final NamedXContentRegistry xContentRegistry
    ) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();

        for (CompressedXContent templateMapping : templateMappings) {
            Map<String, Object> parsedTemplateMapping = MapperService.parseMapping(xContentRegistry, templateMapping.string());
            result.add(parsedTemplateMapping);
        }

        Map<String, Object> parsedRequestMappings = MapperService.parseMapping(xContentRegistry, requestMappings);
        result.add(parsedRequestMappings);

        // Apply disable_objects override logic to ensure template priority ordering is respected
        applyDisableObjectsOverrides(result);

        return result;
    }

    private ClusterState applyCreateIndexRequestWithExistingMetadata(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final IndexMetadata sourceMetadata,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer
    ) throws Exception {
        logger.info("applying create index request using existing index [{}] metadata", sourceMetadata.getIndex().getName());

        final Map<String, Object> mappings = MapperService.parseMapping(xContentRegistry, request.mappings());
        if (mappings.isEmpty() == false) {
            throw new IllegalArgumentException(
                "mappings are not allowed when creating an index from a source index, " + "all mappings are copied from the source index"
            );
        }

        final Settings aggregatedIndexSettings = aggregateIndexSettings(
            currentState,
            request,
            Settings.EMPTY,
            sourceMetadata,
            settings,
            indexScopedSettings,
            shardLimitValidator,
            indexSettingProviders,
            clusterService.getClusterSettings()
        );
        final int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, sourceMetadata);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(aggregatedIndexSettings, request, routingNumShards, currentState);

        return applyCreateIndexWithTemporaryService(
            currentState,
            request,
            silent,
            sourceMetadata,
            tmpImd,
            Collections.singletonList(mappings),
            (indexService, contextTemplate) -> resolveAndValidateAliases(
                request.index(),
                request.aliases(),
                Collections.emptyList(),
                currentState.metadata(),
                aliasValidator,
                xContentRegistry,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                indexService.newQueryShardContext(0, null, () -> 0L, null)
            ),
            List.of(),
            List.of(),
            metadataTransformer
        );
    }

    /**
     * Parses the provided mappings json and the inheritable mappings from the templates (if any)
     * into a map.
     * <p>
     * The template mappings are applied in the order they are encountered in the list (clients
     * should make sure the lower index, closer to the head of the list, templates have the highest
     * {@link IndexTemplateMetadata#order()}). This merging makes no distinction between field
     * definitions, as may result in an invalid field definition
     */
    static Map<String, Object> parseV1Mappings(
        String requestMappings,
        List<CompressedXContent> templateMappings,
        NamedXContentRegistry xContentRegistry
    ) throws Exception {
        // Step 1: Collect all mappings into a list
        List<Map<String, Object>> allMappings = new ArrayList<>();

        // Add template mappings first (lower priority)
        for (CompressedXContent mapping : templateMappings) {
            if (mapping != null) {
                Map<String, Object> templateMapping = MapperService.parseMapping(xContentRegistry, mapping.string());
                if (!templateMapping.isEmpty()) {
                    assert templateMapping.size() == 1 : "expected exactly one mapping value, got: " + templateMapping;
                    // pre-8x templates may have a wrapper type other than _doc, so we re-wrap things here
                    templateMapping = new HashMap<>(Map.of(MapperService.SINGLE_MAPPING_NAME, templateMapping.values().iterator().next()));
                    allMappings.add(templateMapping);
                }
            }
        }

        // Add request mapping last (highest priority)
        Map<String, Object> requestMapping = MapperService.parseMapping(xContentRegistry, requestMappings);
        if (!requestMapping.isEmpty()) {
            allMappings.add(requestMapping);
        }

        // Step 2: Apply shared disable_objects override logic (same as V2 templates)
        applyDisableObjectsOverrides(allMappings);

        // Step 3: Merge all mappings using field-aware replacement logic
        // Process mappings in forward order, with special handling for request mappings
        Map<String, Object> result = new HashMap<>();
        boolean hasRequestMapping = !MapperService.parseMapping(xContentRegistry, requestMappings).isEmpty();

        for (int i = 0; i < allMappings.size(); i++) {
            Map<String, Object> mapping = allMappings.get(i);
            boolean isRequestMapping = hasRequestMapping && (i == allMappings.size() - 1);

            if (result.isEmpty()) {
                result = new HashMap<>(mapping);
            } else {
                if (isRequestMapping) {
                    // For request mappings: request wins over templates
                    Map<String, Object> newMapping = new HashMap<>(mapping);
                    mergeTemplateFieldMappings(newMapping, result);
                    result = newMapping;
                } else {
                    // For template mappings: accumulated result (higher priority) wins over current (lower priority)
                    mergeTemplateFieldMappings(result, mapping);
                }
            }
        }

        return result;
    }

    /**
     * Merges template mappings with complete field definition replacement.
     * Higher priority mappings completely override field definitions from lower priority mappings.
     *
     * @param target the target mapping to merge into (higher priority)
     * @param source the source mapping to merge from (lower priority)
     */
    private static void mergeTemplateFieldMappings(Map<String, Object> target, Map<String, Object> source) {
        for (Map.Entry<String, Object> sourceEntry : source.entrySet()) {
            String key = sourceEntry.getKey();
            Object sourceValue = sourceEntry.getValue();

            if (!target.containsKey(key)) {
                // Key doesn't exist in target, add it
                target.put(key, sourceValue);
            } else if (key.equals("properties") && sourceValue instanceof Map && target.get(key) instanceof Map) {
                // Special handling for "properties" section - merge field definitions with replacement
                @SuppressWarnings("unchecked")
                Map<String, Object> targetProperties = (Map<String, Object>) target.get(key);
                @SuppressWarnings("unchecked")
                Map<String, Object> sourceProperties = (Map<String, Object>) sourceValue;
                mergeFieldProperties(targetProperties, sourceProperties);
            } else if (sourceValue instanceof Map && target.get(key) instanceof Map) {
                // Recursively merge other Map objects (but not field definitions)
                @SuppressWarnings("unchecked")
                Map<String, Object> targetMap = (Map<String, Object>) target.get(key);
                @SuppressWarnings("unchecked")
                Map<String, Object> sourceMap = (Map<String, Object>) sourceValue;
                mergeTemplateFieldMappings(targetMap, sourceMap);
            }
            // For non-Map values or when target already has the key, target value takes precedence (no override)
        }
    }

    /**
     * Merges field properties with complete field definition replacement.
     * If a field exists in both maps, the target field definition completely replaces the source.
     *
     * @param targetProperties the target properties map (higher priority)
     * @param sourceProperties the source properties map (lower priority)
     */
    private static void mergeFieldProperties(Map<String, Object> targetProperties, Map<String, Object> sourceProperties) {
        for (Map.Entry<String, Object> sourceField : sourceProperties.entrySet()) {
            String fieldName = sourceField.getKey();
            Object sourceFieldDef = sourceField.getValue();

            if (!targetProperties.containsKey(fieldName)) {
                // Field doesn't exist in target, add it
                targetProperties.put(fieldName, sourceFieldDef);
            } else {
                // If field exists in target, target takes precedence (higher priority template wins)
                // This ensures complete field definition replacement rather than property merging
            }
        }
    }

    /**
     * Applies disable_objects override logic to a list of mappings.
     * Later mappings in the list override earlier ones for the disable_objects setting,
     * ensuring template priority ordering is respected.
     *
     * @param mappings List of mapping objects to process
     */
    static void applyDisableObjectsOverrides(List<Map<String, Object>> mappings) {
        if (mappings == null || mappings.size() <= 1) {
            return; // No overrides needed for null, empty, or single mapping
        }

        Object finalDisableObjectsValue = null;
        boolean hasDisableObjects = false;

        // Find the last (highest priority) disable_objects value
        for (Map<String, Object> mapping : mappings) {
            if (mapping == null) {
                continue; // Skip null mappings
            }

            Object docObj = mapping.get(MapperService.SINGLE_MAPPING_NAME);
            if (docObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> doc = (Map<String, Object>) docObj;
                if (doc.containsKey("disable_objects")) {
                    finalDisableObjectsValue = doc.get("disable_objects");
                    hasDisableObjects = true;
                }
            }
        }

        // If we found a disable_objects value, apply it to all mappings that have a _doc section
        if (hasDisableObjects) {
            for (Map<String, Object> mapping : mappings) {
                if (mapping == null) {
                    continue; // Skip null mappings
                }

                Object docObj = mapping.get(MapperService.SINGLE_MAPPING_NAME);
                if (docObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> doc = (Map<String, Object>) docObj;
                    // Override with the final disable_objects value
                    doc.put("disable_objects", finalDisableObjectsValue);
                }
            }
        }
    }

    /**
     * Validates and creates the settings for the new index based on the explicitly configured settings via the
     * {@link CreateIndexClusterStateUpdateRequest}, inherited from templates and, if recovering from another index (ie. split, shrink,
     * clone), the resize settings.
     * <p>
     * The template mappings are applied in the order they are encountered in the list (clients should make sure the lower index, closer
     * to the head of the list, templates have the highest {@link IndexTemplateMetadata#order()})
     *
     * @return the aggregated settings for the new index
     */
    static Settings aggregateIndexSettings(
        ClusterState currentState,
        CreateIndexClusterStateUpdateRequest request,
        Settings combinedTemplateSettings,
        @Nullable IndexMetadata sourceMetadata,
        Settings settings,
        IndexScopedSettings indexScopedSettings,
        ShardLimitValidator shardLimitValidator,
        Set<IndexSettingProvider> indexSettingProviders,
        ClusterSettings clusterSettings
    ) {
        // Create builders for the template and request settings. We transform these into builders
        // because we may want settings to be "removed" from these prior to being set on the new
        // index (see more comments below)
        final Settings.Builder templateSettings = Settings.builder().put(combinedTemplateSettings);
        final Settings.Builder requestSettings = Settings.builder().put(request.settings());

        final Settings.Builder indexSettingsBuilder = Settings.builder();

        // Store type of `remote_snapshot` is intended to be system-managed for searchable snapshot indexes so a special case is needed here
        // to prevent a user specifying this value when creating an index
        String storeTypeSetting = request.settings().get(INDEX_STORE_TYPE_SETTING.getKey());
        if (storeTypeSetting != null && storeTypeSetting.equals(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT.toString())) {
            throw new IllegalArgumentException(
                "cannot create index with index setting \"index.store.type\" set to \"remote_snapshot\". Store type can be set to \"remote_snapshot\" only when restoring a remote snapshot by using \"storage_type\": \"remote_snapshot\""
            );
        }

        if (sourceMetadata == null) {
            final Settings.Builder additionalIndexSettings = Settings.builder();
            final Settings templateAndRequestSettings = Settings.builder().put(combinedTemplateSettings).put(request.settings()).build();

            final boolean isDataStreamIndex = request.dataStreamName() != null;
            // Loop through all the explicit index setting providers, adding them to the
            // additionalIndexSettings map
            for (IndexSettingProvider provider : indexSettingProviders) {
                additionalIndexSettings.put(
                    provider.getAdditionalIndexSettings(request.index(), isDataStreamIndex, templateAndRequestSettings)
                );
            }

            // For all the explicit settings, we go through the template and request level settings
            // and see if either a template or the request has "cancelled out" an explicit default
            // setting. For example, if a plugin had as an explicit setting:
            // "index.mysetting": "blah
            // And either a template or create index request had:
            // "index.mysetting": null
            // We want to remove the explicit setting not only from the explicitly set settings, but
            // also from the template and request settings, so that from the newly create index's
            // perspective it is as though the setting has not been set at all (using the default
            // value).
            for (String explicitSetting : additionalIndexSettings.keys()) {
                if (templateSettings.keys().contains(explicitSetting) && templateSettings.get(explicitSetting) == null) {
                    logger.debug(
                        "removing default [{}] setting as it in set to null in a template for [{}] creation",
                        explicitSetting,
                        request.index()
                    );
                    additionalIndexSettings.remove(explicitSetting);
                    templateSettings.remove(explicitSetting);
                }
                if (requestSettings.keys().contains(explicitSetting) && requestSettings.get(explicitSetting) == null) {
                    logger.debug(
                        "removing default [{}] setting as it in set to null in the request for [{}] creation",
                        explicitSetting,
                        request.index()
                    );
                    additionalIndexSettings.remove(explicitSetting);
                    requestSettings.remove(explicitSetting);
                }
            }

            // Finally, we actually add the explicit defaults prior to the template settings and the
            // request settings, so that the precedence goes:
            // Explicit Defaults -> Template -> Request -> Necessary Settings (# of shards, uuid, etc)
            indexSettingsBuilder.put(additionalIndexSettings.build());
            indexSettingsBuilder.put(templateSettings.build());
        }

        // now, put the request settings, so they override templates
        indexSettingsBuilder.put(requestSettings.build());

        if (indexSettingsBuilder.get(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey()) == null) {
            final DiscoveryNodes nodes = currentState.nodes();
            final Version createdVersion = Version.min(Version.CURRENT, nodes.getSmallestNonClientNodeVersion());
            indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), createdVersion);
        }
        if (INDEX_NUMBER_OF_SHARDS_SETTING.exists(indexSettingsBuilder) == false) {
            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, INDEX_NUMBER_OF_SHARDS_SETTING.get(settings));
        }
        if (INDEX_NUMBER_OF_REPLICAS_SETTING.exists(indexSettingsBuilder) == false
            || indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, clusterSettings.get(DEFAULT_REPLICA_COUNT_SETTING));
        }
        if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
            indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
        }

        if (indexSettingsBuilder.get(SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());
        }
        indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, request.getProvidedName());
        indexSettingsBuilder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        updateReplicationStrategy(indexSettingsBuilder, request.settings(), settings, combinedTemplateSettings, clusterSettings);
        updateRemoteStoreSettings(indexSettingsBuilder, currentState, clusterSettings, settings, request.index());
        updatePluggableDataFormatSettings(indexSettingsBuilder, clusterSettings, request.index());

        if (sourceMetadata != null) {
            assert request.resizeType() != null;
            prepareResizeIndexSettings(
                currentState,
                indexSettingsBuilder,
                request.recoverFrom(),
                request.index(),
                request.resizeType(),
                request.copySettings(),
                indexScopedSettings
            );
        }

        List<String> validationErrors = new ArrayList<>();
        validateIndexReplicationTypeSettings(indexSettingsBuilder.build(), clusterSettings).ifPresent(validationErrors::add);
        validatePluggableDataFormatSettings(indexSettingsBuilder.build(), clusterSettings, request.index()).ifPresent(
            validationErrors::add
        );
        validateErrors(request.index(), validationErrors);

        Settings indexSettings = indexSettingsBuilder.build();
        /*
         * We can not validate settings until we have applied templates, otherwise we do not know the actual settings
         * that will be used to create this index.
         */
        shardLimitValidator.validateShardLimit(request.index(), indexSettings, currentState);
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexSettings) == false
            && IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings).onOrAfter(Version.V_2_0_0)) {
            throw new IllegalArgumentException(
                "Creating indices with soft-deletes disabled is no longer supported. "
                    + "Please do not specify a value for setting [index.soft_deletes.enabled]."
            );
        }
        validateTranslogRetentionSettings(indexSettings);
        validateStoreTypeSettings(indexSettings);
        validateRefreshIntervalSettings(request.settings(), clusterSettings);
        validateTranslogFlushIntervalSettingsForCompositeIndex(request.settings(), clusterSettings);
        validateTranslogDurabilitySettings(request.settings(), clusterSettings, settings);
        validateSearchOnlyReplicasSettings(indexSettings);
        validateIndexTotalPrimaryShardsPerNodeSetting(indexSettings);
        return indexSettings;
    }

    private static void validateSearchOnlyReplicasSettings(Settings indexSettings) {
        if (INDEX_NUMBER_OF_SEARCH_REPLICAS_SETTING.exists(indexSettings) && indexSettings.get(SETTING_NUMBER_OF_SEARCH_REPLICAS) != null) {
            if (INDEX_NUMBER_OF_SEARCH_REPLICAS_SETTING.get(indexSettings) > 0
                && Boolean.parseBoolean(indexSettings.get(SETTING_REMOTE_STORE_ENABLED)) == false) {
                throw new IllegalArgumentException(
                    "To set " + SETTING_NUMBER_OF_SEARCH_REPLICAS + ", " + SETTING_REMOTE_STORE_ENABLED + " must be set to true"
                );
            }
        }
    }

    /**
     * Validates ingestion source settings for version compatibility and mapper settings correctness.
     * In a mixed cluster, older nodes may not recognize newer mapper types (e.g., field_mapping),
     * which would cause failures when those nodes try to initialize the ingestion engine.
     * Also validates that mapper_settings keys are recognized for the configured mapper_type.
     */
    static void validateIngestionSourceSettings(Settings settings, ClusterState state) {
        // Partition strategy validation. The setting key itself was introduced in V_3_7_0; reject any explicit
        // value (including [simple], the default) on mixed clusters where some nodes don't recognize the key.
        // And in that case the index metadata replicated to older nodes would carry unknown settings.
        // Also, older nodes would silently fall back to the default mapping while the user configured
        // a different strategy (e.g., modulo), which might cause correctness issues.
        if (IndexMetadata.INGESTION_SOURCE_PARTITION_STRATEGY_SETTING.exists(settings)) {
            Version minNodeVersion = state.nodes().getMinNodeVersion();
            if (minNodeVersion.before(Version.V_3_7_0)) {
                throw new IllegalArgumentException(
                    "index.ingestion_source.source_partition_strategy requires all nodes in the cluster to be on version ["
                        + Version.V_3_7_0
                        + "] or later, but the minimum node version is ["
                        + minNodeVersion
                        + "]"
                );
            }
            // TODO: For source_partition_strategy=simple, surface a warning when numSourcePartitions > numShards
            // (excess source partitions are silently never consumed) and an error when
            // numSourcePartitions < numShards (shards beyond numSourcePartitions-1 fail to initialize).
            // Requires consumerFactory.getSourcePartitionCount() which is added in a follow-up PR
            // (multi-partition consumer factory). The check will be wired here once available.
        }

        if (IndexMetadata.INGESTION_SOURCE_MAPPER_TYPE_SETTING.exists(settings) == false) {
            return;
        }

        IngestionMessageMapper.MapperType mapperType = IndexMetadata.INGESTION_SOURCE_MAPPER_TYPE_SETTING.get(settings);
        Map<String, Object> mapperSettings = IndexMetadata.INGESTION_SOURCE_MAPPER_SETTINGS.getAsMap(settings);

        // Version check for mixed cluster compatibility
        if (mapperType == IngestionMessageMapper.MapperType.FIELD_MAPPING) {
            Version minNodeVersion = state.nodes().getMinNodeVersion();
            if (minNodeVersion.before(Version.V_3_6_0)) {
                throw new IllegalArgumentException(
                    "mapper_type [field_mapping] requires all nodes in the cluster to be on version ["
                        + Version.V_3_6_0
                        + "] or later, but the minimum node version is ["
                        + minNodeVersion
                        + "]"
                );
            }
        }

        // Settings validation to the mapper
        IngestionMessageMapper.validateSettings(mapperType, mapperSettings);
    }

    /**
     * Updates index settings to set replication strategy by default based on cluster level settings or remote store
     * node attributes
     * @param settingsBuilder index settings builder to be updated with relevant settings
     * @param requestSettings settings passed in during index create request
     * @param clusterSettings cluster level settings
     * @param combinedTemplateSettings combined template settings which satisfy the index
     */
    public static void updateReplicationStrategy(
        Settings.Builder settingsBuilder,
        Settings requestSettings,
        Settings nodeSettings,
        Settings combinedTemplateSettings,
        ClusterSettings clusterSettings
    ) {
        // The replication setting is applied in the following order:
        // 1. Strictly SEGMENT if cluster is undergoing remote store migration
        // 2. Explicit index creation request parameter
        // 3. Template property for replication type
        // 4. Replication type according to cluster level settings
        // 5. Defaults to segment if remote store attributes on the cluster
        // 6. Default cluster level setting

        final ReplicationType indexReplicationType;
        if (isMigratingToRemoteStore(clusterSettings)) {
            indexReplicationType = ReplicationType.SEGMENT;
        } else if (INDEX_REPLICATION_TYPE_SETTING.exists(requestSettings)) {
            indexReplicationType = INDEX_REPLICATION_TYPE_SETTING.get(requestSettings);
        } else if (combinedTemplateSettings != null && INDEX_REPLICATION_TYPE_SETTING.exists(combinedTemplateSettings)) {
            indexReplicationType = INDEX_REPLICATION_TYPE_SETTING.get(combinedTemplateSettings);
        } else if (CLUSTER_REPLICATION_TYPE_SETTING.exists(nodeSettings)) {
            indexReplicationType = CLUSTER_REPLICATION_TYPE_SETTING.get(nodeSettings);
        } else if (isRemoteDataAttributePresent(nodeSettings)) {
            indexReplicationType = ReplicationType.SEGMENT;
        } else {
            indexReplicationType = CLUSTER_REPLICATION_TYPE_SETTING.getDefault(nodeSettings);
        }
        settingsBuilder.put(SETTING_REPLICATION_TYPE, indexReplicationType);
    }

    /**
     * Updates index settings to enable remote store by default based on node attributes
     * @param settingsBuilder index settings builder to be updated with relevant settings
     * @param clusterState state of cluster
     * @param clusterSettings cluster level settings
     * @param nodeSettings node level settings
     * @param indexName name of index
     */
    public static void updateRemoteStoreSettings(
        Settings.Builder settingsBuilder,
        ClusterState clusterState,
        ClusterSettings clusterSettings,
        Settings nodeSettings,
        String indexName
    ) {
        if ((isRemoteDataAttributePresent(nodeSettings)
            && clusterSettings.get(REMOTE_STORE_COMPATIBILITY_MODE_SETTING).equals(RemoteStoreNodeService.CompatibilityMode.STRICT))
            || isMigratingToRemoteStore(clusterSettings)) {
            String segmentRepo, translogRepo;

            Optional<DiscoveryNode> remoteNode = clusterState.nodes()
                .getNodes()
                .values()
                .stream()
                .filter(DiscoveryNode::isRemoteStoreNode)
                .findFirst();

            if (remoteNode.isPresent()) {
                segmentRepo = RemoteStoreNodeAttribute.getSegmentRepoName(remoteNode.get().getAttributes());
                translogRepo = RemoteStoreNodeAttribute.getTranslogRepoName(remoteNode.get().getAttributes());
                if (segmentRepo != null) {
                    settingsBuilder.put(SETTING_REMOTE_STORE_ENABLED, true).put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, segmentRepo);
                    if (translogRepo != null) {
                        settingsBuilder.put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, translogRepo);
                    } else if (isMigratingToRemoteStore(clusterSettings)) {
                        ValidationException validationException = new ValidationException();
                        validationException.addValidationErrors(
                            Collections.singletonList(
                                "Cluster is migrating to remote store but remote translog is not configured, failing index creation"
                            )
                        );
                        throw new IndexCreationException(indexName, validationException);
                    }
                } else {
                    ValidationException validationException = new ValidationException();
                    validationException.addValidationErrors(
                        Collections.singletonList("Cluster is migrating to remote store but no remote node found, failing index creation")
                    );
                    throw new IndexCreationException(indexName, validationException);
                }
            }
        }
    }

    /**
     * Stamps the cluster-scope defaults for the pluggable data-format index settings into the
     * index metadata at creation time when no explicit override is supplied. No-op when the
     * pluggable data-format feature flag is disabled or the index matches the allowlist.
     */
    public static void updatePluggableDataFormatSettings(
        Settings.Builder settingsBuilder,
        ClusterSettings clusterSettings,
        String indexName
    ) {
        if (FeatureFlags.isEnabled(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG) == false) {
            return;
        }

        if (isAllowedForPluggableDataFormat(indexName, clusterSettings)) {
            return;
        }

        final Settings current = settingsBuilder.build();

        if (IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.exists(current) == false) {
            settingsBuilder.put(
                IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(),
                clusterSettings.get(IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_ENABLED_SETTING)
            );
        }

        if (IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.exists(current) == false) {
            settingsBuilder.put(
                IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(),
                clusterSettings.get(IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_VALUE_SETTING)
            );
        }
    }

    public static void validateStoreTypeSettings(Settings settings) {
        // deprecate simplefs store type:
        if (IndexModule.Type.SIMPLEFS.match(IndexModule.INDEX_STORE_TYPE_SETTING.get(settings))) {
            DEPRECATION_LOGGER.deprecate(
                "store_type_setting",
                "[simplefs] is deprecated and will be removed in 2.0. Use [niofs], which offers equal or better performance, "
                    + "or other file systems instead."
            );
        }
    }

    /**
     * Calculates the number of routing shards based on the configured value in indexSettings or if recovering from another index
     * it will return the value configured for that index.
     */
    static int getIndexNumberOfRoutingShards(Settings indexSettings, @Nullable IndexMetadata sourceMetadata) {
        final int numTargetShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);
        final Version indexVersionCreated = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings);
        final int routingNumShards;
        if (sourceMetadata == null || sourceMetadata.getNumberOfShards() == 1) {
            // in this case we either have no index to recover from or
            // we have a source index with 1 shard and without an explicit split factor
            // or one that is valid in that case we can split into whatever and auto-generate a new factor.
            if (indexSettings.get(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey()) != null) {
                routingNumShards = IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(indexSettings);
            } else {
                routingNumShards = calculateNumRoutingShards(numTargetShards, indexVersionCreated);
            }
        } else {
            assert IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(indexSettings) == false
                : "index.number_of_routing_shards should not be present on the target index on resize";
            routingNumShards = sourceMetadata.getRoutingNumShards();
        }
        return routingNumShards;
    }

    /**
     * Validate and resolve the aliases explicitly set for the index, together with the ones inherited from the specified
     * templates.
     * <p>
     * The template mappings are applied in the order they are encountered in the list (clients should make sure the lower index, closer
     * to the head of the list, templates have the highest {@link IndexTemplateMetadata#order()})
     *
     * @return the list of resolved aliases, with the explicitly provided aliases occurring first (having a higher priority) followed by
     * the ones inherited from the templates
     */
    public static List<AliasMetadata> resolveAndValidateAliases(
        String index,
        Set<Alias> aliases,
        List<Map<String, AliasMetadata>> templateAliases,
        Metadata metadata,
        AliasValidator aliasValidator,
        NamedXContentRegistry xContentRegistry,
        QueryShardContext queryShardContext
    ) {
        List<AliasMetadata> resolvedAliases = new ArrayList<>();
        for (Alias alias : aliases) {
            aliasValidator.validateAlias(alias, index, metadata);
            if (Strings.hasLength(alias.filter())) {
                aliasValidator.validateAliasFilter(alias.name(), alias.filter(), queryShardContext, xContentRegistry);
            }
            AliasMetadata aliasMetadata = AliasMetadata.builder(alias.name())
                .filter(alias.filter())
                .indexRouting(alias.indexRouting())
                .searchRouting(alias.searchRouting())
                .writeIndex(alias.writeIndex())
                .isHidden(alias.isHidden())
                .build();
            resolvedAliases.add(aliasMetadata);
        }

        Map<String, AliasMetadata> templatesAliases = new HashMap<>();
        for (Map<String, AliasMetadata> templateAliasConfig : templateAliases) {
            // handle aliases
            for (Map.Entry<String, AliasMetadata> entry : templateAliasConfig.entrySet()) {
                AliasMetadata aliasMetadata = entry.getValue();
                // if an alias with same name came with the create index request itself,
                // ignore this one taken from the index template
                if (aliases.contains(new Alias(aliasMetadata.alias()))) {
                    continue;
                }
                // if an alias with same name was already processed, ignore this one
                if (templatesAliases.containsKey(entry.getKey())) {
                    continue;
                }

                // Allow templatesAliases to be templated by replacing a token with the
                // name of the index that we are applying it to
                if (aliasMetadata.alias().contains("{index}")) {
                    String templatedAlias = aliasMetadata.alias().replace("{index}", index);
                    aliasMetadata = AliasMetadata.newAliasMetadata(aliasMetadata, templatedAlias);
                }

                aliasValidator.validateAliasMetadata(aliasMetadata, index, metadata);
                if (aliasMetadata.filter() != null) {
                    aliasValidator.validateAliasFilter(
                        aliasMetadata.alias(),
                        aliasMetadata.filter().uncompressed(),
                        queryShardContext,
                        xContentRegistry
                    );
                }
                templatesAliases.put(aliasMetadata.alias(), aliasMetadata);
                resolvedAliases.add((aliasMetadata));
            }
        }
        return resolvedAliases;
    }

    /**
     * Creates the index into the cluster state applying the provided blocks. The final cluster state will contain an updated routing
     * table based on the live nodes.
     */
    /**
     * Creates an ordinary index, one whose creation is the cluster state update itself.
     *
     * <p>Deliberately refuses a gated index rather than dropping its descriptor write on the floor. A gated
     * creation's write is the creation, so somebody has to await it, and a sink that silently discarded it
     * would reinstate exactly the defect T17 and T23 measured: an acknowledgement that means nothing and a
     * name with no uniqueness. Callers that can create a gated index must use the six argument form and
     * carry the future to whoever answers the client.
     */
    /**
     * Fails a creation that reached the gated branch on the state update thread, instead of blocking there.
     *
     * <p>T49 removed the way that used to happen -- admission now resolves templates, so an index gated only
     * by one is admitted off-thread like any other -- and this is the tripwire for the next way. Admission is
     * an approximation by design, and the property it protects is not one to hold by care: a blocking store
     * write here occupies the thread that serialises every cluster state update, and the write can submit an
     * update of its own and then wait for the thread it is standing on.
     *
     * <p><b>Refusing rather than asserting, because assertions are off in production.</b> A rejected creation
     * is visible, attributable and retryable. A stalled cluster manager presents as everything else being
     * broken, which is how the same defect was found the last two times.
     */
    private static void refuseToWriteAMappingFromTheClusterStateThread(IndexMetadata indexMetadata) {
        // Phase C4b of core-pluggability-refactor-plan.md: ClusterStateMutationThreads
        // .blockingIsUnsafeOnCurrentThread() replaces AbsentIndexDescriptorSuppliers.blockingIsUnsafeHere()
        // here -- the exact same thread-name list, already generalized in Phase C4a specifically so this
        // kind of duplicate list (this call site's own comment used to warn about exactly that risk) would
        // have one place to live instead of two that could drift.
        if (org.opensearch.cluster.ClusterStateMutationThreads.blockingIsUnsafeOnCurrentThread()) {
            String thread = Thread.currentThread().getName();
            throw new IllegalStateException(
                "index ["
                    + indexMetadata.getIndex().getName()
                    + "] is gated and declares a mapping, but reached the write on ["
                    + thread
                    + "], where the store's blocking write would occupy the thread that serialises cluster "
                    + "state updates. Either admission failed to send this creation off that thread, or it "
                    + "arrived through a door that has no admission check: auto-creation, rollover and data "
                    + "stream creation all reach this from inside a cluster state task. Refusing costs this "
                    + "request; blocking would cost the cluster."
            );
        }
    }

    static ClusterState clusterStateCreateIndex(
        ClusterState currentState,
        Set<ClusterBlock> clusterBlocks,
        IndexMetadata indexMetadata,
        BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable,
        BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer
    ) {
        return clusterStateCreateIndex(currentState, clusterBlocks, indexMetadata, rerouteRoutingTable, metadataTransformer, write -> {
            throw new IllegalStateException(
                "index ["
                    + indexMetadata.getIndex().getName()
                    + "] is gated, so its descriptor write is its creation and must be awaited. Use the "
                    + "clusterStateCreateIndex overload that accepts a descriptor write sink."
            );
        });
    }

    static ClusterState clusterStateCreateIndex(
        ClusterState currentState,
        Set<ClusterBlock> clusterBlocks,
        IndexMetadata indexMetadata,
        BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable,
        BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer,
        java.util.function.Consumer<java.util.concurrent.CompletableFuture<Boolean>> descriptorWrite
    ) {
        // Area H's third phase. When the gate is open for this index, creation records a descriptor and
        // writes nothing to cluster state: no metadata entry, no routing entry, no publication, and none
        // of the O(total indices) rebuild that S20 measured at 69 ms per change at fifty thousand
        // indices. The descriptor write is the creation, and it is the thing that must succeed.
        //
        // Failure semantics invert from H2b's here. During dual write a lost descriptor cost a
        // comparison; now it costs the index, so publish is required to report that someone was
        // listening rather than being allowed to no-op.
        //
        // Phase D2 of core-pluggability-refactor-plan.md (final slice): IndexCreationStrategyRegistry
        // .skipsClusterState(...) replaces DescriptorOnlyCreation.skipsClusterState(...) directly here --
        // the same predicate, discovered through the SPI instead of the static registry. Deliberately not
        // IndexCreationStrategyRegistry.claims(...): that answers a different, broader question (is this
        // name/request admitted to the strategy's plane at all), while this branch needs the strategy's own
        // narrower, post-build gate (placement ownership, descriptor representability -- see
        // IndexCreationStrategy#skipsClusterState's own javadoc for why the two can disagree). Everything
        // below this line -- the nine lines T18/T23/T17/T49/T52/W4 are load-bearing for -- is unchanged.
        if (IndexCreationStrategyRegistry.skipsClusterState(indexMetadata)) {
            // T18. The descriptor write is the creation, so it goes through createGated rather than
            // publish: op_type=create makes it atomic against a competing creation (T23 measured eight
            // concurrent creations of one name all acknowledged without it), and the future carries the
            // outcome so the acknowledgement can wait for it (T17 measured an acknowledged creation whose
            // write could not possibly have landed).
            //
            // The cluster state thread does not wait here. It hands the future to the task, which defers
            // its response until the write completes, which is what keeps W4's deadlock closed: the thread
            // that would have to supply a cluster state for this write to route is this one.
            // The mapping first, because the descriptor is the acknowledgement. A descriptor carries a
            // mapping generation and not a mapping, so declared fields have to reach MappingGenerationStore
            // or they are lost -- which is what DescriptorRepresentable had to refuse the whole index for.
            // Writing them here is what lets it stop refusing.
            //
            // Ordered before the descriptor write deliberately. If this fails, the creation fails and no
            // descriptor exists, so neither does the index. The reverse order would leave a name that
            // resolves to an index whose declared fields are missing, which is the same silent loss wearing
            // a different shape.
            //
            // Blocking here is safe only where admission ran, and T49 is the record of what that sentence
            // used to hide. It said every path reaching this branch came through createGatedIndex on
            // GENERIC, because the admission check reads the gating setting from the request. Two things
            // were wrong with that. An index gated by a template says nothing in its request, so it took
            // the ordinary road and reached this line on the state update thread; admission is now given
            // template-merged settings, which closes that one. And createIndex is not the only door:
            // auto-creation, rollover and data stream creation call applyCreateIndexRequest from inside a
            // cluster state task, where no admission check runs and none can. The refusal below is what
            // stands in for the argument, because the argument has been wrong twice.
            //
            // Initial creation-time mappings are carried directly in IndexDescriptor.from(indexMetadata)
            // and written atomically to object storage by IndexDescriptorPublisher.createGated(indexMetadata),
            // avoiding system index round-trips entirely.
            //
            // The refusal itself: this comment used to describe it without the call actually being here,
            // which is the defect Phase A1 of the core-pluggability-refactor-plan.md found and closed. A
            // creation reaching this branch from createGatedIndex's GENERIC executor is fine; one reaching it
            // from inside a cluster state task (auto-creation, rollover, data stream creation) is exactly the
            // W4 deadlock this method exists to refuse instead of hitting.
            refuseToWriteAMappingFromTheClusterStateThread(indexMetadata);
            java.util.concurrent.CompletableFuture<Boolean> write = IndexDescriptorPublisher.createGated(indexMetadata);
            if (write == null) {
                throw new IllegalStateException(
                    "index ["
                        + indexMetadata.getIndex().getName()
                        + "] is configured to skip its cluster state entry, but no descriptor creator is "
                        + "installed, so creating it would leave no record of it anywhere"
                );
            }
            descriptorWrite.accept(write);
            if (metadataTransformer != null) {
                Metadata.Builder builder = Metadata.builder(currentState.metadata());
                metadataTransformer.accept(builder, indexMetadata);
                return ClusterState.builder(currentState).metadata(builder.build()).build();
            }
            return currentState;
        }

        String indexName = indexMetadata.getIndex().getName();

        // The other half of closing the two-plane name collision, and the half that has to live here.
        //
        // DescriptorGate#gatable refuses to gate a name outside the serverless namespace, so no name out
        // there can be held by a descriptor alone. This refuses a name inside it a cluster state entry, so
        // no name in here can be held by cluster state. Each name has exactly one authority. Neither
        // authority has to consult the other -- which is what made the collision unfixable where it was
        // found, because the consulting would have to be a blocking descriptor read on this very thread, the
        // deadlock W4 paid for. A string comparison is total, needs no store, and is safe anywhere.
        //
        // A refusal rather than the fallback that used to be here. An admitted creation the gate declines
        // once had an ordinary road to take, and taking it is exactly how an ordinary index came to hold a
        // name a live descriptor already held. In the namespace there is no such road: an index that cannot
        // be gated cannot exist under this name, and the caller is told which feature stopped it rather than
        // being handed a differently-shaped index than it asked for.
        //
        // Phase D2 of core-pluggability-refactor-plan.md (final slice): IndexCreationStrategyRegistry
        // .claims(indexName) replaces DescriptorOnlyCreation.isRegistered() && namesAServerlessIndex(...) --
        // the same predicate (see IndexCreationStrategy#claims(String)'s own javadoc for why the name-only
        // overload, not the request-taking one, is what belongs here: this branch runs after the index is
        // already built, with no CreateIndexClusterStateUpdateRequest in scope). The error text's namespace
        // description is now sourced from the registered strategy too, instead of interpolating
        // DescriptorOnlyCreation.SERVERLESS_NAME_PREFIX directly -- the message-text half of the same
        // core-shouldn't-name-the-plugin's-vocabulary problem the boolean check itself already had.
        if (IndexCreationStrategyRegistry.claims(indexName)) {
            String reason = DescriptorRepresentable.whyNotRepresentable(indexMetadata);
            throw new IllegalArgumentException(
                "index ["
                    + indexName
                    + "] is in "
                    + IndexCreationStrategyRegistry.describeClaimedNamespace()
                    + " and so may not have a cluster state entry, but it could not be gated"
                    + (reason == null ? "" : ": " + reason)
                    + ". Create it under a name outside that namespace, or drop what makes it "
                    + "unrepresentable as a descriptor."
            );
        }

        Metadata.Builder builder = Metadata.builder(currentState.metadata()).put(indexMetadata, false);
        if (metadataTransformer != null) {
            metadataTransformer.accept(builder, indexMetadata);
        }
        Metadata newMetadata = builder.build();

        ClusterBlocks.Builder blocks = createClusterBlocksBuilder(currentState, indexName, clusterBlocks);
        blocks.updateBlocks(indexMetadata);

        ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metadata(newMetadata).build();

        // An index whose placement is computed publishes no routing entry: every node derives the same
        // answer from the node list, so publishing it would be state that says nothing new and has to be
        // diffed on every cluster state change. Skipping publication is what makes the supplier
        // reachable at all -- with an entry published, it would never be consulted.
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable());
        // Phase C4b of core-pluggability-refactor-plan.md: updatedState.routingTable().shouldPublishRouting(...)
        // replaces AbsentIndexRoutingSuppliers.shouldPublishRouting(...) here -- same predicate, discovered
        // through the resolver attached to this state's own routing table.
        if (updatedState.routingTable().shouldPublishRouting(updatedState.metadata().index(indexName))) {
            routingTableBuilder.addAsNew(updatedState.metadata().index(indexName));
        } else {
            // Creation is the assignment, so creation sets the primary term. A term is normally bumped by
            // the cluster manager when it assigns a primary, and for an index the allocator never touches
            // it would stay at zero. Zero is not a legal term: activatePrimaryMode fails adding the peer
            // recovery retention lease with "primary term must be positive but was [0]", and the shard is
            // failed and removed after recovery. Setting it here rather than on the node keeps every later
            // reader agreeing, which the node-local version did not: the shard is constructed from
            // metadata, so a node that substituted its own term tripped "term is only increased as part of
            // primary promotion" instead.
            updatedState = ClusterState.builder(updatedState)
                .metadata(
                    Metadata.builder(updatedState.metadata()).put(withInitialPrimaryTerms(updatedState.metadata().index(indexName)), true)
                )
                .build();
        }
        updatedState = ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build();
        return rerouteRoutingTable.apply(updatedState, "index [" + indexName + "] created");
    }

    /**
     * The same index metadata with every shard's primary term at least one.
     *
     * <p>Only for indices whose routing is not published. A term orders primary failovers, and under
     * computed placement the lease in {@code ShardHead} is the authority on who may write, so one is a
     * legal starting value rather than a meaningful one. Failover, when it arrives, has to take its term
     * from the lease.
     */
    private static IndexMetadata withInitialPrimaryTerms(IndexMetadata indexMetadata) {
        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
        for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            if (indexMetadata.primaryTerm(shardId) == 0) {
                builder.primaryTerm(shardId, 1);
            }
        }
        return builder.build();
    }

    static IndexMetadata buildIndexMetadata(
        String indexName,
        List<AliasMetadata> aliases,
        Supplier<DocumentMapper> documentMapperSupplier,
        Settings indexSettings,
        int routingNumShards,
        @Nullable IndexMetadata sourceMetadata,
        boolean isSystem,
        Map<String, DiffableStringMap> customData,
        Context context
    ) {
        IndexMetadata.Builder indexMetadataBuilder = createIndexMetadataBuilder(indexName, sourceMetadata, indexSettings, routingNumShards);
        indexMetadataBuilder.system(isSystem);
        // now, update the mappings with the actual source
        Map<String, MappingMetadata> mappingsMetadata = new HashMap<>();
        DocumentMapper mapper = documentMapperSupplier.get();
        if (mapper != null) {
            MappingMetadata mappingMd = new MappingMetadata(mapper);
            mappingsMetadata.put(mapper.type(), mappingMd);
        }

        for (MappingMetadata mappingMd : mappingsMetadata.values()) {
            indexMetadataBuilder.putMapping(mappingMd);
        }

        // apply the aliases in reverse order as the lower index ones have higher order
        for (int i = aliases.size() - 1; i >= 0; i--) {
            indexMetadataBuilder.putAlias(aliases.get(i));
        }

        for (Map.Entry<String, DiffableStringMap> entry : customData.entrySet()) {
            indexMetadataBuilder.putCustom(entry.getKey(), entry.getValue());
        }

        indexMetadataBuilder.context(context);

        indexMetadataBuilder.state(IndexMetadata.State.OPEN);
        return indexMetadataBuilder.build();
    }

    /**
     * Creates an {@link IndexMetadata.Builder} for the provided index and sets a valid primary term for all the shards if a source
     * index meta data is provided (this represents the case where we're shrinking/splitting an index and the primary term for the newly
     * created index needs to be gte than the maximum term in the source index).
     */
    private static IndexMetadata.Builder createIndexMetadataBuilder(
        String indexName,
        @Nullable IndexMetadata sourceMetadata,
        Settings indexSettings,
        int routingNumShards
    ) {
        final IndexMetadata.Builder builder = IndexMetadata.builder(indexName);
        builder.setRoutingNumShards(routingNumShards);
        builder.settings(indexSettings);

        if (sourceMetadata != null) {
            /*
             * We need to arrange that the primary term on all the shards in the shrunken index is at least as large as
             * the maximum primary term on all the shards in the source index. This ensures that we have correct
             * document-level semantics regarding sequence numbers in the shrunken index.
             */
            final long primaryTerm = IntStream.range(0, sourceMetadata.getNumberOfShards())
                .mapToLong(sourceMetadata::primaryTerm)
                .max()
                .getAsLong();
            for (int shardId = 0; shardId < builder.numberOfShards(); shardId++) {
                builder.primaryTerm(shardId, primaryTerm);
            }
        }
        return builder;
    }

    private static ClusterBlocks.Builder createClusterBlocksBuilder(ClusterState currentState, String index, Set<ClusterBlock> blocks) {
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
        if (!blocks.isEmpty()) {
            for (ClusterBlock block : blocks) {
                blocksBuilder.addIndexBlock(index, block);
            }
        }
        return blocksBuilder;
    }

    private static void updateIndexMappingsAndBuildSortOrder(
        IndexService indexService,
        CreateIndexClusterStateUpdateRequest request,
        List<Map<String, Object>> mappings,
        @Nullable IndexMetadata sourceMetadata,
        List<IndexCreationValidator> indexCreationValidators
    ) throws IOException {
        MapperService mapperService = indexService.mapperService();
        for (Map<String, Object> mapping : mappings) {
            if (mapping.isEmpty() == false) {
                mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mapping, MergeReason.INDEX_TEMPLATE);
            }
        }

        if (mapperService.isCompositeIndexPresent()) {
            CompositeIndexValidator.validate(mapperService, indexService.getCompositeIndexSettings(), indexService.getIndexSettings());
        }

        for (IndexCreationValidator validator : indexCreationValidators) {
            validator.validate(mapperService, indexService.getIndexSettings());
        }

        if (sourceMetadata == null) {
            // now that the mapping is merged we can validate the index sort.
            // we cannot validate for index shrinking since the mapping is empty
            // at this point. The validation will take place later in the process
            // (when all shards are copied in a single place).
            indexService.getIndexSortSupplier().get();
        }
        if (request.dataStreamName() != null) {
            MetadataCreateDataStreamService.validateTimestampFieldMapping(mapperService);
        }
    }

    private static void validateActiveShardCount(ActiveShardCount waitForActiveShards, IndexMetadata indexMetadata) {
        if (waitForActiveShards == ActiveShardCount.DEFAULT) {
            waitForActiveShards = indexMetadata.getWaitForActiveShards();
        }
        if (waitForActiveShards.validate(indexMetadata.getNumberOfReplicas()) == false) {
            throw new IllegalArgumentException(
                "invalid wait_for_active_shards["
                    + waitForActiveShards
                    + "]: cannot be greater than number of shard copies ["
                    + (indexMetadata.getNumberOfReplicas() + 1)
                    + "]"
            );
        }
    }

    private void validate(CreateIndexClusterStateUpdateRequest request, ClusterState state) {
        validateIndexName(request.index(), state);
        validateClaimedNamespaceRequest(request);
        validateIndexSettings(request.index(), request.settings(), forbidPrivateIndexSettings);
        validateContext(request);
        validateIngestionSourceSettings(request.settings(), state);
    }

    /**
     * Refuses a creation in the serverless namespace that asks for something a serverless index cannot be.
     *
     * <p>The name decides the plane, so these can no longer be answered by quietly making the index an
     * ordinary one -- that is the ambiguity the namespace exists to remove, and it is how a name came to be
     * claimable in both planes. Each of these is a condition {@code DescriptorRepresentable} refuses, said
     * at the point the client can still do something about it.
     *
     * <p>Only what the request itself carries. A template that contributes an alias to a name in this
     * namespace is not caught here, and is still decided the old way -- the gate declines and the index
     * keeps a cluster state entry. That is a hole in the contract rather than a correctness one (nothing is
     * written to two places), and closing it means resolving templates during validation, which is a larger
     * change than this.
     */
    private void validateClaimedNamespaceRequest(CreateIndexClusterStateUpdateRequest request) {
        // Phase D2 of core-pluggability-refactor-plan.md: same migration as createIndex()'s own top branch,
        // negated -- see whyATemporaryIndexServiceIsStillNeeded's comment for the De Morgan's equivalence.
        // Deliberately not the larger change the plan's own D2 sketch also names for this method (deleting
        // it entirely in favor of a plugin-owned IndexCreationValidator) -- that changes what gets validated
        // and where; this changes only how the same predicate is discovered.
        //
        // Final D2 slice: the error text below now sources its namespace description from the registered
        // strategy (IndexCreationStrategyRegistry.describeClaimedNamespace()) instead of interpolating
        // DescriptorOnlyCreation.SERVERLESS_NAME_PREFIX directly -- the same message-text fix applied at
        // clusterStateCreateIndex's own two-plane-collision refusal, so core stops naming this plugin's
        // vocabulary in both places that do, not just the boolean check the first D2 slices already moved.
        if (IndexCreationStrategyRegistry.claims(request.index(), request) == false) {
            return;
        }
        final String unsupported;
        if (request.aliases().isEmpty() == false) {
            unsupported = "aliases " + request.aliases().stream().map(alias -> alias.name()).collect(toList());
        } else if (request.context() != null) {
            unsupported = "a context";
        } else if (request.dataStreamName() != null) {
            unsupported = "membership of data stream [" + request.dataStreamName() + "]";
        } else if (request.recoverFrom() != null) {
            unsupported = "being built from [" + request.recoverFrom().getName() + "]";
        } else {
            return;
        }
        throw new IllegalArgumentException(
            "index ["
                + request.index()
                + "] is in "
                + IndexCreationStrategyRegistry.describeClaimedNamespace()
                + ", whose indices keep no cluster state entry, and it requests "
                + unsupported
                + ", which such an index cannot have. Create it outside the namespace, or drop what it "
                + "cannot support."
        );
    }

    public void validateIndexSettings(String indexName, final Settings settings, final boolean forbidPrivateIndexSettings)
        throws IndexCreationException {
        List<String> validationErrors = getIndexSettingsValidationErrors(settings, forbidPrivateIndexSettings, indexName);
        validateIndexReplicationTypeSettings(settings, clusterService.getClusterSettings()).ifPresent(validationErrors::add);
        validatePluggableDataFormatSettings(settings, clusterService.getClusterSettings(), indexName).ifPresent(validationErrors::add);
        validateErrors(indexName, validationErrors);
    }

    private static void validateErrors(String indexName, List<String> validationErrors) {
        if (validationErrors.isEmpty() == false) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new IndexCreationException(indexName, validationException);
        }
    }

    List<String> getIndexSettingsValidationErrors(final Settings settings, final boolean forbidPrivateIndexSettings, String indexName) {
        List<String> validationErrors = getIndexSettingsValidationErrors(settings, forbidPrivateIndexSettings, Optional.of(indexName));
        return validationErrors;
    }

    List<String> getIndexSettingsValidationErrors(
        final Settings settings,
        final boolean forbidPrivateIndexSettings,
        Optional<String> indexName
    ) {
        List<String> validationErrors = validateIndexCustomPath(settings, env.sharedDataDir());
        if (forbidPrivateIndexSettings) {
            validationErrors.addAll(validatePrivateSettingsNotExplicitlySet(settings, indexScopedSettings));
        }
        if (indexName.isEmpty() || indexName.get().charAt(0) != '.') {
            // Apply aware replica balance validation only to non system indices
            int replicaCount = settings.getAsInt(
                IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
                clusterService.getClusterSettings().get(DEFAULT_REPLICA_COUNT_SETTING)
            );
            int searchReplicaCount = settings.getAsInt(SETTING_NUMBER_OF_SEARCH_REPLICAS, 0);
            AutoExpandReplicas autoExpandReplica = AutoExpandReplicas.SETTING.get(settings);

            Optional<String> replicaValidationError = awarenessReplicaBalance.validate(replicaCount, autoExpandReplica);
            replicaValidationError.ifPresent(validationErrors::add);
            Optional<String> searchReplicaValidationError = awarenessReplicaBalance.validate(
                searchReplicaCount,
                AutoExpandSearchReplicas.SETTING.get(settings)
            );
            searchReplicaValidationError.ifPresent(validationErrors::add);
        }
        return validationErrors;
    }

    private static List<String> validatePrivateSettingsNotExplicitlySet(Settings settings, IndexScopedSettings indexScopedSettings) {
        List<String> validationErrors = new ArrayList<>();
        for (final String key : settings.keySet()) {
            final Setting<?> setting = indexScopedSettings.get(key);
            if (setting == null) {
                // see: https://github.com/opensearch-project/OpenSearch/issues/1019
                if (!indexScopedSettings.isPrivateSetting(key)) {
                    validationErrors.add("expected [" + key + "] to be private but it was not");
                }
            } else if (setting.isPrivateIndex()) {
                validationErrors.add("private index setting [" + key + "] can not be set explicitly");
            }
        }
        return validationErrors;
    }

    /**
     * Validates that the configured index data path (if any) is a sub-path of the configured shared data path (if any)
     *
     * @param settings the index configured settings
     * @param sharedDataPath the configured `path.shared_data` (if any)
     * @return a list containing validaton errors or an empty list if there aren't any errors
     */
    private static List<String> validateIndexCustomPath(Settings settings, @Nullable Path sharedDataPath) {
        String customPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(settings);
        List<String> validationErrors = new ArrayList<>();
        if (Strings.isEmpty(customPath) == false) {
            if (sharedDataPath == null) {
                validationErrors.add("path.shared_data must be set in order to use custom data paths");
            } else {
                Path resolvedPath = PathUtils.get(new Path[] { sharedDataPath }, customPath);
                if (resolvedPath == null) {
                    validationErrors.add("custom path [" + customPath + "] is not a sub-path of path.shared_data [" + sharedDataPath + "]");
                }
            }
        }
        return validationErrors;
    }

    /**
     * Validates {@code index.replication.type} is matches with cluster level setting {@code cluster.indices.replication.strategy}
     * when {@code cluster.index.restrict.replication.type} is set to true.
     *
     * @param requestSettings settings passed in during index create request
     * @param clusterSettings cluster setting
     */
    private static Optional<String> validateIndexReplicationTypeSettings(Settings requestSettings, ClusterSettings clusterSettings) {
        if (clusterSettings.get(IndicesService.CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING)
            && requestSettings.hasValue(SETTING_REPLICATION_TYPE)
            && requestSettings.get(INDEX_REPLICATION_TYPE_SETTING.getKey())
                .equals(clusterSettings.get(CLUSTER_REPLICATION_TYPE_SETTING).name()) == false) {
            return Optional.of(
                "index setting [index.replication.type] is not allowed to be set as ["
                    + IndicesService.CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING.getKey()
                    + "=true]"
            );
        }
        return Optional.empty();
    }

    /**
     * Validates that {@code index.pluggable.dataformat.enabled} and {@code index.pluggable.dataformat} match the
     * cluster-level defaults {@code cluster.pluggable.dataformat.enabled} and
     * {@code cluster.pluggable.dataformat} when
     * {@code cluster.restrict.pluggable.dataformat} is set to true.
     *
     * @param requestSettings settings resulting from merging request, templates, and cluster-level defaults
     * @param clusterSettings cluster setting
     * @param indexName name of the index being created
     */
    private static Optional<String> validatePluggableDataFormatSettings(
        Settings requestSettings,
        ClusterSettings clusterSettings,
        String indexName
    ) {
        if (FeatureFlags.isEnabled(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG) == false) {
            return Optional.empty();
        }
        if (clusterSettings.get(IndicesService.CLUSTER_RESTRICT_PLUGGABLE_DATAFORMAT_SETTING) == false) {
            return Optional.empty();
        }
        if (isAllowedForPluggableDataFormat(indexName, clusterSettings)) {
            return Optional.empty();
        }

        if (requestSettings.hasValue(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey())
            && IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.get(requestSettings)
                .equals(clusterSettings.get(IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_ENABLED_SETTING)) == false) {
            return Optional.of(
                "index setting ["
                    + IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey()
                    + "] cannot differ from cluster default ["
                    + clusterSettings.get(IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_ENABLED_SETTING)
                    + "] when ["
                    + IndicesService.CLUSTER_RESTRICT_PLUGGABLE_DATAFORMAT_SETTING.getKey()
                    + "=true]"
            );
        }

        if (requestSettings.hasValue(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey())
            && IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.get(requestSettings)
                .equals(clusterSettings.get(IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_VALUE_SETTING)) == false) {
            return Optional.of(
                "index setting ["
                    + IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey()
                    + "] cannot differ from cluster default ["
                    + clusterSettings.get(IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_VALUE_SETTING)
                    + "] when ["
                    + IndicesService.CLUSTER_RESTRICT_PLUGGABLE_DATAFORMAT_SETTING.getKey()
                    + "=true]"
            );
        }
        return Optional.empty();
    }

    /**
     * Returns {@code true} if the given index name matches any prefix in the
     * {@code cluster.pluggable.dataformat.restrict.allowlist} setting, meaning it should bypass
     * pluggable data-format default-stamping and restrict validation.
     */
    private static boolean isAllowedForPluggableDataFormat(String indexName, ClusterSettings clusterSettings) {
        List<String> allowlist = clusterSettings.get(IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_RESTRICT_ALLOWLIST);
        return allowlist.stream().anyMatch(indexName::startsWith);
    }

    /**
     * Validates the settings and mappings for shrinking an index.
     *
     * @return the list of nodes at least one instance of the source index shards are allocated
     */
    static List<String> validateShrinkIndex(ClusterState state, String sourceIndex, String targetIndexName, Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexName, targetIndexSettings);
        assert IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings);
        IndexMetadata.selectShrinkShards(0, sourceMetadata, IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));

        if (sourceMetadata.getNumberOfShards() == 1) {
            throw new IllegalArgumentException("can't shrink an index with only one shard");
        }

        // now check that index is all on one node
        final IndexRoutingTable table = state.routingTable().index(sourceIndex);
        Map<String, AtomicInteger> nodesToNumRouting = new HashMap<>();
        int numShards = sourceMetadata.getNumberOfShards();
        // No routing entry means no started shards, so no node holds a full copy. Falling through leaves
        // nodesToAllocateOn empty and produces the existing, clearer error below.
        if (table != null) {
            for (ShardRouting routing : table.shardsWithState(ShardRoutingState.STARTED)) {
                nodesToNumRouting.computeIfAbsent(routing.currentNodeId(), (s) -> new AtomicInteger(0)).incrementAndGet();
            }
        }
        List<String> nodesToAllocateOn = new ArrayList<>();
        for (Map.Entry<String, AtomicInteger> entries : nodesToNumRouting.entrySet()) {
            int numAllocations = entries.getValue().get();
            assert numAllocations <= numShards : "wait what? " + numAllocations + " is > than num shards " + numShards;
            if (numAllocations == numShards) {
                nodesToAllocateOn.add(entries.getKey());
            }
        }
        if (nodesToAllocateOn.isEmpty()) {
            throw new IllegalStateException("index " + sourceIndex + " must have all shards allocated on the same node to shrink index");
        }
        return nodesToAllocateOn;
    }

    static void validateSplitIndex(ClusterState state, String sourceIndex, String targetIndexName, Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexName, targetIndexSettings);
        IndexMetadata.selectSplitShard(0, sourceMetadata, IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static void validateCloneIndex(ClusterState state, String sourceIndex, String targetIndexName, Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexName, targetIndexSettings);
        IndexMetadata.selectCloneShard(0, sourceMetadata, IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static IndexMetadata validateResize(ClusterState state, String sourceIndex, String targetIndexName, Settings targetIndexSettings) {
        if (state.metadata().hasIndex(targetIndexName)) {
            throw new ResourceAlreadyExistsException(state.metadata().index(targetIndexName).getIndex());
        }
        final IndexMetadata sourceMetadata = state.metadata().index(sourceIndex);
        if (sourceMetadata == null) {
            throw new IndexNotFoundException(sourceIndex);
        }

        IndexAbstraction source = state.metadata().getIndicesLookup().get(sourceIndex);
        assert source != null;
        if (source.getParentDataStream() != null
            && source.getParentDataStream().getWriteIndex().getIndex().equals(sourceMetadata.getIndex())) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "cannot resize the write index [%s] for data stream [%s]",
                    sourceIndex,
                    source.getParentDataStream().getName()
                )
            );
        }

        // ensure write operations on the source index is blocked
        if (state.blocks().indexBlocked(ClusterBlockLevel.WRITE, sourceIndex) == false) {
            throw new IllegalStateException(
                "index " + sourceIndex + " must block write operations to resize index. use \"index.blocks.write=true\""
            );
        }

        if (IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings)) {
            // this method applies all necessary checks ie. if the target shards are less than the source shards
            // of if the source shards are divisible by the number of target shards
            IndexMetadata.getRoutingFactor(
                sourceMetadata.getNumberOfShards(),
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings)
            );
        }
        return sourceMetadata;
    }

    static void prepareResizeIndexSettings(
        final ClusterState currentState,
        final Settings.Builder indexSettingsBuilder,
        final Index resizeSourceIndex,
        final String resizeIntoName,
        final ResizeType type,
        final boolean copySettings,
        final IndexScopedSettings indexScopedSettings
    ) {
        // we use "i.r.a.initial_recovery" rather than "i.r.a.require|include" since we want the replica to allocate right away
        // once we are allocated.
        final String initialRecoveryIdFilter = IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey() + "_id";

        final IndexMetadata sourceMetadata = currentState.metadata().index(resizeSourceIndex.getName());
        if (type == ResizeType.SHRINK) {
            final List<String> nodesToAllocateOn = validateShrinkIndex(
                currentState,
                resizeSourceIndex.getName(),
                resizeIntoName,
                indexSettingsBuilder.build()
            );
            indexSettingsBuilder.put(initialRecoveryIdFilter, Strings.arrayToCommaDelimitedString(nodesToAllocateOn.toArray()));
        } else if (type == ResizeType.SPLIT) {
            validateSplitIndex(currentState, resizeSourceIndex.getName(), resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder.putNull(initialRecoveryIdFilter);
        } else if (type == ResizeType.CLONE) {
            validateCloneIndex(currentState, resizeSourceIndex.getName(), resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder.putNull(initialRecoveryIdFilter);
        } else {
            throw new IllegalStateException("unknown resize type is " + type);
        }

        final Settings.Builder builder = Settings.builder();
        if (copySettings) {
            // copy all settings and non-copyable settings and settings that have already been set (e.g., from the request)
            for (final String key : sourceMetadata.getSettings().keySet()) {
                final Setting<?> setting = indexScopedSettings.get(key);
                if (setting == null) {
                    assert indexScopedSettings.isPrivateSetting(key) : key;
                } else if (setting.getProperties().contains(Setting.Property.NotCopyableOnResize)) {
                    continue;
                }
                // do not override settings that have already been set (for example, from the request)
                if (indexSettingsBuilder.keys().contains(key)) {
                    continue;
                }
                builder.copy(key, sourceMetadata.getSettings());
            }
        } else {
            final Predicate<String> sourceSettingsPredicate = (s) -> (s.startsWith("index.similarity.")
                || s.startsWith("index.analysis.")
                || s.startsWith("index.sort.")
                || s.equals("index.soft_deletes.enabled")) && indexSettingsBuilder.keys().contains(s) == false;
            builder.put(sourceMetadata.getSettings().filter(sourceSettingsPredicate));
        }

        indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), sourceMetadata.getCreationVersion())
            .put(IndexMetadata.SETTING_VERSION_UPGRADED, sourceMetadata.getUpgradedVersion())
            .put(builder.build())
            .put(IndexMetadata.SETTING_ROUTING_PARTITION_SIZE, sourceMetadata.getRoutingPartitionSize())
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), resizeSourceIndex.getName())
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.getKey(), resizeSourceIndex.getUUID());
    }

    /**
     * Returns a default number of routing shards based on the number of shards of the index. The default number of routing shards will
     * allow any index to be split at least once and at most 10 times by a factor of two. The closer the number or shards gets to 1024
     * the less default split operations are supported
     */
    public static int calculateNumRoutingShards(int numShards, Version indexVersionCreated) {
        // only select this automatically for indices that are created on or after 7.0 this will prevent this new behaviour
        // until we have a fully upgraded cluster. Additionally it will make integratin testing easier since mixed clusters
        // will always have the behavior of the min node in the cluster.
        //
        // We use as a default number of routing shards the higher number that can be expressed
        // as {@code numShards * 2^x`} that is less than or equal to the maximum number of shards: 1024.
        int log2MaxNumShards = 10; // logBase2(1024)
        int log2NumShards = 32 - Integer.numberOfLeadingZeros(numShards - 1); // ceil(logBase2(numShards))
        int numSplits = log2MaxNumShards - log2NumShards;
        numSplits = Math.max(1, numSplits); // Ensure the index can be split at least once
        return numShards * 1 << numSplits;
    }

    public static void validateTranslogRetentionSettings(Settings indexSettings) {
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexSettings)) {
            if (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexSettings)
                || IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexSettings)) {
                DEPRECATION_LOGGER.deprecate(
                    "translog_retention",
                    "Translog retention settings "
                        + "[index.translog.retention.age] "
                        + "and [index.translog.retention.size] are deprecated and effectively ignored. "
                        + "They will be removed in a future version."
                );
            }
        }
    }

    /**
     * Validates {@code index.translog.flush_threshold_size} is equal or below the {@code indices.composite_index.translog.max_flush_threshold_size}
     * for composite indices based on {{@code index.composite_index}}
     *
     * @param requestSettings settings passed in during index create/update request
     * @param clusterSettings cluster setting
     */
    public static void validateTranslogFlushIntervalSettingsForCompositeIndex(Settings requestSettings, ClusterSettings clusterSettings) {
        if (StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.exists(requestSettings) == false
            || requestSettings.get(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey()) == null) {
            return;
        }
        ByteSizeValue translogFlushSize = INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.get(requestSettings);
        ByteSizeValue compositeIndexMaxFlushSize = clusterSettings.get(
            CompositeIndexSettings.COMPOSITE_INDEX_MAX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING
        );
        if (translogFlushSize.compareTo(compositeIndexMaxFlushSize) > 0) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "You can configure '%s' with upto '%s' for composite index",
                    INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                    compositeIndexMaxFlushSize
                )
            );
        }
    }

    /**
     * Validates {@code index.translog.flush_threshold_size} is equal or below the {@code indices.composite_index.translog.max_flush_threshold_size}
     * for composite indices based on {{@code index.composite_index}}
     * This is used during update index settings flow
     *
     * @param requestSettings settings passed in during index update request
     * @param clusterSettings cluster setting
     * @param indexSettings index settings
     */
    public static Optional<String> validateTranslogFlushIntervalSettingsForCompositeIndex(
        Settings requestSettings,
        ClusterSettings clusterSettings,
        Settings indexSettings
    ) {
        if (INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.exists(requestSettings) == false
            || requestSettings.get(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey()) == null
            || StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.exists(indexSettings) == false
            || indexSettings.get(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey()) == null) {
            return Optional.empty();
        }
        ByteSizeValue translogFlushSize = INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.get(requestSettings);
        ByteSizeValue compositeIndexMaxFlushSize = clusterSettings.get(
            CompositeIndexSettings.COMPOSITE_INDEX_MAX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING
        );
        if (translogFlushSize.compareTo(compositeIndexMaxFlushSize) > 0) {
            return Optional.of(
                String.format(
                    Locale.ROOT,
                    "You can configure '%s' with upto '%s' for composite index",
                    INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                    compositeIndexMaxFlushSize
                )
            );
        }
        return Optional.empty();
    }

    /**
     * Validates {@code index.refresh_interval} is equal or below the {@code cluster.minimum.index.refresh_interval}.
     *
     * @param requestSettings settings passed in during index create/update request
     * @param clusterSettings cluster setting
     */
    public static void validateRefreshIntervalSettings(Settings requestSettings, ClusterSettings clusterSettings) {
        if (IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.exists(requestSettings) == false
            || requestSettings.get(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()) == null) {
            return;
        }
        TimeValue requestRefreshInterval = IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.get(requestSettings);
        // If the refresh interval supplied is -1, we allow the index to be created because -1 means no periodic refresh.
        if (requestRefreshInterval.millis() == -1) {
            return;
        }
        TimeValue clusterMinimumRefreshInterval = clusterSettings.get(IndicesService.CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING);
        if (requestRefreshInterval.millis() < clusterMinimumRefreshInterval.millis()) {
            throw new IllegalArgumentException(
                "invalid index.refresh_interval ["
                    + requestRefreshInterval
                    + "]: cannot be smaller than cluster.minimum.index.refresh_interval ["
                    + clusterMinimumRefreshInterval
                    + "]"
            );
        }
    }

    /**
     * Validates the {@code index.routing.allocation.total_primary_shards_per_node} setting during index creation.
     * Ensures this setting is only specified for remote store enabled clusters.
     */
    // TODO : Update this check for SegRep to DocRep migration on need basis
    public static void validateIndexTotalPrimaryShardsPerNodeSetting(Settings indexSettings) {
        // Get the setting value
        int indexPrimaryShardsPerNode = INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.get(indexSettings);
        int indexRemoteCapablePrimaryShardsPerNode = INDEX_TOTAL_REMOTE_CAPABLE_PRIMARY_SHARDS_PER_NODE_SETTING.get(indexSettings);

        // If default value (-1), no validation needed
        if (indexPrimaryShardsPerNode == -1 && indexRemoteCapablePrimaryShardsPerNode == -1) {
            return;
        }

        // Check if remote store is enabled
        boolean isRemoteStoreEnabled = IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexSettings);
        if (!isRemoteStoreEnabled) {
            throw new IllegalArgumentException(
                "Setting ["
                    + INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey()
                    + "] or ["
                    + INDEX_TOTAL_REMOTE_CAPABLE_PRIMARY_SHARDS_PER_NODE_SETTING.getKey()
                    + "] can only be used with remote store enabled clusters"
            );
        }
    }

    /**
     * Validates {@code index.translog.durability} is not async if the {@code cluster.remote_store.index.restrict.async-durability} is set to true.
     *
     * @param requestSettings settings passed in during index create/update request
     * @param clusterSettings cluster setting
     */
    static void validateTranslogDurabilitySettings(Settings requestSettings, ClusterSettings clusterSettings, Settings settings) {
        if ((isRemoteDataAttributePresent(settings) == false && isMigratingToRemoteStore(clusterSettings) == false)
            || IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.exists(requestSettings) == false
            || clusterSettings.get(IndicesService.CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING) == false) {
            return;
        }
        Translog.Durability durability = IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.get(requestSettings);
        if (durability.equals(Translog.Durability.ASYNC)) {
            throw new IllegalArgumentException(
                "index setting [index.translog.durability=async] is not allowed as cluster setting ["
                    + IndicesService.CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING.getKey()
                    + "=true]"
            );
        }

    }

    void validateContext(CreateIndexClusterStateUpdateRequest request) {
        final boolean isContextAllowed = FeatureFlags.isEnabled(FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES);

        if (request.context() != null && !isContextAllowed) {
            throw new InvalidIndexContextException(
                request.context().name(),
                request.index(),
                "index specifies a context which cannot be used without enabling: "
                    + SystemTemplatesService.SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED.getKey()
            );
        }

        if (request.context() != null && findContextTemplateName(clusterService.state().metadata(), request.context()) == null) {
            throw new InvalidIndexContextException(
                request.context().name(),
                request.index(),
                "index specifies a context which is not loaded on the cluster."
            );
        }
    }
}
