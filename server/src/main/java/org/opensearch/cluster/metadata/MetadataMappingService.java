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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateTaskListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.compositeindex.CompositeIndexValidator;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.service.ClusterManagerTask.PUT_MAPPING;
import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;

/**
 * Service responsible for submitting mapping changes
 *
 * @opensearch.internal
 */
public class MetadataMappingService {

    private static final Logger logger = LogManager.getLogger(MetadataMappingService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;
    private final ClusterManagerTaskThrottler.ThrottlingKey putMappingTaskKey;

    final RefreshTaskExecutor refreshExecutor = new RefreshTaskExecutor();
    final PutMappingExecutor putMappingExecutor = new PutMappingExecutor();

    @Inject
    public MetadataMappingService(ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.threadPool = threadPool;

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        putMappingTaskKey = clusterService.registerClusterManagerTask(PUT_MAPPING, true);

    }

    static class RefreshTask {
        final String index;
        final String indexUUID;

        RefreshTask(String index, final String indexUUID) {
            this.index = index;
            this.indexUUID = indexUUID;
        }

        @Override
        public String toString() {
            return "[" + index + "][" + indexUUID + "]";
        }
    }

    class RefreshTaskExecutor implements ClusterStateTaskExecutor<RefreshTask> {
        @Override
        public ClusterTasksResult<RefreshTask> execute(ClusterState currentState, List<RefreshTask> tasks) throws Exception {
            ClusterState newClusterState = executeRefresh(currentState, tasks);
            return ClusterTasksResult.<RefreshTask>builder().successes(tasks).build(newClusterState);
        }
    }

    /**
     * Batch method to apply all the queued refresh operations. The idea is to try and batch as much
     * as possible so we won't create the same index all the time for example for the updates on the same mapping
     * and generate a single cluster change event out of all of those.
     */
    ClusterState executeRefresh(final ClusterState currentState, final List<RefreshTask> allTasks) throws Exception {
        // break down to tasks per index, so we can optimize the on demand index service creation
        // to only happen for the duration of a single index processing of its respective events
        Map<String, List<RefreshTask>> tasksPerIndex = new HashMap<>();
        for (RefreshTask task : allTasks) {
            if (task.index == null) {
                logger.debug("ignoring a mapping task of type [{}] with a null index.", task);
            }
            tasksPerIndex.computeIfAbsent(task.index, k -> new ArrayList<>()).add(task);
        }

        boolean dirty = false;
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());

        for (Map.Entry<String, List<RefreshTask>> entry : tasksPerIndex.entrySet()) {
            IndexMetadata indexMetadata = mdBuilder.get(entry.getKey());
            if (indexMetadata == null) {
                // index got deleted on us, ignore...
                logger.debug("[{}] ignoring tasks - index meta data doesn't exist", entry.getKey());
                continue;
            }
            final Index index = indexMetadata.getIndex();
            // the tasks lists to iterate over, filled with the list of mapping tasks, trying to keep
            // the latest (based on order) update mapping one per node
            List<RefreshTask> allIndexTasks = entry.getValue();
            boolean hasTaskWithRightUUID = false;
            for (RefreshTask task : allIndexTasks) {
                if (indexMetadata.isSameUUID(task.indexUUID)) {
                    hasTaskWithRightUUID = true;
                } else {
                    logger.debug("{} ignoring task [{}] - index meta data doesn't match task uuid", index, task);
                }
            }
            if (hasTaskWithRightUUID == false) {
                continue;
            }

            // construct the actual index if needed, and make sure the relevant mappings are there
            boolean removeIndex = false;
            IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
            if (indexService == null) {
                // we need to create the index here, and add the current mapping to it, so we can merge
                indexService = indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
                removeIndex = true;
                indexService.mapperService().merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
            }

            IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
            try {
                boolean indexDirty = refreshIndexMapping(indexService, builder);
                if (indexDirty) {
                    mdBuilder.put(builder);
                    dirty = true;
                }
            } finally {
                if (removeIndex) {
                    indicesService.removeIndex(index, NO_LONGER_ASSIGNED, "created for mapping processing");
                }
            }
        }

        if (!dirty) {
            return currentState;
        }
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    private boolean refreshIndexMapping(IndexService indexService, IndexMetadata.Builder builder) {
        boolean dirty = false;
        String index = indexService.index().getName();
        try {
            MapperService mapperService = indexService.mapperService();
            DocumentMapper mapper = mapperService.documentMapper();
            if (mapper != null) {
                if (mapper.mappingSource().equals(builder.mapping().source()) == false) {
                    dirty = true;
                }
            }
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to refresh-mapping in cluster state", index), e);
        }
        return dirty;
    }

    /**
     * Refreshes mappings if they are not the same between original and parsed version
     */
    public void refreshMapping(final String index, final String indexUUID) {
        final RefreshTask refreshTask = new RefreshTask(index, indexUUID);
        clusterService.submitStateUpdateTask(
            "refresh-mapping [" + index + "]",
            refreshTask,
            ClusterStateTaskConfig.build(Priority.HIGH),
            refreshExecutor,
            (source, e) -> logger.warn(() -> new ParameterizedMessage("failure during [{}]", source), e)
        );
    }

    class PutMappingExecutor implements ClusterStateTaskExecutor<PutMappingClusterStateUpdateRequest> {
        @Override
        public ClusterTasksResult<PutMappingClusterStateUpdateRequest> execute(
            ClusterState currentState,
            List<PutMappingClusterStateUpdateRequest> tasks
        ) throws Exception {
            Map<Index, MapperService> indexMapperServices = new HashMap<>();
            ClusterTasksResult.Builder<PutMappingClusterStateUpdateRequest> builder = ClusterTasksResult.builder();
            try {
                for (PutMappingClusterStateUpdateRequest request : tasks) {
                    try {
                        // A gated index has no metadata entry by design, so getIndexSafe below
                        // would throw and a document carrying a new field would fail. Its mapping lives in
                        // the store instead, and that is handled in putMapping before anything is submitted
                        // here -- doing it in this method did it on the cluster manager's update
                        // thread, and the store blocks.
                        //
                        // Deliberately not left here as a fallback. putMapping is the only submitter of this
                        // executor, so a copy of the branch would be unreachable, and an unreachable copy of
                        // a blocking call on this thread is worse than none: it reads as a safety net while
                        // being the defect. If a gated request ever does arrive here, getIndexSafe fails it
                        // loudly, which is the outcome that gets noticed.
                        assert MappingGenerationStore.isRegistered() == false || isGated(currentState, request) == false
                            : "a gated put-mapping reached the cluster state update thread, which the GENERIC-pool dispatch in putMapping exists to prevent";
                        for (Index index : request.indices()) {
                            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
                            if (indexMapperServices.containsKey(indexMetadata.getIndex()) == false) {
                                MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);
                                indexMapperServices.put(index, mapperService);
                                // add mappings for all types, we need them for cross-type validation
                                mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
                            }
                        }
                        currentState = applyRequest(currentState, request, indexMapperServices);
                        builder.success(request);
                    } catch (Exception e) {
                        builder.failure(request, e);
                    }
                }
                return builder.build(currentState);
            } finally {
                IOUtils.close(indexMapperServices.values());
            }
        }

        @Override
        public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
            return putMappingTaskKey;
        }

        /** Whether every index in this request is absent from cluster state, which is what gated means. */
        boolean isGated(ClusterState currentState, PutMappingClusterStateUpdateRequest request) {
            for (Index index : request.indices()) {
                if (currentState.metadata().hasIndex(index.getName())) {
                    return false;
                }
            }
            return request.indices().length > 0;
        }

        /**
         * Records the request's fields against the index's mapping generation.
         *
         * <p>Extraction is {@link DescriptorRepresentable#fieldDefinitionsOrNull(Object)}, the same call the
         * creation path makes. Sharing it is the point rather than a tidy-up: these were two implementations
         * documented as mirroring each other, and the divergence was not cosmetic. A mapping the creation
         * path refused outright, this one accepted and silently reduced.
         *
         * <p>The store now holds a field's whole definition, so object and nested fields and every
         * field parameter round-trip. What is left to refuse is a property whose definition is not an object
         * at all.
         */
        void recordGatedMapping(PutMappingClusterStateUpdateRequest request) throws IOException {
            for (Index index : request.indices()) {
                refuseIfDescriptorShowsTheIndexIsGone(index);
            }
            Map<String, Object> parsed = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), request.source(), false);
            Map<String, Object> fields = DescriptorRepresentable.fieldDefinitionsOrNull(parsed.get("properties"));
            if (fields == null) {
                // Refused rather than partially recorded, and this is the half of the defect that was
                // worse. The extraction here used to skip any property it could not read and record the
                // rest, then report success -- so a put-mapping adding an object field to a gated index was
                // acknowledged with the field silently absent. The same field at creation was refused and
                // kept the index resident, which is exactly the "gated at creation and refused on update,
                // or the reverse" that the shared extractor's javadoc says must not exist.
                //
                // There is no fallback available: the index is not in cluster state, so this cannot be
                // applied the ordinary way. Failing is the only answer that does not lose the field, and
                // the outer catch turns it into a failure of this request alone.
                throw new IllegalArgumentException(
                    "index ["
                        + request.indices()[0].getName()
                        + "] is held outside cluster state and its mapping store holds a flat map of field "
                        + "name to type, so it cannot carry an object or nested field, or a field parameter "
                        + "such as a date format or an analyzer. Applying this mapping would drop them"
                );
            }
            if (fields.isEmpty()) {
                return;
            }
            for (Index index : request.indices()) {
                MappingGenerationStore.updateMapping(index.getUUID(), fields);
            }
        }

        /**
         * Refuses a gated put-mapping whose index the descriptor plane no longer says is live.
         *
         * <p>{@link #isGated} decides gating purely from the index's absence in cluster state, and that is
         * equally true of a live gated index and one the deletion-time prune already removed: cluster
         * state never carried an entry for either. The uuid this request names was resolved by the
         * coordinating node from its own descriptor cache, which invalidates on a tombstone it has seen but
         * not on one it has not -- so a node whose cache has not yet caught up can still route a put-mapping
         * or a dynamic field inference at a uuid whose tombstone is already durable elsewhere. Nothing
         * previously asked the store whether that uuid was still current, so the write landed, recreated the
         * document the deletion had pruned, and nothing was ever going to remove it again.
         *
         * <p><b>The contract this closes the hole by:</b> refusing here, not letting the write land and sweeping
         * it up later. The alternative -- a second pass that prunes stray mappings after the fact -- was
         * tried first, keyed off the tombstone the deletion already wrote, and rejected as a second writer
         * racing the same records. This resolves the descriptor for the uuid fresh, on the thread doing the write, which is
         * off the cluster manager's update thread already and so may block. For a live index the resolution
         * is a cache hit against the same descriptor cache every other resolution on this path already
         * pays for, not a new cost.
         *
         * <p>Skipped when descriptor resolution is not registered at all, which is not a hole: {@code
         * DescriptorGate} registers the descriptor supplier and {@link MappingGenerationStore} together in
         * one {@code install} call, so a node that reached {@link #isGated} answering true by way of the
         * store also has descriptor resolution to consult. A caller that registers only the store, as several
         * tests below the descriptor plane do, is exercising the mapping store in isolation and has no
         * tombstone to consult in the first place.
         *
         * <p><b>A null answer is not treated as "gone".</b> {@link AbsentIndexDescriptorSuppliers#supply}
         * documents null as "no answer" and swallows any failure that is not a {@link
         * DescriptorUnavailableException} into it, precisely so a resolver bug degrades a request rather than
         * failing it -- every other caller of this seam relies on that. Refusing on null as well as on a
         * confirmed tombstone would let that same resolver bug fail a live index's put-mapping outright,
         * which is a regression this task must not introduce to close a narrower one. It is also not what a
         * genuine deletion looks like here: {@code BlobDescriptorBackend#get} answers a tombstoned name with
         * the tombstone record, not null, so this only ever refuses on an answer that actually says so.
         */
        // Deliberately still AbsentIndexDescriptorSuppliers directly, not migrated to the resolver-backed
        // Metadata accessors: this needs the raw IndexDescriptor's own uuid() and the
        // three-way null/tombstoned/live distinction, which IndexMetadataResolver's generic, collapsed
        // "null means absent" contract deliberately does not expose (see that interface's own javadoc).
        private void refuseIfDescriptorShowsTheIndexIsGone(Index index) {
            if (AbsentIndexDescriptorSuppliers.isRegistered() == false) {
                return;
            }
            IndexDescriptor current = AbsentIndexDescriptorSuppliers.supply(index.getName());
            if (current != null && (current.exists() == false || current.uuid().equals(index.getUUID()) == false)) {
                throw new org.opensearch.index.IndexNotFoundException(
                    "its tombstone is already durable; the mapping store refuses a write against a uuid the descriptor plane no "
                        + "longer resolves as live",
                    index.getName()
                );
            }
        }

        private ClusterState applyRequest(
            ClusterState currentState,
            PutMappingClusterStateUpdateRequest request,
            Map<Index, MapperService> indexMapperServices
        ) throws IOException {
            CompressedXContent mappingUpdateSource = new CompressedXContent(request.source());
            final Metadata metadata = currentState.metadata();
            final List<IndexMetadata> updateList = new ArrayList<>();
            for (Index index : request.indices()) {
                MapperService mapperService = indexMapperServices.get(index);
                // IMPORTANT: always get the metadata from the state since it get's batched
                // and if we pull it from the indexService we might miss an update etc.
                final IndexMetadata indexMetadata = currentState.getMetadata().getIndexSafe(index);

                // this is paranoia... just to be sure we use the exact same metadata tuple on the update that
                // we used for the validation, it makes this mechanism little less scary (a little)
                updateList.add(indexMetadata);
                // try and parse it (no need to add it here) so we can bail early in case of parsing exception
                DocumentMapper existingMapper = mapperService.documentMapper();
                DocumentMapper newMapper = mapperService.parse(MapperService.SINGLE_MAPPING_NAME, mappingUpdateSource);
                if (existingMapper != null) {
                    // first, simulate: just call merge and ignore the result
                    existingMapper.merge(newMapper.mapping(), MergeReason.MAPPING_UPDATE);
                }

            }
            Metadata.Builder builder = Metadata.builder(metadata);
            boolean updated = false;
            for (IndexMetadata indexMetadata : updateList) {
                boolean updatedMapping = false;
                // do the actual merge here on the master, and update the mapping source
                // we use the exact same indexService and metadata we used to validate above here to actually apply the update
                final Index index = indexMetadata.getIndex();
                final MapperService mapperService = indexMapperServices.get(index);
                boolean isCompositeFieldPresent = !mapperService.getCompositeFieldTypes().isEmpty();
                CompressedXContent existingSource = null;
                DocumentMapper existingMapper = mapperService.documentMapper();
                if (existingMapper != null) {
                    existingSource = existingMapper.mappingSource();
                }
                DocumentMapper mergedMapper = mapperService.merge(
                    MapperService.SINGLE_MAPPING_NAME,
                    mappingUpdateSource,
                    MergeReason.MAPPING_UPDATE
                );

                CompositeIndexValidator.validate(
                    mapperService,
                    indicesService.getCompositeIndexSettings(),
                    mapperService.getIndexSettings(),
                    isCompositeFieldPresent
                );

                CompressedXContent updatedSource = mergedMapper.mappingSource();

                if (existingSource != null) {
                    if (existingSource.equals(updatedSource)) {
                        // same source, no changes, ignore it
                    } else {
                        updatedMapping = true;
                        // use the merged mapping source
                        if (logger.isDebugEnabled()) {
                            logger.debug("{} update_mapping [{}] with source [{}]", index, mergedMapper.type(), updatedSource);
                        } else if (logger.isInfoEnabled()) {
                            logger.info("{} update_mapping [{}]", index, mergedMapper.type());
                        }

                    }
                } else {
                    updatedMapping = true;
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} create_mapping with source [{}]", index, updatedSource);
                    } else if (logger.isInfoEnabled()) {
                        logger.info("{} create_mapping", index);
                    }
                }

                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                // Mapping updates on a single type may have side-effects on other types so we need to
                // update mapping metadata on all types
                DocumentMapper mapper = mapperService.documentMapper();
                if (mapper != null) {
                    indexMetadataBuilder.putMapping(new MappingMetadata(mapper.mappingSource()));
                }
                if (updatedMapping) {
                    indexMetadataBuilder.mappingVersion(1 + indexMetadataBuilder.mappingVersion());
                }
                /*
                 * This implicitly increments the index metadata version and builds the index metadata. This means that we need to have
                 * already incremented the mapping version if necessary. Therefore, the mapping version increment must remain before this
                 * statement.
                 */
                builder.put(indexMetadataBuilder);
                updated |= updatedMapping;
            }
            if (updated) {
                return ClusterState.builder(currentState).metadata(builder).build();
            } else {
                return currentState;
            }
        }
    }

    public void putMapping(final PutMappingClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        // A gated index has no cluster state entry, so a put-mapping against one changes no cluster state at
        // all: the fields go to MappingGenerationStore and nothing is published. Handling that here, before
        // any task is submitted, is what keeps it off the cluster manager's update thread.
        //
        // It used to be handled inside PutMappingExecutor#execute, which runs on that thread, and the store
        // is backed by an ordinary index whose read and write both block. So every put-mapping on a gated
        // index made two blocking round trips on the single thread whose serialization is the ceiling this
        // whole design exists to remove -- the same mistake creation and deletion were already moved off,
        // in the one metadata path that was not looked at.
        //
        // It was also an assertion failure rather than merely slow: "Expected current thread to not be the
        // cluster-manager service thread. Reason: [Blocking operation]". IndexBackedMappingStore's javadoc
        // argues the blocking is safe because a mapping update runs on a transport thread "not on the
        // cluster state thread", which is true of dynamic field inference and false of put-mapping.
        //
        // GENERIC rather than the calling thread, because TransportPutMappingAction declares
        // ThreadPool.Names.SAME: the caller here is a transport thread, and blocking one of those trades a
        // stalled cluster manager for a stalled transport pool.
        if (MappingGenerationStore.isRegistered() && putMappingExecutor.isGated(clusterService.state(), request)) {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    putMappingExecutor.recordGatedMapping(request);
                    listener.onResponse(new ClusterStateUpdateResponse(true));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
            return;
        }
        clusterService.submitStateUpdateTask(
            "put-mapping " + Strings.arrayToCommaDelimitedString(request.indices()),
            request,
            ClusterStateTaskConfig.build(Priority.HIGH, request.clusterManagerNodeTimeout()),
            putMappingExecutor,
            new AckedClusterStateTaskListener() {

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return true;
                }

                @Override
                public void onAllNodesAcked(@Nullable Exception e) {
                    listener.onResponse(new ClusterStateUpdateResponse(e == null));
                }

                @Override
                public void onAckTimeout() {
                    listener.onResponse(new ClusterStateUpdateResponse(false));
                }

                @Override
                public TimeValue ackTimeout() {
                    return request.ackTimeout();
                }
            }
        );
    }
}
