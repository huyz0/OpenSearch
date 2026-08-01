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
import org.opensearch.action.admin.indices.delete.DeleteIndexClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.snapshots.RestoreService;
import org.opensearch.snapshots.SnapshotInProgressException;
import org.opensearch.snapshots.SnapshotsService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.service.ClusterManagerTask.DELETE_INDEX;

/**
 * Deletes indices.
 *
 * @opensearch.internal
 */
public class MetadataDeleteIndexService {

    private static final Logger logger = LogManager.getLogger(MetadataDeleteIndexService.class);

    private final Settings settings;
    private final ClusterService clusterService;

    private final AllocationService allocationService;
    private final ClusterManagerTaskThrottler.ThrottlingKey deleteIndexTaskKey;

    @Inject
    public MetadataDeleteIndexService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.allocationService = allocationService;

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        deleteIndexTaskKey = clusterService.registerClusterManagerTask(DELETE_INDEX, true);

    }

    public void deleteIndices(
        final DeleteIndexClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        // What was deleted, captured while the transform runs so the tombstone write below knows which
        // descriptors to store. Overwritten rather than appended to, because a cluster state task may be
        // re-executed and only the run that commits is the one whose deletions happened.
        final java.util.concurrent.atomic.AtomicReference<java.util.List<IndexMetadata>> deletedIndices =
            new java.util.concurrent.atomic.AtomicReference<>(java.util.List.of());

        // Which of these are gated, decided here rather than inside the transform.
        //
        // It has to be here because AbsentIndexDescriptorSuppliers refuses to answer on a cluster state
        // thread at all: C1's guard returns null on clusterManagerService#updateTask, since resolving a
        // descriptor there means a remote read on the thread that applies cluster state, which is the W4
        // deadlock. The first attempt at this partitioned inside execute and every index came back
        // not-gated, so the delete threw exactly as before -- the guard did its job and the code asking the
        // question was in the one place forbidden to ask it.
        //
        // This method runs on a transport thread, where blocking is allowed and where T39 already puts the
        // equivalent read for shard opening.
        final Map<Index, IndexMetadata> gatedDeletions = new HashMap<>();
        final Metadata currentMetadata = clusterService.state().metadata();
        for (Index index : request.indices()) {
            if (currentMetadata.index(index) == null) {
                IndexMetadata descriptorMetadata = AbsentIndexDescriptorSuppliers.metadataOrDescriptor(currentMetadata, index);
                if (descriptorMetadata != null) {
                    gatedDeletions.put(index, descriptorMetadata);
                }
            }
        }

        // The acknowledgement is deferred until the tombstones are durable.
        //
        // The publish hook inside the transform is asynchronous and best-effort, because it runs on the
        // cluster state thread where a blocking write deadlocks. Retries close transient failures but not a
        // crash between the commit and the write landing, and inferring a tombstone from a missing
        // descriptor was rejected: an unavailable store and a store with no record give the same answer, so
        // acting on absence would discard live shard data.
        //
        // Ordering is what is left. This window, after the state is committed and before the client is told
        // the delete succeeded, is the only place a write can be both off the cluster state thread and
        // ahead of the acknowledgement. Nothing blocks: the listener is deferred, not waited on.
        final ActionListener<ClusterStateUpdateResponse> durableListener = ActionListener.wrap(
            response -> DurableTombstones.whenDurable(
                deletedIndices.get(),
                ActionListener.wrap(ignored -> listener.onResponse(response), listener::onFailure)
            ),
            listener::onFailure
        );

        clusterService.submitStateUpdateTask(
            "delete-index " + Arrays.toString(request.indices()),
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, durableListener) {

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return deleteIndexTaskKey;
                }

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    java.util.List<IndexMetadata> deleted = new java.util.ArrayList<>();
                    for (Index index : request.indices()) {
                        IndexMetadata metadata = currentState.metadata().index(index);
                        if (metadata == null) {
                            // Gated: the descriptor is the only record, so it is also the only thing a
                            // tombstone can be derived from. Left out of this list, a gated delete would
                            // acknowledge with no durable no behind it, which is the resurrection
                            // DurableTombstones exists to prevent and is worse for a gated index than for an
                            // ordinary one, since there is no graveyard entry standing behind it either.
                            // Read from the map resolved on the calling thread, never re-resolved here.
                            metadata = gatedDeletions.get(index);
                        }
                        if (metadata != null) {
                            deleted.add(metadata);
                        }
                    }
                    deletedIndices.set(java.util.List.copyOf(deleted));
                    return deleteIndices(currentState, Sets.newHashSet(request.indices()), gatedDeletions);
                }
            }
        );
    }

    /**
     * Delete some indices from the cluster state.
     */
    public ClusterState deleteIndices(ClusterState currentState, Set<Index> indices) {
        return deleteIndices(currentState, indices, Map.of());
    }

    /**
     * Deletes indices, treating the ones named in {@code gatedDeletions} as having no cluster state entry.
     *
     * <p>A gated index has no cluster state entry, so everything below would fail on it, and
     * {@code getIndexSafe} is where it failed: "no such index" for a name the resolver had just resolved to
     * a concrete {@link Index} carrying the descriptor's uuid. Creation grew its branch for this
     * ({@code MetadataCreateIndexService} consulting {@link DescriptorOnlyCreation}) and deletion never grew
     * the matching one, so a gated index could be created and could never be deleted.
     *
     * <p>Nothing caught it because the only test that deletes one lives in a class that had been muted for
     * the symptom rather than the cause: the shard was still holding its lock at teardown precisely because
     * the delete had thrown.
     *
     * <p>The map is passed in rather than computed here because resolving a descriptor is a remote read and
     * this runs on the cluster state thread, where {@link AbsentIndexDescriptorSuppliers} refuses to answer.
     */
    private ClusterState deleteIndices(ClusterState currentState, Set<Index> indices, Map<Index, IndexMetadata> gatedDeletions) {
        final Metadata meta = currentState.metadata();

        if (gatedDeletions.isEmpty() == false) {
            final Set<Index> gated = new HashSet<>();
            for (Index index : indices) {
                if (meta.index(index) == null && gatedDeletions.containsKey(index)) {
                    gated.add(index);
                }
            }
            for (Index index : gated) {
                logger.info("{} deleting gated index, recording a tombstone rather than a cluster state change", index);
                IndexDescriptorPublisher.publishTombstone(gatedDeletions.get(index));
            }
            // Deliberately not added to the IndexGraveyard. The graveyard is a bounded list carried in every
            // cluster state, and putting gated deletions in it would reintroduce per-index cluster state cost
            // on the one operation that had escaped it. The tombstoned descriptor is the durable no for these,
            // which is what DurableTombstones and IndexDescriptorPublisher.publishTombstone already say.
            final Set<Index> remaining = new HashSet<>(indices);
            remaining.removeAll(gated);
            if (remaining.isEmpty()) {
                // Returning the state unchanged means no publication and no reroute. That is the point: a
                // gated delete must cost what a gated creation costs, and both are one descriptor write.
                return currentState;
            }
            indices = remaining;
        }

        final Set<Index> indicesToDelete = new HashSet<>();
        final Map<Index, DataStream> backingIndices = new HashMap<>();
        for (Index index : indices) {
            IndexMetadata im = meta.getIndexSafe(index);
            IndexAbstraction.DataStream parent = meta.getIndicesLookup().get(im.getIndex().getName()).getParentDataStream();
            if (parent != null) {
                if (parent.getWriteIndex().equals(im)) {
                    throw new IllegalArgumentException(
                        "index ["
                            + index.getName()
                            + "] is the write index for data stream ["
                            + parent.getName()
                            + "] and cannot be deleted"
                    );
                } else {
                    backingIndices.put(index, parent.getDataStream());
                }
            }
            indicesToDelete.add(im.getIndex());
        }

        // Check if index deletion conflicts with any running snapshots
        Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, indicesToDelete);
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException(
                "Cannot delete indices that are being snapshotted: "
                    + snapshottingIndices
                    + ". Try again after snapshot finishes or cancel the currently running snapshot."
            );
        }

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(meta);
        ClusterBlocks.Builder clusterBlocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

        final IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder(metadataBuilder.indexGraveyard());
        final int previousGraveyardSize = graveyardBuilder.tombstones().size();
        for (final Index index : indices) {
            String indexName = index.getName();
            logger.info("{} deleting index", index);
            routingTableBuilder.remove(indexName);
            clusterBlocksBuilder.removeIndexBlocks(indexName);
            // Area H's tombstone, recorded before the metadata entry goes, since it is derived from it.
            // Deletion has to be remembered rather than represented by absence: a node that was
            // partitioned during the delete cannot tell "this index never existed" from "I have not
            // looked yet", and adopting its dangling data on rejoin is the resurrection IndexGraveyard
            // exists to prevent. A tombstoned descriptor is the durable no, and unlike the graveyard,
            // which keeps a bounded list and forgets older deletions, it does not expire.
            IndexMetadata deleted = metadataBuilder.get(indexName);
            if (deleted != null) {
                IndexDescriptorPublisher.publishTombstone(deleted);
            }
            metadataBuilder.remove(indexName);
            if (backingIndices.containsKey(index)) {
                DataStream parent = metadataBuilder.dataStream(backingIndices.get(index).getName());
                metadataBuilder.put(parent.removeBackingIndex(index));
            }
        }
        // add tombstones to the cluster state for each deleted index
        final IndexGraveyard currentGraveyard = graveyardBuilder.addTombstones(indices).build(settings);
        metadataBuilder.indexGraveyard(currentGraveyard); // the new graveyard set on the metadata
        logger.trace(
            "{} tombstones purged from the cluster state. Previous tombstone size: {}. Current tombstone size: {}.",
            graveyardBuilder.getNumPurged(),
            previousGraveyardSize,
            currentGraveyard.getTombstones().size()
        );

        Metadata newMetadata = metadataBuilder.build();
        ClusterBlocks blocks = clusterBlocksBuilder.build();

        // update snapshot restore entries
        Map<String, ClusterState.Custom> customs = currentState.getCustoms();
        final RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
        RestoreInProgress updatedRestoreInProgress = RestoreService.updateRestoreStateWithDeletedIndices(restoreInProgress, indices);
        if (updatedRestoreInProgress != restoreInProgress) {
            final Map<String, ClusterState.Custom> builder = new HashMap<>(customs);
            builder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
            customs = Collections.unmodifiableMap(builder);
        }

        return allocationService.reroute(
            ClusterState.builder(currentState)
                .routingTable(routingTableBuilder.build())
                .metadata(newMetadata)
                .blocks(blocks)
                .customs(customs)
                .build(),
            "deleted indices [" + indices + "]"
        );
    }
}
