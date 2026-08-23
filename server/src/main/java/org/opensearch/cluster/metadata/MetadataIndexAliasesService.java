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

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.AliasAction.NewAliasValidator;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.opensearch.cluster.service.ClusterManagerTask.INDEX_ALIASES;
import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;

/**
 * Service responsible for submitting add and remove aliases requests
 *
 * @opensearch.internal
 */
public class MetadataIndexAliasesService {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AliasValidator aliasValidator;

    private final MetadataDeleteIndexService deleteIndexService;

    private final NamedXContentRegistry xContentRegistry;
    private final ClusterManagerTaskThrottler.ThrottlingKey indexAliasTaskKey;

    @Inject
    public MetadataIndexAliasesService(
        ClusterService clusterService,
        IndicesService indicesService,
        AliasValidator aliasValidator,
        MetadataDeleteIndexService deleteIndexService,
        NamedXContentRegistry xContentRegistry
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.aliasValidator = aliasValidator;
        this.deleteIndexService = deleteIndexService;
        this.xContentRegistry = xContentRegistry;

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        indexAliasTaskKey = clusterService.registerClusterManagerTask(INDEX_ALIASES, true);

    }

    public void indicesAliases(
        final IndicesAliasesClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        // Descriptor writes issued while the state update runs, so the acknowledgement can wait for them.
        //
        // An alias action against a gated index changes nothing in cluster state -- the index has no entry
        // there -- so the descriptor write is the whole of the operation. It used to be issued inside
        // execute() with its future discarded, which meant the request was acknowledged whether or not the
        // alias was ever recorded, and identically when no updater was installed at all.
        //
        // Overwritten rather than appended to, because a cluster state task may be re-executed and only the
        // run that commits is the one whose writes matter. The same reasoning MetadataDeleteIndexService
        // applies to the list of indices it tombstones, and it is safe for the same reason: each write is an
        // idempotent read-modify-write at the store, so a re-executed task issues it twice and converges.
        final java.util.concurrent.atomic.AtomicReference<List<java.util.concurrent.CompletableFuture<Boolean>>> gatedWrites =
            new java.util.concurrent.atomic.AtomicReference<>(List.of());

        final ActionListener<ClusterStateUpdateResponse> deferred = ActionListener.wrap(
            response -> whenGatedAliasWritesLand(gatedWrites.get(), response, listener),
            listener::onFailure
        );

        clusterService.submitStateUpdateTask(
            "index-aliases",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, deferred) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return indexAliasTaskKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return applyAliasActions(currentState, request.actions(), gatedWrites::set);
                }
            }
        );
    }

    /**
     * Holds the acknowledgement until every gated alias write is durable, failing it if one is not.
     *
     * <p>Nothing blocks. The futures complete on the descriptor store's own executor and the listener is
     * completed from there, which is what lets a write be both off the cluster state thread and ahead of the
     * client being told the request succeeded -- the same window {@code ClaimedIndexLifecycle} uses for a
     * deletion's record.
     */
    private static void whenGatedAliasWritesLand(
        final List<java.util.concurrent.CompletableFuture<Boolean>> writes,
        final ClusterStateUpdateResponse response,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        if (writes.isEmpty()) {
            listener.onResponse(response);
            return;
        }
        java.util.concurrent.CompletableFuture.allOf(writes.toArray(new java.util.concurrent.CompletableFuture[0]))
            .whenComplete((ignored, failure) -> {
                if (failure != null) {
                    Throwable cause = failure.getCause() != null ? failure.getCause() : failure;
                    listener.onFailure(
                        cause instanceof Exception
                            ? (Exception) cause
                            : new IllegalStateException("an alias change could not be recorded", cause)
                    );
                    return;
                }
                for (java.util.concurrent.CompletableFuture<Boolean> write : writes) {
                    if (Boolean.TRUE.equals(write.getNow(Boolean.FALSE)) == false) {
                        listener.onFailure(new IllegalStateException("an alias change could not be recorded against its index"));
                        return;
                    }
                }
                listener.onResponse(response);
            });
    }

    /**
     * Handles the cluster state transition to a version that reflects the provided {@link AliasAction}s.
     *
     * <p>The two-argument form drops whatever descriptor writes the actions produce, which is right only for
     * a caller that has no acknowledgement to defer. {@link #indicesAliases} uses the three-argument form.
     */
    public ClusterState applyAliasActions(ClusterState currentState, Iterable<AliasAction> actions) {
        return applyAliasActions(currentState, actions, writes -> {});
    }

    /**
     * @param gatedWrites receives the descriptor writes issued for indices with no cluster state entry, so
     *                    the caller can hold its acknowledgement until they land. Called once, with every
     *                    write this pass issued, including when there are none.
     */
    public ClusterState applyAliasActions(
        ClusterState currentState,
        Iterable<AliasAction> actions,
        java.util.function.Consumer<List<java.util.concurrent.CompletableFuture<Boolean>>> gatedWrites
    ) {
        final List<java.util.concurrent.CompletableFuture<Boolean>> descriptorWrites = new ArrayList<>();
        try {
            return applyAliasActions(currentState, actions, descriptorWrites);
        } finally {
            // Reported even when an action threw, so writes already issued for earlier actions are still
            // waited on rather than left in flight behind a failed request.
            gatedWrites.accept(List.copyOf(descriptorWrites));
        }
    }

    private ClusterState applyAliasActions(
        ClusterState currentState,
        Iterable<AliasAction> actions,
        List<java.util.concurrent.CompletableFuture<Boolean>> descriptorWrites
    ) {
        List<Index> indicesToClose = new ArrayList<>();
        Map<String, IndexService> indices = new HashMap<>();
        try {
            boolean changed = false;
            // Gather all the indexes that must be removed first so:
            // 1. We don't cause error when attempting to replace an index with a alias of the same name.
            // 2. We don't allow removal of aliases from indexes that we're just going to delete anyway. That'd be silly.
            Set<Index> indicesToDelete = new HashSet<>();
            for (AliasAction action : actions) {
                if (action.removeIndex()) {
                    IndexMetadata index = currentState.metadata().getIndices().get(action.getIndex());
                    if (index == null) {
                        throw new IndexNotFoundException(action.getIndex());
                    }
                    validateAliasTargetIsNotDSBackingIndex(currentState, action);
                    indicesToDelete.add(index.getIndex());
                    changed = true;
                }
            }
            // Remove the indexes if there are any to remove
            if (changed) {
                currentState = deleteIndexService.deleteIndices(currentState, indicesToDelete);
            }
            Metadata.Builder metadata = Metadata.builder(currentState.metadata());
            // Run the remaining alias actions
            final Set<String> maybeModifiedIndices = new HashSet<>();
            for (AliasAction action : actions) {
                if (action.removeIndex()) {
                    // Handled above
                    continue;
                }
                IndexMetadata index = metadata.get(action.getIndex());
                if (index == null) {
                    // Deliberately still AbsentIndexDescriptorSuppliers directly, not migrated to the
                    // resolver-backed Metadata accessors: what is being asked here is specifically "does
                    // this name resolve in the gated plane", which the generic IndexMetadata contract cannot
                    // express. It is used for that question alone -- the alias list itself is read by the
                    // store when it applies the mutation, not from this possibly-stale copy.
                    IndexDescriptor gated = AbsentIndexDescriptorSuppliers.supply(action.getIndex());
                    if (gated != null && gated.exists()) {
                        if (action instanceof AliasAction.Add addAction) {
                            // IndexDescriptor#aliases is a plain List<String> -- it has nowhere to record
                            // a filter, a routing value, or
                            // a write-index flag. Silently keeping only the name while an operator asked
                            // for one of those too is exactly the "provably has none" invariant
                            // Metadata#aliasesForConcreteIndex and IndexNameExpressionResolver#filteringAliases
                            // both rely on, quietly turned false: a filtered alias would appear to succeed
                            // here and then never be enforced there, which is a silent document-level-security
                            // bypass, not a cosmetic gap. Refusing the action is strictly narrower than the
                            // previous (buggy) behavior, so nothing that worked correctly before stops working.
                            if (addAction.getFilter() != null
                                || addAction.getIndexRouting() != null
                                || addAction.getSearchRouting() != null
                                || addAction.writeIndex() != null) {
                                throw new IllegalArgumentException(
                                    "alias ["
                                        + addAction.getAlias()
                                        + "] for index ["
                                        + action.getIndex()
                                        + "] cannot carry a filter, a routing value, or a write-index flag: "
                                        + "this index type records only alias names, and silently dropping any "
                                        + "of those would leave a filter that looks configured but is never "
                                        + "enforced. A plain (name-only) alias is supported; add filtering or "
                                        + "routing once this index type has somewhere to store them."
                                );
                            }
                            // The change is handed over rather than applied here, and the difference is the
                            // whole of it. `gated` came out of the resolution cache and can be a freshness
                            // window stale -- on this thread it is the cached copy or nothing, since the
                            // resolver refuses I/O on the cluster state thread. Writing
                            // gated.withAliases(...) back was unconditional, so it also reverted whatever
                            // else had changed on the real descriptor meanwhile, a concurrent dynamic-field
                            // addition most of all. The store applies this to what it actually holds, under
                            // a conditional write.
                            final String toAdd = addAction.getAlias();
                            descriptorWrites.add(IndexDescriptorPublisher.updateGated(action.getIndex(), current -> {
                                if (current.aliases().contains(toAdd)) {
                                    // Already there. Returning the descriptor unchanged is how this says
                                    // "nothing to write" without a write, which keeps a repeated request
                                    // free rather than merely harmless.
                                    return current;
                                }
                                List<String> withAdded = new ArrayList<>(current.aliases());
                                withAdded.add(toAdd);
                                return current.withAliases(withAdded);
                            }));
                        } else if (action instanceof AliasAction.Remove removeAction) {
                            final String toRemove = removeAction.getAlias();
                            descriptorWrites.add(IndexDescriptorPublisher.updateGated(action.getIndex(), current -> {
                                List<String> withoutRemoved = new ArrayList<>(current.aliases());
                                return withoutRemoved.remove(toRemove) ? current.withAliases(withoutRemoved) : current;
                            }));
                        }
                        continue;
                    }
                    throw new IndexNotFoundException(action.getIndex());
                }
                validateAliasTargetIsNotDSBackingIndex(currentState, action);
                NewAliasValidator newAliasValidator = (alias, indexRouting, filter, writeIndex) -> {
                    /* It is important that we look up the index using the metadata builder we are modifying so we can remove an
                     * index and replace it with an alias. */
                    Function<String, IndexMetadata> indexLookup = name -> metadata.get(name);
                    aliasValidator.validateAlias(alias, action.getIndex(), indexRouting, indexLookup);
                    if (Strings.hasLength(filter)) {
                        IndexService indexService = indices.get(index.getIndex().getName());
                        if (indexService == null) {
                            indexService = indicesService.indexService(index.getIndex());
                            if (indexService == null) {
                                // temporarily create the index and add mappings so we can parse the filter
                                try {
                                    indexService = indicesService.createIndex(index, emptyList(), false);
                                    indicesToClose.add(index.getIndex());
                                } catch (IOException e) {
                                    throw new OpenSearchException("Failed to create temporary index for parsing the alias", e);
                                }
                                indexService.mapperService().merge(index, MapperService.MergeReason.MAPPING_RECOVERY);
                            }
                            indices.put(action.getIndex(), indexService);
                        }
                        // the context is only used for validation so it's fine to pass fake values for the shard id,
                        // but the current timestamp should be set to real value as we may use `now` in a filtered alias
                        aliasValidator.validateAliasFilter(
                            alias,
                            filter,
                            indexService.newQueryShardContext(0, null, () -> System.currentTimeMillis(), null),
                            xContentRegistry
                        );
                    }
                };
                if (action.apply(newAliasValidator, metadata, index)) {
                    changed = true;
                    maybeModifiedIndices.add(index.getIndex().getName());
                }
            }

            for (final String maybeModifiedIndex : maybeModifiedIndices) {
                final IndexMetadata currentIndexMetadata = currentState.metadata().index(maybeModifiedIndex);
                final IndexMetadata newIndexMetadata = metadata.get(maybeModifiedIndex);
                // only increment the aliases version if the aliases actually changed for this index
                if (currentIndexMetadata.getAliases().equals(newIndexMetadata.getAliases()) == false) {
                    assert currentIndexMetadata.getAliasesVersion() == newIndexMetadata.getAliasesVersion();
                    metadata.put(new IndexMetadata.Builder(newIndexMetadata).aliasesVersion(1 + currentIndexMetadata.getAliasesVersion()));
                }
            }

            if (changed) {
                ClusterState updatedState = ClusterState.builder(currentState).metadata(metadata).build();
                // even though changes happened, they resulted in 0 actual changes to metadata
                // i.e. remove and add the same alias to the same index
                if (!updatedState.metadata().equalsAliases(currentState.metadata())) {
                    return updatedState;
                }
            }
            return currentState;
        } finally {
            for (Index index : indicesToClose) {
                indicesService.removeIndex(index, NO_LONGER_ASSIGNED, "created for alias processing");
            }
        }
    }

    private void validateAliasTargetIsNotDSBackingIndex(ClusterState currentState, AliasAction action) {
        IndexAbstraction indexAbstraction = currentState.metadata().getIndicesLookup().get(action.getIndex());
        assert indexAbstraction != null : "invalid cluster metadata. index [" + action.getIndex() + "] was not found";
        if (indexAbstraction.getParentDataStream() != null) {
            throw new IllegalArgumentException(
                "The provided index [ "
                    + action.getIndex()
                    + "] is a backing index belonging to data stream ["
                    + indexAbstraction.getParentDataStream().getName()
                    + "]. Data streams and their backing indices don't support alias operations."
            );
        }
    }
}
