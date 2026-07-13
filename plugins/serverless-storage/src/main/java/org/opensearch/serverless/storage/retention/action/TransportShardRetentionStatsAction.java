/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.gc.BundleReferenceCounter;
import org.opensearch.serverless.storage.gc.ManifestId;
import org.opensearch.serverless.storage.gc.ManifestRetentionPolicy;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.retention.PitrRetentionPolicy;
import org.opensearch.serverless.storage.security.RestrictingBlobContainer;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The actual work behind {@link ShardRetentionStatsAction}: builds the same shape of stores {@code
 * GcSchedulerTask}'s own background sweep needs, from {@link
 * ServerlessStoragePlugin#blobContainerForDirectoryFactory}, then runs the exact same dry-run
 * policy computations ({@link ManifestRetentionPolicy}, {@link BundleReferenceCounter}) that
 * sweep would -- but only counts the results rather than deleting anything, so a one-off stats
 * query and the recurring sweep's actual decision can never silently disagree.
 *
 * <p>Requires no routing to a specific data node, same reasoning as {@code
 * TransportCompactionTriggerAction}: retention state lives entirely in the shared object store via
 * {@link BlobContainer}s, never in any node-local state.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * this does real blob-store I/O (lists every manifest and bundle the shard has).
 */
public class TransportShardRetentionStatsAction extends HandledTransportAction<ShardRetentionStatsRequest, ShardRetentionStatsResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's shard {@link BlobContainer} and this node's configured retention windows.
     * @param threadPool dispatches the actual stats computation off the transport thread.
     */
    @Inject
    public TransportShardRetentionStatsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool
    ) {
        super(ShardRetentionStatsAction.NAME, transportService, actionFilters, ShardRetentionStatsRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard to compute retention stats for.
     * @param listener notified with the result once the computation (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, ShardRetentionStatsRequest request, ActionListener<ShardRetentionStatsResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                String indexUuid = request.indexUuid();
                int shardId = request.shardId();
                // Credential scoping per tier (rfc-serverless-opensearch.md &sect;15): this is a
                // pure dry-run computation (lists manifests/bundles/pins, never writes or deletes
                // anything -- see this class's own javadoc), so the container is wrapped read-only.
                BlobContainer container = new RestrictingBlobContainer(
                    plugin.blobContainerForDirectoryFactory(indexUuid, shardId),
                    false,
                    false
                );
                BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
                BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
                BlobContainerDurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(container);

                List<CommitManifest> manifests = manifestStore.listManifests();
                Set<PinRecord> pins = pinRegistry.getPins(indexUuid, shardId);
                Set<ManifestId> durablyPinnedManifestIds = new HashSet<>();
                int pitrPinCount = 0;
                for (PinRecord pin : pins) {
                    durablyPinnedManifestIds.add(pin.toManifestId());
                    if (pin.pinId().equals(PitrRetentionPolicy.PITR_PIN_ID)) {
                        pitrPinCount++;
                    }
                }

                long gcRetentionWindowMillis = plugin.gcRetentionWindowMillis();
                long pitrWindowMillis = plugin.pitrWindowMillis();

                int deletableManifestCount = 0;
                int bundleCount = 0;
                int deletableBundleCount = 0;
                if (manifests.isEmpty() == false) {
                    long retentionCutoffMillis = System.currentTimeMillis() - gcRetentionWindowMillis;
                    List<CommitManifest> deletableManifests = ManifestRetentionPolicy.computeDeletableManifests(
                        manifests,
                        retentionCutoffMillis,
                        Set.of(),
                        durablyPinnedManifestIds
                    );
                    deletableManifestCount = deletableManifests.size();

                    List<CommitManifest> retainedManifests = ManifestRetentionPolicy.computeRetainedManifests(
                        manifests,
                        retentionCutoffMillis,
                        Set.of(),
                        durablyPinnedManifestIds
                    );
                    Set<String> liveBundles = BundleReferenceCounter.computeLiveBundles(retainedManifests);
                    Set<String> allKnownBundles = bundleStore.listBundleNames();
                    bundleCount = allKnownBundles.size();
                    deletableBundleCount = BundleReferenceCounter.computeDeletableBundles(allKnownBundles, liveBundles).size();
                }

                listener.onResponse(
                    new ShardRetentionStatsResponse(
                        manifests.size(),
                        deletableManifestCount,
                        bundleCount,
                        deletableBundleCount,
                        pins.size(),
                        pitrPinCount,
                        gcRetentionWindowMillis,
                        pitrWindowMillis
                    )
                );
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
