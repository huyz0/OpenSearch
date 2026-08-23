/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardRecoveryStrategy;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;

import java.io.IOException;

/**
 * Shared restore/release logic for this plugin's engine-native snapshots (see the design writeup
 * at {@code docs-site/src/content/docs/design/snapshot-restore-proposal.md}), reused by two
 * different callers with two different lifecycles:
 *
 * <ul>
 *   <li>{@link ObjectStoreShardRecoveryStrategy#engineNativeSnapshots()} hands this instance to
 *       core for {@link #restore} against a specific, live restore-target shard.
 *   <li>{@link org.opensearch.index.engine.EngineNativeSnapshotReleasers} holds the same single
 *       instance (registered once, node-wide, under {@link #ENGINE_ID}) for {@link #release},
 *       called when a snapshot is deleted -- potentially long after the original (or even the
 *       restore-target) shard no longer exists anywhere in the cluster.
 * </ul>
 *
 * <p>Both operations need the <em>original</em> snapshotted shard's own container -- which may
 * belong to a different index than whatever shard restore is materializing into -- resolved via
 * {@code containerResolver}, the same {@link ShardCloner.ContainerResolver} shape {@code
 * ShardCloner} itself already uses for cross-index resolution.
 *
 * <p>This class used to implement {@code EngineFactory} with a {@code newReadWriteEngine} that
 * threw, purely because that was the type the release registry was keyed to -- a stub whose only
 * purpose was to satisfy a type it could never honor. {@link
 * ShardRecoveryStrategy.EngineNativeSnapshots} is exactly these two operations and nothing else,
 * so the stub is gone.
 */
public final class EngineNativeSnapshotSupport implements ShardRecoveryStrategy.EngineNativeSnapshots {

    /** The {@code engineId} tag every pointer this plugin produces is written under. */
    public static final String ENGINE_ID = "serverless-storage/v1";

    private final ShardCloner.ContainerResolver containerResolver;

    public EngineNativeSnapshotSupport(ShardCloner.ContainerResolver containerResolver) {
        this.containerResolver = containerResolver;
    }

    @Override
    public boolean restore(IndexShard indexShard, Store store, byte[] snapshotPointer) throws IOException {
        CommitManifest manifest = EngineNativeSnapshotPayload.fromBytes(snapshotPointer).manifest();

        // The manifest's bundle files physically live in the ORIGINAL shard's container -- this
        // plugin never copies bundle bytes on snapshot, only pins the generation (see
        // ObjectStoreWriterEngine#attemptEngineNativeSnapshot) -- so materialization must read from
        // there, not from the restore target's own (still-empty) container. `containerResolver`
        // resolves (indexUuid, shardId) against THIS cluster's own live repository/encryption
        // configuration, never the snapshot's actual origin -- there is currently no repository/
        // location identity carried in the pointer payload to check against. Restoring within the
        // same cluster/repository this snapshot was taken from (the only currently-supported case)
        // resolves correctly; restoring into a different cluster or after the repository's
        // underlying location changed will fail here with a NoSuchFileException, caught below and
        // re-thrown with an explanation, rather than surfacing as an opaque low-level read failure.
        BlobContainer sourceContainer = containerResolver.resolve(manifest.indexUuid(), manifest.shardId());
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(sourceContainer));
        Directory directory = store.directory();
        try {
            materializer.materialize(manifest, directory);
        } catch (java.nio.file.NoSuchFileException e) {
            throw new IOException(
                "engine-native snapshot restore could not find the original shard's data ("
                    + manifest.indexUuid()
                    + "/"
                    + manifest.shardId()
                    + ") in this cluster's own repository configuration. This plugin's engine-native "
                    + "snapshots reference the original shard's data by (indexUuid, shardId) resolved "
                    + "against the CURRENT cluster's repository configuration at restore time, not the "
                    + "snapshot's actual origin -- restoring into a different cluster, or after this "
                    + "repository's underlying storage location changed, is not supported",
                e
            );
        }

        String translogUUID = Translog.createEmptyTranslog(
            indexShard.shardPath().resolveTranslog(),
            manifest.localCheckpoint(),
            indexShard.shardId(),
            indexShard.getPendingPrimaryTerm()
        );
        store.associateIndexWithNewTranslog(translogUUID);
        return true;
    }

    @Override
    public void release(byte[] snapshotPointer) throws IOException {
        EngineNativeSnapshotPayload payload = EngineNativeSnapshotPayload.fromBytes(snapshotPointer);
        CommitManifest manifest = payload.manifest();
        BlobContainer sourceContainer = containerResolver.resolve(manifest.indexUuid(), manifest.shardId());
        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(sourceContainer);
        pinRegistry.removePin(manifest.indexUuid(), manifest.shardId(), payload.pinId());
    }
}
