/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

import java.util.Optional;

/**
 * {@link EngineFactory} for reader shards: resolves the shard's currently-published head via
 * {@link ShardStateStore}, reads the corresponding {@link CommitManifest}, and opens an
 * {@link ObjectStoreReaderEngine} against it. A reader shard that has never had anything published
 * to it (no head yet) has nothing to open -- that is a configuration/timing error a reader shard
 * should not exist for, so it fails engine creation rather than opening an empty index.
 */
public final class ReaderEngineFactory implements EngineFactory {

    private final ShardStateStore shardStateStore;
    private final BlobContainerManifestStore manifestStore;
    private final ObjectStoreCommitMaterializer materializer;

    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer
    ) {
        this.shardStateStore = shardStateStore;
        this.manifestStore = manifestStore;
        this.materializer = materializer;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        try {
            String indexUuid = config.getShardId().getIndex().getUUID();
            int shardId = config.getShardId().getId();
            Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, shardId);
            if (head.isEmpty()) {
                throw new IllegalStateException("no published head for shard " + config.getShardId() + "; nothing for a reader to open");
            }
            ShardHead shardHead = head.get().head();
            CommitManifest manifest = manifestStore.readManifest(shardHead.primaryTerm(), shardHead.latestManifestGeneration());
            return ObjectStoreReaderEngine.open(config, manifest, materializer);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new EngineCreationFailureException(config.getShardId(), "failed to create object-store reader engine", e);
        }
    }
}
