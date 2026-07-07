/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectoryEntry;
import org.opensearch.serverless.storage.directory.ShardRole;

/** {@link EngineFactory} for writer shards: produces {@link ObjectStoreWriterEngine}s. */
public final class WriterEngineFactory implements EngineFactory {

    /** How long a directory entry for a writer shard is trusted before it's treated as stale. */
    private static final long DIRECTORY_ENTRY_TTL_MILLIS = 60_000L;

    private final ObjectStoreCommitHeadPublisher headPublisher;
    private final ShardDirectory shardDirectory;
    private final String localNodeId;

    public WriterEngineFactory(ObjectStoreCommitHeadPublisher headPublisher, ShardDirectory shardDirectory, String localNodeId) {
        this.headPublisher = headPublisher;
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        try {
            ObjectStoreWriterEngine engine = new ObjectStoreWriterEngine(config, headPublisher);
            // Report to the directory tier on activation (rfc-serverless-metadata-plane.md
            // &sect;9's activation path, step 3): a coordinator can now route directly to this
            // node instead of paying a shard-head read on every request. This is a hint, not a
            // fact -- see ShardDirectory's javadoc -- so a failure here is never worth failing
            // engine construction over; the shard is still fully correct and usable, just
            // undiscoverable via the fast path until the next successful report.
            String indexUuid = config.getShardId().getIndex().getUUID();
            int shardId = config.getShardId().getId();
            shardDirectory.report(
                indexUuid,
                shardId,
                new ShardDirectoryEntry(
                    localNodeId,
                    ShardRole.WRITER,
                    config.getPrimaryTermSupplier().getAsLong(),
                    0,
                    System.currentTimeMillis() + DIRECTORY_ENTRY_TTL_MILLIS
                )
            );
            return engine;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new EngineCreationFailureException(config.getShardId(), "failed to create object-store writer engine", e);
        }
    }
}
