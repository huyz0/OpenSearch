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

/** {@link EngineFactory} for writer shards: produces {@link ObjectStoreWriterEngine}s. */
public final class WriterEngineFactory implements EngineFactory {

    private final ObjectStoreCommitHeadPublisher headPublisher;

    public WriterEngineFactory(ObjectStoreCommitHeadPublisher headPublisher) {
        this.headPublisher = headPublisher;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        try {
            return new ObjectStoreWriterEngine(config, headPublisher);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new EngineCreationFailureException(config.getShardId(), "failed to create object-store writer engine", e);
        }
    }
}
