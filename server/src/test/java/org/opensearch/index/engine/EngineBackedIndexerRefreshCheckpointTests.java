/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link EngineBackedIndexer} asks its engine for refreshed checkpoints, so any engine that tracks
 * them -- not only {@link InternalEngine} -- can serve as a segment replication primary.
 */
public class EngineBackedIndexerRefreshCheckpointTests extends OpenSearchTestCase {

    public void testDelegatesToTheEngine() {
        Engine engine = mock(Engine.class);
        when(engine.lastRefreshedCheckpoint()).thenReturn(7L);
        when(engine.currentOngoingRefreshCheckpoint()).thenReturn(9L);
        EngineBackedIndexer indexer = new EngineBackedIndexer(engine);
        assertEquals(7L, indexer.lastRefreshedCheckpoint());
        assertEquals(9L, indexer.currentOngoingRefreshCheckpoint());
    }

    public void testAnEngineThatDoesNotTrackThemStillRefuses() {
        Engine engine = mock(Engine.class, CALLS_REAL_METHODS);
        EngineBackedIndexer indexer = new EngineBackedIndexer(engine);
        expectThrows(UnsupportedOperationException.class, indexer::lastRefreshedCheckpoint);
        expectThrows(UnsupportedOperationException.class, indexer::currentOngoingRefreshCheckpoint);
    }
}
