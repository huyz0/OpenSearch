/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.EngineNativeSnapshotPointer;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.snapshots.SnapshotId;

import java.util.Optional;

/**
 * Proves the generic core seam {@code Engine#attemptEngineNativeSnapshot} /
 * {@code IndexShard#attemptEngineNativeSnapshot} actually dispatches through the real, live shard
 * to whichever engine is currently backing it -- not just that it compiles. Deliberately
 * engine-agnostic (a bare {@link InternalEngine} override, not any specific plugin's engine),
 * matching {@link EngineRecoveryOperationsTests}'s own sibling seam and shape: the seam itself is
 * core, generic infrastructure; a specific engine-native snapshot mechanism built on top of it
 * (plugins/serverless-storage's manifest-pinning implementation) is that plugin's own concern and
 * is tested there.
 */
public class AttemptEngineNativeSnapshotTests extends IndexShardTestCase {

    public void testDefaultAttemptEngineNativeSnapshotReturnsEmpty() throws Exception {
        // The default (empty) Engine#attemptEngineNativeSnapshot -- every engine that doesn't
        // override it, i.e. current behavior for the entire codebase outside
        // plugins/serverless-storage -- must leave SnapshotShardsService free to fall back to the
        // classic snapshot path unchanged.
        IndexShard shard = newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(new InternalEngineFactory()));
        try {
            Optional<EngineNativeSnapshotPointer> result = shard.attemptEngineNativeSnapshot(
                new SnapshotId("test-snap", "test-snap-uuid")
            );
            assertTrue("a plain InternalEngine (the default attemptEngineNativeSnapshot()) must return empty", result.isEmpty());
        } finally {
            closeShards(shard);
        }
    }

    public void testOverriddenAttemptEngineNativeSnapshotReturnsThePointerThroughTheShard() throws Exception {
        final EngineNativeSnapshotPointer expected = new EngineNativeSnapshotPointer("test-engine/v1", new byte[] { 1, 2, 3 });
        EngineFactory engineFactory = config -> new InternalEngine(config) {
            @Override
            public Optional<EngineNativeSnapshotPointer> attemptEngineNativeSnapshot(SnapshotId snapshotId) {
                return Optional.of(expected);
            }
        };

        IndexShard shard = newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(engineFactory));
        try {
            Optional<EngineNativeSnapshotPointer> result = shard.attemptEngineNativeSnapshot(
                new SnapshotId("test-snap", "test-snap-uuid")
            );
            assertTrue("the pointer the engine returned must reach the IndexShard caller unchanged", result.isPresent());
            assertEquals(expected.engineId(), result.get().engineId());
            assertArrayEquals(expected.payload(), result.get().payload());
        } finally {
            closeShards(shard);
        }
    }

    public void testAttemptEngineNativeSnapshotThrowsWhenShardNotStartedOrClosed() throws Exception {
        // Mirrors IndexShard#acquireLastIndexCommit's own shard-state check, which
        // attemptEngineNativeSnapshot deliberately copies: a shard mid-recovery (not yet STARTED)
        // must reject the call rather than dispatching to a not-yet-ready engine.
        IndexShard shard = newShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(new InternalEngineFactory()));
        try {
            expectThrows(
                IllegalIndexShardStateException.class,
                () -> shard.attemptEngineNativeSnapshot(new SnapshotId("test-snap", "test-snap-uuid"))
            );
        } finally {
            closeShards(shard);
        }
    }
}
