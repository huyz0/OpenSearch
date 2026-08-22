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
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.translog.Translog;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Proves the generic core seam {@code Engine#engineRecoveryOperations()} /
 * {@code IndexShard#recoverAdditionalEngineOperations} actually wires an engine's extra recovery
 * operations through the real shard-recovery path -- not just that it compiles. This is
 * deliberately engine-agnostic (a bare {@link InternalEngine} override, not any specific plugin's
 * engine) since the seam itself is core, generic infrastructure; a specific durability mechanism
 * built on top of it (e.g. plugins/serverless-storage's WAL replay) is that plugin's own concern
 * and is tested there.
 */
public class EngineRecoveryOperationsTests extends IndexShardTestCase {

    public void testExtraEngineRecoveryOperationsAreAppliedWhenShardStarts() throws Exception {
        EngineFactory engineFactory = config -> new InternalEngine(config) {
            @Override
            public List<Translog.Operation> engineRecoveryOperations() {
                long primaryTerm = config.getPrimaryTermSupplier().getAsLong();
                return List.of(new Translog.Index("extra-doc", 0, primaryTerm, 1L, "{}".getBytes(StandardCharsets.UTF_8), null, -1));
            }
        };

        // Not newStartedShard(): its own post-recovery sanity check
        // (getMaxSeqNoOfUpdatesOrDeletes() == seqNoStats().getMaxSeqNo()) assumes every applied op
        // came through the ordinary replication-tracking path, which a synthetic replay op crafted
        // directly for this test deliberately doesn't -- unrelated to what this test verifies.
        IndexShard shard = newShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(engineFactory));
        try {
            recoverShardFromStore(shard);
            assertEquals(
                "the extra operation Engine#engineRecoveryOperations() supplied must have actually been applied",
                0L,
                shard.seqNoStats().getMaxSeqNo()
            );
        } finally {
            closeShards(shard);
        }
    }

    public void testDefaultEngineRecoveryOperationsIsEmptyAndDoesNotAffectNormalShardStart() throws Exception {
        IndexShard shard = newStartedShard(true);
        try {
            assertEquals(
                "a plain InternalEngine (the default engineRecoveryOperations()) must not have replayed anything",
                -1L,
                shard.seqNoStats().getMaxSeqNo()
            );
        } finally {
            closeShards(shard);
        }
    }
}
