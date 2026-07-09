/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;

import java.io.IOException;

/**
 * Proves the generic core seam {@code EngineFactory#recoverMissingLocalStore(IndexShard, Store)} /
 * {@code StoreRecovery#internalRecoverFromStore} actually wires an engine's ability to materialize
 * a missing local commit from elsewhere into the real shard-recovery path -- not just that it
 * compiles. Deliberately engine-agnostic (a bare {@link EngineFactory} override, not any specific
 * plugin's engine), matching {@link EngineRecoveryOperationsTests}'s own sibling seam and shape: the
 * seam itself is core, generic infrastructure; a specific durability mechanism built on top of it
 * (plugins/serverless-storage's manifest-backed local-store recovery) is that plugin's own concern
 * and is tested there, including in real multi-node crash-recovery integration tests.
 */
public class RecoverMissingLocalStoreTests extends IndexShardTestCase {

    /**
     * Simulates a shard initializing on a node whose local disk never had this shard's data
     * (rfc-serverless-opensearch.md &sect;7.1.2's "no peer recovery" writer shards are the
     * motivating case) via {@link RecoverySource.ExistingStoreRecoverySource} against a
     * deliberately empty {@link Store}.
     */
    private IndexShard newShardExpectingAnExistingStoreThatIsActuallyEmpty(EngineFactory engineFactory) throws IOException {
        ShardId shardId = new ShardId("index", "_na_", 0);
        var shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        return newShard(shardRouting, Settings.EMPTY, new EngineBackedIndexerFactory(engineFactory));
    }

    public void testDefaultRecoverMissingLocalStoreLeavesRecoveryFailingAsBefore() throws Exception {
        // The default (false) EngineFactory#recoverMissingLocalStore -- every EngineFactory that
        // doesn't override it, i.e. current behavior for the entire codebase outside
        // plugins/serverless-storage -- must leave core's existing "shard allocated for local
        // recovery, should exist, but doesn't" failure completely unchanged.
        IndexShard shard = newShardExpectingAnExistingStoreThatIsActuallyEmpty(new InternalEngineFactory());
        try {
            expectThrows(IndexShardRecoveryException.class, () -> recoverShardFromStore(shard));
        } finally {
            closeShards(shard);
        }
    }

    public void testOverriddenRecoverMissingLocalStoreLetsRecoveryProceed() throws Exception {
        // An EngineFactory that overrides the hook to materialize a fresh local commit + translog
        // (mirroring what StoreRecovery#recoverEmptyStore already does for its own, unconditional
        // case, per the seam's own javadoc) must let recovery proceed normally instead of failing,
        // proving core's conditional branch genuinely re-reads the store afterward rather than
        // trusting the true return value blindly.
        EngineFactory engineFactory = new EngineFactory() {
            @Override
            public org.opensearch.index.engine.Engine newReadWriteEngine(org.opensearch.index.engine.EngineConfig config) {
                return new org.opensearch.index.engine.InternalEngine(config);
            }

            @Override
            public boolean recoverMissingLocalStore(IndexShard indexShard, Store store) throws IOException {
                store.createEmpty(indexShard.indexSettings().getIndexVersionCreated().luceneVersion);
                String translogUUID = Translog.createEmptyTranslog(
                    indexShard.shardPath().resolveTranslog(),
                    SequenceNumbers.NO_OPS_PERFORMED,
                    indexShard.shardId(),
                    indexShard.getPendingPrimaryTerm()
                );
                store.associateIndexWithNewTranslog(translogUUID);
                return true;
            }
        };

        IndexShard shard = newShardExpectingAnExistingStoreThatIsActuallyEmpty(engineFactory);
        try {
            recoverShardFromStore(shard);
            assertEquals(
                "a freshly-materialized-from-elsewhere store must start with no operations, not fail recovery",
                -1L,
                shard.seqNoStats().getMaxSeqNo()
            );
        } finally {
            closeShards(shard);
        }
    }
}
