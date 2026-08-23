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
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;

import java.io.IOException;

/**
 * Proves the generic core seam {@link ShardRecoveryStrategy#recoverLocalStore} / {@code
 * StoreRecovery#internalRecoverFromStore} actually wires a strategy's ability to materialize
 * a missing local commit from elsewhere into the real shard-recovery path -- not just that it
 * compiles. Deliberately strategy-agnostic (a bare {@link ShardRecoveryStrategy}, not any specific
 * plugin's), matching {@link EngineRecoveryOperationsTests}'s own sibling seam and shape: the
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
    private IndexShard newShardExpectingAnExistingStoreThatIsActuallyEmpty(ShardRecoveryStrategy strategy) throws IOException {
        ShardId shardId = new ShardId("index", "_na_", 0);
        var shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        this.shardRecoveryStrategy = strategy;
        return newShard(shardRouting, Settings.EMPTY, new EngineBackedIndexerFactory(new InternalEngineFactory()));
    }

    public void testLocalLuceneStrategyLeavesRecoveryFailingAsBefore() throws Exception {
        // Core's own local-lucene strategy -- the default for every index outside
        // plugins/serverless-storage -- must leave core's existing "shard allocated for local
        // recovery, should exist, but doesn't" failure completely unchanged.
        IndexShard shard = newShardExpectingAnExistingStoreThatIsActuallyEmpty(LocalLuceneShardRecoveryStrategy.INSTANCE);
        try {
            expectThrows(IndexShardRecoveryException.class, () -> recoverShardFromStore(shard));
        } finally {
            closeShards(shard);
        }
    }

    public void testStrategyThatMaterializesLetsRecoveryProceed() throws Exception {
        // A strategy that materializes a fresh local commit + translog (mirroring what
        // StoreRecovery#recoverEmptyStore already does for its own, unconditional case, per the
        // seam's own javadoc) must let recovery proceed normally instead of failing, proving core's
        // conditional branch genuinely re-reads the store afterward rather than trusting the true
        // return value blindly.
        IndexShard shard = newShardExpectingAnExistingStoreThatIsActuallyEmpty((indexShard, store, recoverySourceType) -> {
            assertEquals(
                "core must tell the strategy WHICH case it is asking about -- that parameter is the only thing "
                    + "that used to distinguish three otherwise byte-identical hooks",
                RecoverySource.Type.EXISTING_STORE,
                recoverySourceType
            );
            store.createEmpty(indexShard.indexSettings().getIndexVersionCreated().luceneVersion);
            String translogUUID = Translog.createEmptyTranslog(
                indexShard.shardPath().resolveTranslog(),
                SequenceNumbers.NO_OPS_PERFORMED,
                indexShard.shardId(),
                indexShard.getPendingPrimaryTerm()
            );
            store.associateIndexWithNewTranslog(translogUUID);
            return true;
        });
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

    public void testStrategyDecliningAfterCallingItselfStaleFailsRecoveryLoudly() throws Exception {
        // localStoreIsStale true followed by recoverLocalStore false leaves the shard with nothing:
        // core cleans the local Lucene index between the two calls. That must fail loudly rather
        // than open an engine on a store recovery just emptied.
        ShardRecoveryStrategy staleButUnableToReplace = new ShardRecoveryStrategy() {
            @Override
            public boolean recoverLocalStore(IndexShard indexShard, Store store, RecoverySource.Type recoverySourceType) {
                return false;
            }

            @Override
            public boolean localStoreIsStale(IndexShard indexShard, String localSegmentsFileName) {
                return true;
            }
        };
        // Recover once with a working strategy so a readable local commit exists, then reopen the
        // same store with the stale-but-unable-to-replace one: localStoreIsStale is only consulted
        // when there IS something readable on disk.
        IndexShard shard = newShardExpectingAnExistingStoreThatIsActuallyEmpty((indexShard, store, recoverySourceType) -> {
            store.createEmpty(indexShard.indexSettings().getIndexVersionCreated().luceneVersion);
            String translogUUID = Translog.createEmptyTranslog(
                indexShard.shardPath().resolveTranslog(),
                SequenceNumbers.NO_OPS_PERFORMED,
                indexShard.shardId(),
                indexShard.getPendingPrimaryTerm()
            );
            store.associateIndexWithNewTranslog(translogUUID);
            return true;
        });
        recoverShardFromStore(shard);
        this.shardRecoveryStrategy = staleButUnableToReplace;
        IndexShard reopened = reinitShard(shard);
        try {
            expectThrows(IndexShardRecoveryException.class, () -> recoverShardFromStore(reopened));
        } finally {
            closeShards(reopened);
        }
    }
}
