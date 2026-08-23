/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.index.store.Store;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

/**
 * Every {@link ShardRecoveryStrategy} answer that has a default is a "no", and core's own {@link
 * LocalLuceneShardRecoveryStrategy} must give exactly those same answers for every recovery source
 * -- that equivalence is what makes a stock node's behavior identical to what it was before this
 * seam existed, so it is asserted here rather than assumed.
 *
 * <p>Unlike the sibling {@code RecoverMissingLocalStoreTests} family, which drives a real shard
 * through recovery to prove the seam is actually wired into {@code StoreRecovery}, these are plain
 * defaults with no machinery of their own and are proven correct by a direct call.
 */
public class ShardRecoveryStrategyDefaultsTests extends OpenSearchTestCase {

    /** A strategy that overrides only the one method with no default, to observe the inherited answers. */
    private static final ShardRecoveryStrategy ONLY_MANDATORY_METHOD_IMPLEMENTED = (indexShard, store, recoverySourceType) -> false;

    public void testDefaultLocalStoreIsStaleReturnsFalse() throws Exception {
        assertFalse(
            "a strategy that hasn't opted in must leave a readable local commit authoritative, because for an "
                + "ordinary shard it is -- answering 'stale' would delete a local store with nothing to replace it",
            ONLY_MANDATORY_METHOD_IMPLEMENTED.localStoreIsStale(mock(IndexShard.class), "segments_1")
        );
    }

    public void testDefaultOwnsRemoteSegmentDurabilityReturnsFalse() {
        assertFalse(
            "a strategy that hasn't opted in must leave core's own remote-store upload path wired in exactly as "
                + "it always has been -- this is a durability claim, not a performance hint",
            ONLY_MANDATORY_METHOD_IMPLEMENTED.ownsRemoteSegmentDurability()
        );
    }

    public void testDefaultEngineNativeSnapshotsIsEmpty() {
        assertTrue(
            "a strategy that hasn't opted in must tell StoreRecovery to skip the remote engine-native probe "
                + "entirely, not claim support it doesn't have",
            ONLY_MANDATORY_METHOD_IMPLEMENTED.engineNativeSnapshots().isEmpty()
        );
    }

    public void testLocalLuceneStrategyDeclinesEveryRecoverySource() throws Exception {
        for (RecoverySource.Type type : RecoverySource.Type.values()) {
            assertFalse(
                "core's own strategy has nowhere else to materialize from, for any recovery source -- a true here "
                    + "for ["
                    + type
                    + "] would change what a stock node does",
                LocalLuceneShardRecoveryStrategy.INSTANCE.recoverLocalStore(mock(IndexShard.class), mock(Store.class), type)
            );
        }
    }

    public void testLocalLuceneStrategyInheritsEveryDefaultAnswer() throws Exception {
        assertFalse(LocalLuceneShardRecoveryStrategy.INSTANCE.localStoreIsStale(mock(IndexShard.class), "segments_1"));
        assertFalse(LocalLuceneShardRecoveryStrategy.INSTANCE.ownsRemoteSegmentDurability());
        assertTrue(LocalLuceneShardRecoveryStrategy.INSTANCE.engineNativeSnapshots().isEmpty());
    }
}
