/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

/**
 * {@link EngineFactory#recoverFromEngineNativeSnapshot} and {@link
 * EngineFactory#releaseEngineNativeSnapshot} are plain interface defaults with no engine/shard
 * machinery of their own to exercise -- unlike the sibling {@code recoverMissingLocalStore} family
 * (see {@code RecoverMissingLocalStoreTests}), which drives a real shard through recovery, these
 * two defaults are proven correct by a direct call: every {@link EngineFactory} that doesn't
 * override them (the entire codebase outside plugins/serverless-storage) must get "unsupported,
 * fall back" (false) and "nothing to release" (no-op) respectively.
 */
public class EngineFactoryEngineNativeSnapshotDefaultsTests extends OpenSearchTestCase {

    // newReadWriteEngine is never actually invoked by these tests -- only the two default methods
    // under test are called -- so it just needs to exist to satisfy the @FunctionalInterface.
    private static final EngineFactory DEFAULT_ENGINE_FACTORY = config -> {
        throw new UnsupportedOperationException("not used by this test");
    };

    public void testDefaultRecoverFromEngineNativeSnapshotReturnsFalse() throws Exception {
        assertFalse(
            "an EngineFactory that hasn't opted in must tell StoreRecovery it can't handle an "
                + "engine-native pointer, not silently succeed",
            DEFAULT_ENGINE_FACTORY.recoverFromEngineNativeSnapshot(mock(IndexShard.class), mock(Store.class), new byte[] { 1, 2, 3 })
        );
    }

    public void testDefaultReleaseEngineNativeSnapshotIsANoOpAndDoesNotThrow() throws Exception {
        // Nothing to assert on beyond "doesn't throw" -- an EngineFactory that never produces
        // engine-native pointers (attemptEngineNativeSnapshot's own default) correspondingly never
        // has anything of its own to release.
        DEFAULT_ENGINE_FACTORY.releaseEngineNativeSnapshot(new byte[] { 1, 2, 3 });
    }

    public void testDefaultSupportsEngineNativeSnapshotsReturnsFalse() {
        assertFalse(
            "an EngineFactory that hasn't opted in must tell StoreRecovery to skip the remote "
                + "engine-native probe entirely, not claim support it doesn't have",
            DEFAULT_ENGINE_FACTORY.supportsEngineNativeSnapshots()
        );
    }
}
