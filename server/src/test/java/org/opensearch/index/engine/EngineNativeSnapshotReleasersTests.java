/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.index.shard.ShardRecoveryStrategy;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class EngineNativeSnapshotReleasersTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        // The registry is a static, node-wide singleton -- clean up unconditionally so this test
        // never leaks a registration into an unrelated test running later in the same JVM.
        EngineNativeSnapshotReleasers.unregister("test-engine-id");
        super.tearDown();
    }

    public void testIsEmptyByDefault() {
        assertTrue("a fresh registry (nothing registered under this test's id) must report empty", isEmptyOfTestId());
    }

    public void testIsNotEmptyAfterRegister() {
        EngineNativeSnapshotReleasers.register("test-engine-id", mock(ShardRecoveryStrategy.EngineNativeSnapshots.class));
        assertFalse("BlobStoreRepository's delete-path gate relies on this flipping false once anything is registered", isEmptyOfTestId());
    }

    public void testIsEmptyAgainAfterUnregister() {
        EngineNativeSnapshotReleasers.register("test-engine-id", mock(ShardRecoveryStrategy.EngineNativeSnapshots.class));
        EngineNativeSnapshotReleasers.unregister("test-engine-id");
        assertTrue(isEmptyOfTestId());
    }

    public void testFindReturnsTheRegisteredReleaser() {
        ShardRecoveryStrategy.EngineNativeSnapshots releaser = mock(ShardRecoveryStrategy.EngineNativeSnapshots.class);
        EngineNativeSnapshotReleasers.register("test-engine-id", releaser);
        assertTrue(EngineNativeSnapshotReleasers.find("test-engine-id").isPresent());
        assertSame(releaser, EngineNativeSnapshotReleasers.find("test-engine-id").get());
    }

    public void testFindReturnsEmptyForAnUnregisteredId() {
        assertTrue(EngineNativeSnapshotReleasers.find("never-registered-id").isEmpty());
    }

    // isEmpty() is genuinely node-wide (not scoped to this test's own id), so this only asserts a
    // meaningful result when nothing else in the same JVM has ever registered anything -- true for
    // every other test in this class given tearDown's unconditional cleanup, and this is the only
    // test file that registers anything under EngineNativeSnapshotReleasers at all.
    private static boolean isEmptyOfTestId() {
        return EngineNativeSnapshotReleasers.isEmpty();
    }
}
