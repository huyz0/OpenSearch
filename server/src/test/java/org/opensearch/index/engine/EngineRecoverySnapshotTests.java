/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class EngineRecoverySnapshotTests extends OpenSearchTestCase {
    public void testDefaultEngineNeedsNoAdditionalRecovery() throws Exception {
        final Engine engine = mock(Engine.class);
        doCallRealMethod().when(engine).engineRecoverySnapshot();
        try (var snapshot = engine.engineRecoverySnapshot()) {
            assertEquals(0, snapshot.totalOperations());
            assertNull(snapshot.next());
        }
    }
}
