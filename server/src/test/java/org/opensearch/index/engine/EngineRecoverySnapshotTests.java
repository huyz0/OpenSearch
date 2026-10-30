/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.index.translog.Translog;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EngineRecoverySnapshotTests extends OpenSearchTestCase {
    public void testExistingListOverridesAreAdaptedInOrder() throws Exception {
        final Engine engine = mock(Engine.class);
        final var first = new Translog.Delete("first", 0, 1, 1);
        final var second = new Translog.Delete("second", 1, 1, 1);
        when(engine.engineRecoveryOperations()).thenReturn(List.of(first, second));
        doCallRealMethod().when(engine).engineRecoverySnapshot();
        try (var snapshot = engine.engineRecoverySnapshot()) {
            assertEquals(2, snapshot.totalOperations());
            assertSame(first, snapshot.next());
            assertSame(second, snapshot.next());
            assertNull(snapshot.next());
        }
    }

    public void testDefaultEngineNeedsNoAdditionalRecovery() throws Exception {
        final Engine engine = mock(Engine.class);
        doCallRealMethod().when(engine).engineRecoveryOperations();
        doCallRealMethod().when(engine).engineRecoverySnapshot();
        try (var snapshot = engine.engineRecoverySnapshot()) {
            assertEquals(0, snapshot.totalOperations());
            assertNull(snapshot.next());
        }
    }
}
