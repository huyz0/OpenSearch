/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.test.OpenSearchTestCase;

public class DataStreamBackingIndexNamesTests extends OpenSearchTestCase {

    public void testParsesARealCoreGeneratedBackingIndexName() {
        String backingIndexName = DataStream.getDefaultBackingIndexName("logs-app", 1);
        assertEquals("logs-app", DataStreamBackingIndexNames.parseDataStreamName(backingIndexName).orElseThrow());
    }

    public void testParsesAStreamNameThatItselfContainsHyphensAndDigits() {
        String backingIndexName = DataStream.getDefaultBackingIndexName("logs-app-2024-01", 42);
        assertEquals("logs-app-2024-01", DataStreamBackingIndexNames.parseDataStreamName(backingIndexName).orElseThrow());
    }

    public void testHandlesAHighGenerationNumberBeyondSixDigits() {
        String backingIndexName = DataStream.getDefaultBackingIndexName("logs-app", 1_234_567);
        // Core's own %06d format only zero-pads, it never truncates -- a 7-digit generation still
        // has a fixed-width all-digit suffix, just wider than 6, so this must still parse cleanly.
        assertEquals("logs-app", DataStreamBackingIndexNames.parseDataStreamName(backingIndexName).orElseThrow());
    }

    public void testRejectsAnOrdinaryIndexName() {
        assertTrue(DataStreamBackingIndexNames.parseDataStreamName("my-ordinary-index").isEmpty());
    }

    public void testRejectsNull() {
        assertTrue(DataStreamBackingIndexNames.parseDataStreamName(null).isEmpty());
    }

    public void testRejectsThePrefixAloneWithNoStreamName() {
        assertTrue(DataStreamBackingIndexNames.parseDataStreamName(".ds--000001").isEmpty());
    }
}
