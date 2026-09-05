/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ReactivateShardsRequestTests extends OpenSearchTestCase {

    public void testValidateRejectsAnEmptyIndexName() {
        assertNotNull(new ReactivateShardsRequest("", false).validate());
        assertNotNull(new ReactivateShardsRequest(null, false).validate());
    }

    public void testValidateAcceptsANonEmptyIndexName() {
        assertNull(new ReactivateShardsRequest("some-index", false).validate());
    }

    public void testSerializationRoundTrip() throws Exception {
        ReactivateShardsRequest original = new ReactivateShardsRequest("round-trip-index", true);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        ReactivateShardsRequest deserialized = new ReactivateShardsRequest(StreamInput.wrap(out.bytes().toBytesRef().bytes));
        assertEquals(original.indexName(), deserialized.indexName());
        assertEquals(original.reader(), deserialized.reader());
    }

    /**
     * Finding L-3: the shard set has to survive the wire, because the filter that narrows the scope
     * runs on the coordinating node and the action that honours it runs on the cluster manager.
     */
    public void testShardIdsSurviveSerialization() throws Exception {
        ReactivateShardsRequest original = new ReactivateShardsRequest("round-trip-index", false, java.util.Set.of(0, 7, 42));

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        ReactivateShardsRequest deserialized = new ReactivateShardsRequest(StreamInput.wrap(out.bytes().toBytesRef().bytes));
        assertEquals(java.util.Set.of(0, 7, 42), deserialized.shardIds());
    }

    public void testTheDefaultScopeIsTheWholeIndex() {
        // An empty set means "every suspended shard", which is what a search needs and what any
        // caller that cannot resolve shards must fall back to.
        assertTrue(new ReactivateShardsRequest("idx", false).shardIds().isEmpty());
        assertTrue(new ReactivateShardsRequest("idx", false, null).shardIds().isEmpty());
    }

    public void testSerializationRoundTripForWriterRequest() throws Exception {
        ReactivateShardsRequest original = new ReactivateShardsRequest("round-trip-index", false);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        ReactivateShardsRequest deserialized = new ReactivateShardsRequest(StreamInput.wrap(out.bytes().toBytesRef().bytes));
        assertFalse(deserialized.reader());
    }
}
