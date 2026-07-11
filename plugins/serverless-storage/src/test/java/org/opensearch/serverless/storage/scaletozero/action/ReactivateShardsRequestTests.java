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
        assertNotNull(new ReactivateShardsRequest("").validate());
        assertNotNull(new ReactivateShardsRequest((String) null).validate());
    }

    public void testValidateAcceptsANonEmptyIndexName() {
        assertNull(new ReactivateShardsRequest("some-index").validate());
    }

    public void testSerializationRoundTrip() throws Exception {
        ReactivateShardsRequest original = new ReactivateShardsRequest("round-trip-index");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        ReactivateShardsRequest deserialized = new ReactivateShardsRequest(StreamInput.wrap(out.bytes().toBytesRef().bytes));
        assertEquals(original.indexName(), deserialized.indexName());
    }
}
