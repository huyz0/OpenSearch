/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class NodeObjectStoreRequestStatsRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        NodeObjectStoreRequestStatsRequest original = new NodeObjectStoreRequestStatsRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        new NodeObjectStoreRequestStatsRequest(out.bytes().streamInput());
    }

    public void testValidateAcceptsAnEmptyRequest() {
        assertNull(new NodeObjectStoreRequestStatsRequest().validate());
    }
}
