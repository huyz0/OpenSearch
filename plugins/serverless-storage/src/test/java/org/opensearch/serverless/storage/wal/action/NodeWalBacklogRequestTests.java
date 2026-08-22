/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class NodeWalBacklogRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        NodeWalBacklogRequest original = new NodeWalBacklogRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        new NodeWalBacklogRequest(out.bytes().streamInput());
    }

    public void testValidateAcceptsAnEmptyRequest() {
        assertNull(new NodeWalBacklogRequest().validate());
    }
}
